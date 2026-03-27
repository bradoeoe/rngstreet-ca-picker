from __future__ import annotations

import logging
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

from rng_ca_bot.config import load_settings
from rng_ca_bot.db import Database
from rng_ca_bot.rewards import format_reward_display, redeem_reward_key_payload

LOGGER = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
CLIENT_DIST_DIR = REPO_ROOT / "frontend" / "client" / "dist"
MIGRATIONS_DIR = REPO_ROOT / "sql"


def create_app() -> Flask:
    settings = load_settings()
    db = Database(settings)
    db.run_migrations(MIGRATIONS_DIR)

    app = Flask(__name__, static_folder=None)
    app.config["db"] = db
    app.config["reward_admin_api_key"] = settings.reward_admin_api_key

    def _serialize_reward_row(row: dict) -> dict:
        amount = int(row["reward_amount"]) if row.get("reward_amount") is not None else None
        quantity = int(row["reward_quantity"]) if row.get("reward_quantity") is not None else None
        label = str(row.get("reward_label") or "").strip()
        kind = str(row.get("reward_kind") or "").strip().casefold() or ("gp" if amount else "item")
        return {
            "reward_key": str(row.get("reward_key") or "").strip(),
            "discord_user_id": str(row.get("discord_user_id") or "").strip(),
            "rsn": str(row.get("rsn") or "").strip(),
            "task_id": int(row["task_id"]) if row.get("task_id") is not None else None,
            "status": str(row.get("status") or "").strip(),
            "reward_tier": str(row.get("reward_tier") or "").strip(),
            "reward_kind": kind,
            "reward_label": label,
            "reward_amount": amount,
            "reward_quantity": quantity,
            "reward_image_url": str(row.get("reward_image_url") or "").strip() or None,
            "display_value": format_reward_display(
                kind=kind,
                label=label,
                amount=amount,
                quantity=quantity,
            ),
            "payout_status": str(row.get("payout_status") or "").strip() or "unpaid",
            "payout_marked_at": row["payout_marked_at"].isoformat() if row.get("payout_marked_at") else None,
            "payout_marked_by": str(row.get("payout_marked_by") or "").strip() or None,
            "payout_notes": str(row.get("payout_notes") or "").strip() or None,
            "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
            "verified_at": row["verified_at"].isoformat() if row.get("verified_at") else None,
            "used_at": row["used_at"].isoformat() if row.get("used_at") else None,
        }

    def _require_admin():
        expected_key = str(app.config.get("reward_admin_api_key") or "").strip()
        if not expected_key:
            return (
                jsonify(
                    {
                        "status": "disabled",
                        "message": "Admin payout endpoints are disabled until REWARD_ADMIN_API_KEY is set.",
                    }
                ),
                503,
            )

        provided_key = str(request.headers.get("X-Admin-Key") or "").strip()
        if provided_key != expected_key:
            return jsonify({"status": "forbidden", "message": "Admin key missing or invalid."}), 403
        return None

    @app.after_request
    def add_cors_headers(response):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Admin-Key"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        return response

    @app.get("/api/health")
    def api_health():
        return jsonify({"status": "ok"})

    @app.route("/api/redeem", methods=["POST", "OPTIONS"])
    def api_redeem():
        if request.method == "OPTIONS":
            return ("", 204)

        payload = request.get_json(silent=True) or {}
        reward_key = str(payload.get("reward_key") or "").strip()
        status_code, response = redeem_reward_key_payload(app.config["db"], reward_key)
        return jsonify(response), int(status_code)

    @app.get("/api/admin/payouts")
    def api_admin_payouts():
        auth_response = _require_admin()
        if auth_response is not None:
            return auth_response

        raw_limit = str(request.args.get("limit") or "").strip()
        limit = 200
        if raw_limit:
            try:
                limit = max(1, min(int(raw_limit), 500))
            except ValueError:
                return jsonify({"status": "invalid", "message": "limit must be a number."}), 400
        payout_status = str(request.args.get("status") or "unpaid").strip().casefold()

        try:
            with app.config["db"].connection() as conn:
                counts = app.config["db"].get_reward_payout_counts(conn)
                payouts = app.config["db"].list_reward_payouts(conn, payout_status=payout_status, limit=limit)
        except ValueError as exc:
            return jsonify({"status": "invalid", "message": str(exc)}), 400

        return jsonify(
            {
                "status": "ok",
                "counts": counts,
                "payouts": [_serialize_reward_row(row) for row in payouts],
            }
        )

    @app.post("/api/admin/payouts/<reward_key>")
    def api_admin_update_payout(reward_key: str):
        auth_response = _require_admin()
        if auth_response is not None:
            return auth_response

        payload = request.get_json(silent=True) or {}
        payout_status = str(payload.get("payout_status") or "paid").strip().casefold()
        actor = str(payload.get("actor") or "").strip() or None
        notes = str(payload.get("notes") or "").strip() or None

        with app.config["db"].connection() as conn:
            try:
                reward = app.config["db"].update_reward_payout_status(
                    conn,
                    reward_key.strip().upper(),
                    payout_status=payout_status,
                    marked_by=actor,
                    notes=notes,
                )
                if reward is None:
                    conn.rollback()
                    return jsonify({"status": "invalid", "message": "Reward key not found."}), 404
                if str(reward.get("status") or "").strip() != "redeemed":
                    conn.rollback()
                    return (
                        jsonify(
                            {
                                "status": "invalid",
                                "message": "Only redeemed rewards can be marked paid or unpaid.",
                            }
                        ),
                        409,
                    )
                conn.commit()
            except ValueError as exc:
                conn.rollback()
                return jsonify({"status": "invalid", "message": str(exc)}), 400
            except Exception:
                conn.rollback()
                raise

        return jsonify({"status": "ok", "reward": _serialize_reward_row(reward)})

    @app.get("/")
    def serve_index():
        if CLIENT_DIST_DIR.exists():
            return send_from_directory(CLIENT_DIST_DIR, "index.html")
        return (
            "React client has not been built yet. Start Vite in frontend/client or run a production build.",
            200,
        )

    @app.get("/<path:path>")
    def serve_client_asset(path: str):
        asset_path = CLIENT_DIST_DIR / path
        if asset_path.exists() and asset_path.is_file():
            return send_from_directory(CLIENT_DIST_DIR, path)
        if CLIENT_DIST_DIR.exists():
            return send_from_directory(CLIENT_DIST_DIR, "index.html")
        return ("Not found", 404)

    return app


app = create_app()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )
    LOGGER.info("Starting Flask rewards server on http://127.0.0.1:5000")
    app.run(host="127.0.0.1", port=5000, debug=True)
