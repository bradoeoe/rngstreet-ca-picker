import { useEffect, useRef, useState } from "react";

const SPIN_DURATION_MS = 14500;

function RewardCard({ item, revealed = false, selected = false }) {
  const cardClass = [
    "card",
    item?.card_class || "white",
    selected ? "selected" : "",
    revealed ? "opened" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <article className={cardClass}>
      {revealed ? (
        <div className="reveal">
          {item?.image_url ? <img className="reveal-thumb" src={item.image_url} alt={item.label} /> : null}
          <div className="reveal-amount">{item.display_value}</div>
          <div className="reveal-note">Unlocked</div>
          <div className="reveal-desc">{item.kind === "item" ? "RuneScape item reward" : "RuneScape GP reward"}</div>
        </div>
      ) : (
        <div className="case-shell">
          <div className="case-lid" />
          <div className="case-body">
            <div className="case-badge" />
            <div className="case-title">RNG Case</div>
            <div className="case-desc">Street Supply</div>
          </div>
        </div>
      )}
    </article>
  );
}

export default function App() {
  const [rewardKey, setRewardKey] = useState("");
  const [statusMessage, setStatusMessage] = useState("");
  const [statusKind, setStatusKind] = useState("");
  const [loading, setLoading] = useState(false);
  const [payload, setPayload] = useState(null);
  const [spinState, setSpinState] = useState("idle");
  const [resultMode, setResultMode] = useState(null);

  const viewportRef = useRef(null);
  const stripRef = useRef(null);
  const spinTimerRef = useRef(null);

  useEffect(() => {
    return () => {
      if (spinTimerRef.current) {
        window.clearTimeout(spinTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    if (!payload || resultMode !== "ok" || spinState !== "spinning") {
      return;
    }
    const strip = stripRef.current;
    const viewport = viewportRef.current;
    if (!strip || !viewport) {
      return;
    }

    strip.className = "strip spinning";
    strip.style.transition = "none";
    strip.style.transform = "translateX(0px)";

    const rafA = window.requestAnimationFrame(() => {
      const rafB = window.requestAnimationFrame(() => {
        const selected = strip.children[payload.selected_index];
        if (!selected) {
          return;
        }
        const targetX = viewport.clientWidth / 2 - (selected.offsetLeft + selected.clientWidth / 2);
        strip.style.transition = "transform 14.5s cubic-bezier(0.06, 0.98, 0.12, 1)";
        strip.style.transform = `translateX(${targetX}px)`;
      });
      strip.dataset.rafB = String(rafB);
    });

    spinTimerRef.current = window.setTimeout(() => {
      setSpinState("finished");
      setStatusMessage("Reward redeemed successfully.");
      setStatusKind("success");
    }, SPIN_DURATION_MS + 220);

    return () => {
      window.cancelAnimationFrame(rafA);
      const rafB = Number(strip.dataset.rafB || 0);
      if (rafB) {
        window.cancelAnimationFrame(rafB);
      }
      if (spinTimerRef.current) {
        window.clearTimeout(spinTimerRef.current);
      }
    };
  }, [payload, resultMode, spinState]);

  async function handleSubmit(event) {
    event.preventDefault();
    const normalizedKey = rewardKey.trim().toUpperCase();
    if (!normalizedKey) {
      setStatusMessage("Enter a reward key first.");
      setStatusKind("error");
      return;
    }

    setLoading(true);
    setStatusMessage("Opening case...");
    setStatusKind("");

    try {
      const response = await fetch("/api/redeem", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ reward_key: normalizedKey }),
      });
      const nextPayload = await response.json();

      if (nextPayload.status === "ok") {
        setPayload(nextPayload);
        setResultMode("ok");
        setSpinState("spinning");
        return;
      }

      if (nextPayload.status === "used" && nextPayload.reward) {
        setPayload(nextPayload);
        setResultMode("used");
        setSpinState("finished");
        setStatusMessage(nextPayload.message || "That reward key has already been redeemed.");
        setStatusKind("error");
        return;
      }

      setPayload(null);
      setResultMode(null);
      setSpinState("idle");
      setStatusMessage(nextPayload.message || "That reward key could not be redeemed.");
      setStatusKind("error");
    } catch (_error) {
      setPayload(null);
      setResultMode(null);
      setSpinState("idle");
      setStatusMessage("Could not redeem reward key right now. Please try again.");
      setStatusKind("error");
    } finally {
      setLoading(false);
    }
  }

  const showCaseWrap = Boolean(payload);
  const reel = payload?.reel || [];
  const selectedIndex = payload?.selected_index ?? 0;
  const selectedReward = payload?.reward || null;
  const stripClass = spinState === "finished" ? "strip finished" : spinState === "spinning" ? "strip spinning" : "strip";

  return (
    <main className="shell">
      <section className="hero">
        <h1>Reward Case</h1>
        <p className="sub">Enter your reward key to crack open an RNG Street case and reveal your prize.</p>
      </section>

      <section className="content">
        <form className="form" onSubmit={handleSubmit}>
          <input
            id="reward-key"
            name="reward_key"
            type="text"
            placeholder="Enter reward key here"
            autoComplete="off"
            value={rewardKey}
            onChange={(event) => setRewardKey(event.target.value.toUpperCase())}
            required
          />
          <button type="submit" disabled={loading}>
            {loading ? "Opening..." : "Open Case"}
          </button>
        </form>

        <div className={`status${statusKind ? ` ${statusKind}` : ""}`}>{statusMessage}</div>

        {showCaseWrap ? (
          <section className="case-wrap active">
            <div className="case-header">
              <span>Reward</span>
              <span>{payload.reward_key ? `Key ${payload.reward_key}` : ""}</span>
            </div>

            {resultMode === "ok" ? (
              <>
                <div className="viewport" ref={viewportRef}>
                  <div className="pointer" />
                  <div className={stripClass} ref={stripRef}>
                    {reel.map((item, index) => (
                      <RewardCard
                        key={`${payload.reward_key}-${index}`}
                        item={item}
                        selected={index === selectedIndex}
                        revealed={spinState === "finished" && index === selectedIndex}
                      />
                    ))}
                  </div>
                </div>

                {spinState === "finished" && selectedReward ? (
                  <div className="result active">
                    {selectedReward.image_url ? (
                      <img className="result-thumb" src={selectedReward.image_url} alt={selectedReward.label} />
                    ) : null}
                    <h2>{selectedReward.display_value}</h2>
                    <p>The case cracked open and revealed <strong>{selectedReward.label}</strong>.</p>
                  </div>
                ) : null}
              </>
            ) : (
              <div className="result active">
                <h2>Already Redeemed</h2>
                <p>{payload.message || "That reward key has already been redeemed."}</p>
                <div className="used-card">
                  <RewardCard item={selectedReward} revealed />
                </div>
              </div>
            )}
          </section>
        ) : null}
      </section>
    </main>
  );
}
