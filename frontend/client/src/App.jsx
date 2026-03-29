import { useEffect, useMemo, useRef, useState } from "react";

const SPIN_DURATION_MS = 14500;

const MODE_CONFIG = {
  reward: {
    title: "Reward Case",
    sub: "Enter your reward key to crack open an RNG Street case and reveal your prize.",
    inputName: "reward_key",
    inputPlaceholder: "Enter reward key here",
    endpoint: "/api/redeem",
    keyField: "reward_key",
    itemField: "reward",
    openLabel: "Open Case",
    openingLabel: "Opening...",
    successMessage: "Reward redeemed successfully.",
    usedTitle: "Already Redeemed",
    usedMessage: "That reward key has already been redeemed.",
    openStatus: "Opening case...",
    missingKeyMessage: "Enter a reward key first.",
    errorMessage: "That reward key could not be redeemed.",
    networkErrorMessage: "Could not redeem reward key right now. Please try again.",
    sectionLabel: "Reward",
  },
  task: {
    title: "Task Roll Case",
    sub: "Enter your task roll key to spin for a new task or an unlimited reroll result.",
    inputName: "task_roll_key",
    inputPlaceholder: "Enter task roll key here",
    endpoint: "/api/task-roll/redeem",
    keyField: "task_roll_key",
    itemField: "task",
    openLabel: "Roll Task",
    openingLabel: "Rolling...",
    successMessage: "Task rolled successfully.",
    usedTitle: "Already Used",
    usedMessage: "That task roll key has already been used.",
    openStatus: "Spinning task reel...",
    missingKeyMessage: "Enter a task roll key first.",
    errorMessage: "That task roll key could not be used.",
    networkErrorMessage: "Could not roll a task right now. Please try again.",
    sectionLabel: "Task Roll",
  },
};

function CaseCard({ item, revealed = false, selected = false }) {
  const cardClass = [
    "card",
    item?.card_class || "white",
    selected ? "selected" : "",
    revealed ? "opened" : "",
  ]
    .filter(Boolean)
    .join(" ");

  const revealNote = item?.kind === "task" ? "Task Assigned" : "Unlocked";
  const revealDesc =
    item?.kind === "task"
      ? item?.subtitle || "RuneScape combat task"
      : item?.kind === "item"
        ? "RuneScape item reward"
        : "RuneScape GP reward";

  return (
    <article className={cardClass}>
      {revealed ? (
        <div className="reveal">
          {item?.image_url ? <img className="reveal-thumb" src={item.image_url} alt={item.label} /> : null}
          <div className="reveal-amount">{item?.display_value || item?.label || "-"}</div>
          <div className="reveal-note">{revealNote}</div>
          <div className="reveal-desc">{revealDesc}</div>
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

function initialMode() {
  try {
    const params = new URLSearchParams(window.location.search);
    return params.get("mode") === "task" ? "task" : "reward";
  } catch (_error) {
    return "reward";
  }
}

function keyFromUrl(inputName) {
  try {
    const params = new URLSearchParams(window.location.search);
    return (params.get(inputName) || "").trim().toUpperCase();
  } catch (_error) {
    return "";
  }
}

export default function App() {
  const [caseMode, setCaseMode] = useState(initialMode);
  const [inputKey, setInputKey] = useState(() => {
    const mode = initialMode();
    const modeInputName = MODE_CONFIG[mode]?.inputName || MODE_CONFIG.reward.inputName;
    return keyFromUrl(modeInputName);
  });
  const [statusMessage, setStatusMessage] = useState("");
  const [statusKind, setStatusKind] = useState("");
  const [loading, setLoading] = useState(false);
  const [payload, setPayload] = useState(null);
  const [spinState, setSpinState] = useState("idle");
  const [resultMode, setResultMode] = useState(null);

  const viewportRef = useRef(null);
  const stripRef = useRef(null);
  const spinTimerRef = useRef(null);

  const modeConfig = useMemo(() => MODE_CONFIG[caseMode] || MODE_CONFIG.reward, [caseMode]);
  const selectedItem = payload?.[modeConfig.itemField] || null;
  const selectedKey = payload?.[modeConfig.keyField] || "";

  useEffect(() => {
    let nextKey = "";
    try {
      const url = new URL(window.location.href);
      if (caseMode === "task") {
        url.searchParams.set("mode", "task");
      } else {
        url.searchParams.delete("mode");
      }
      nextKey = (url.searchParams.get(modeConfig.inputName) || "").trim().toUpperCase();
      window.history.replaceState({}, "", url.toString());
    } catch (_error) {
      // no-op
    }
    setInputKey(nextKey);
    setStatusMessage("");
    setStatusKind("");
    setPayload(null);
    setSpinState("idle");
    setResultMode(null);
  }, [caseMode, modeConfig.inputName]);

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
      setStatusMessage(payload.message || modeConfig.successMessage);
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
  }, [payload, resultMode, spinState, modeConfig.successMessage]);

  async function handleSubmit(event) {
    event.preventDefault();
    const normalizedKey = inputKey.trim().toUpperCase();
    if (!normalizedKey) {
      setStatusMessage(modeConfig.missingKeyMessage);
      setStatusKind("error");
      return;
    }

    setLoading(true);
    setStatusMessage(modeConfig.openStatus);
    setStatusKind("");

    try {
      const response = await fetch(modeConfig.endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ [modeConfig.inputName]: normalizedKey }),
      });
      const nextPayload = await response.json();
      const selected = nextPayload?.[modeConfig.itemField] || null;

      if (nextPayload.status === "ok") {
        setPayload(nextPayload);
        setResultMode("ok");
        setSpinState("spinning");
        return;
      }

      if (nextPayload.status === "used" && selected) {
        setPayload(nextPayload);
        setResultMode("used");
        setSpinState("finished");
        setStatusMessage(nextPayload.message || modeConfig.usedMessage);
        setStatusKind("error");
        return;
      }

      setPayload(null);
      setResultMode(null);
      setSpinState("idle");
      setStatusMessage(nextPayload.message || modeConfig.errorMessage);
      setStatusKind("error");
    } catch (_error) {
      setPayload(null);
      setResultMode(null);
      setSpinState("idle");
      setStatusMessage(modeConfig.networkErrorMessage);
      setStatusKind("error");
    } finally {
      setLoading(false);
    }
  }

  const showCaseWrap = Boolean(payload);
  const reel = payload?.reel || [];
  const selectedIndex = payload?.selected_index ?? 0;
  const stripClass = spinState === "finished" ? "strip finished" : spinState === "spinning" ? "strip spinning" : "strip";

  return (
    <main className="shell">
      <section className="hero">
        <h1>{modeConfig.title}</h1>
        <p className="sub">{modeConfig.sub}</p>
        <div className="mode-toggle">
          <button
            type="button"
            className={caseMode === "reward" ? "active" : ""}
            onClick={() => setCaseMode("reward")}
          >
            Rewards
          </button>
          <button
            type="button"
            className={caseMode === "task" ? "active" : ""}
            onClick={() => setCaseMode("task")}
          >
            Tasks
          </button>
        </div>
      </section>

      <section className="content">
        <form className="form" onSubmit={handleSubmit}>
          <input
            id={`${caseMode}-key`}
            name={modeConfig.inputName}
            type="text"
            placeholder={modeConfig.inputPlaceholder}
            autoComplete="off"
            value={inputKey}
            onChange={(event) => setInputKey(event.target.value.toUpperCase())}
            required
          />
          <button type="submit" disabled={loading}>
            {loading ? modeConfig.openingLabel : modeConfig.openLabel}
          </button>
        </form>

        <div className={`status${statusKind ? ` ${statusKind}` : ""}`}>{statusMessage}</div>

        {showCaseWrap ? (
          <section className="case-wrap active">
            <div className="case-header">
              <span>{modeConfig.sectionLabel}</span>
              <span>{selectedKey ? `Key ${selectedKey}` : ""}</span>
            </div>

            {resultMode === "ok" ? (
              <>
                <div className="viewport" ref={viewportRef}>
                  <div className="pointer" />
                  <div className={stripClass} ref={stripRef}>
                    {reel.map((item, index) => (
                      <CaseCard
                        key={`${selectedKey}-${index}`}
                        item={item}
                        selected={index === selectedIndex}
                        revealed={spinState === "finished" && index === selectedIndex}
                      />
                    ))}
                  </div>
                </div>

                {spinState === "finished" && selectedItem ? (
                  <div className="result active">
                    {selectedItem.image_url ? (
                      <img className="result-thumb" src={selectedItem.image_url} alt={selectedItem.label} />
                    ) : null}
                    <h2>{selectedItem.display_value}</h2>
                    <p>
                      {selectedItem.kind === "task" ? "Task roll result:" : "The case cracked open and revealed"}{" "}
                      <strong>{selectedItem.label}</strong>.
                    </p>
                    {selectedItem.rerolls_remaining !== undefined ? (
                      <p>Rerolls remaining: <strong>{selectedItem.rerolls_remaining}</strong></p>
                    ) : null}
                  </div>
                ) : null}
              </>
            ) : (
              <div className="result active">
                <h2>{modeConfig.usedTitle}</h2>
                <p>{payload.message || modeConfig.usedMessage}</p>
                <div className="used-card">
                  <CaseCard item={selectedItem} revealed />
                </div>
              </div>
            )}
          </section>
        ) : null}
      </section>
    </main>
  );
}
