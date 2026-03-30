import { useEffect, useMemo, useRef, useState } from "react";

const SPIN_DURATION_MS = 12500;
const SPIN_EASING = "cubic-bezier(0.06, 0.98, 0.12, 1)";
const TASK_KEY_PREFIX = "TASK-";
const LANDING_INNER_MIN_RATIO = 0.12;
const LANDING_INNER_MAX_RATIO = 0.88;
const LANDING_EDGE_GUTTER_PX = 8;

const MODE_CONFIG = {
  reward: {
    inputName: "reward_key",
    endpoint: "/api/redeem",
    itemField: "reward",
    keyField: "reward_key",
    openStatus: "Opening reward case...",
    successMessage: "Reward redeemed successfully.",
    usedMessage: "That reward key has already been redeemed.",
    errorMessage: "That reward key could not be redeemed.",
    networkErrorMessage: "Could not redeem reward key right now. Please try again.",
    sectionLabel: "Reward Case",
  },
  task: {
    inputName: "task_roll_key",
    endpoint: "/api/task-roll/redeem",
    itemField: "task",
    keyField: "task_roll_key",
    openStatus: "Rolling task case...",
    successMessage: "Task rolled successfully.",
    usedMessage: "That task roll key has already been used.",
    errorMessage: "That task roll key could not be used.",
    networkErrorMessage: "Could not roll a task right now. Please try again.",
    sectionLabel: "Task Roll",
  },
};

function detectModeFromKey(rawKey) {
  const normalized = (rawKey || "").trim().toUpperCase();
  if (normalized.startsWith(TASK_KEY_PREFIX)) {
    return "task";
  }
  return "reward";
}

function readQueryValue(name) {
  try {
    const params = new URLSearchParams(window.location.search);
    return (params.get(name) || "").trim().toUpperCase();
  } catch (_error) {
    return "";
  }
}

function initialKey() {
  const taskKey = readQueryValue("task_roll_key");
  if (taskKey) {
    return taskKey;
  }
  const rewardKey = readQueryValue("reward_key");
  if (rewardKey) {
    return rewardKey;
  }
  return "";
}

function initialMode() {
  const startingKey = initialKey();
  if (startingKey) {
    return detectModeFromKey(startingKey);
  }
  try {
    const params = new URLSearchParams(window.location.search);
    return params.get("mode") === "task" ? "task" : "reward";
  } catch (_error) {
    return "reward";
  }
}

function updateUrlWithKey(mode, key) {
  try {
    const nextMode = mode === "task" ? "task" : "reward";
    const cfg = MODE_CONFIG[nextMode];
    if (!cfg) {
      return;
    }
    const url = new URL(window.location.href);
    url.searchParams.delete("reward_key");
    url.searchParams.delete("task_roll_key");
    if (nextMode === "task") {
      url.searchParams.set("mode", "task");
    } else {
      url.searchParams.delete("mode");
    }
    url.searchParams.set(cfg.inputName, key.trim().toUpperCase());
    window.history.replaceState({}, "", url.toString());
  } catch (_error) {
    // no-op
  }
}

function buildResultDetails(mode, item) {
  if (!item) {
    return [];
  }

  const tierText = item.tier_label || item.tier || "-";
  if (mode === "task") {
    const taskDescription =
      String(item.task_description || item.description || "").trim() || "No description available.";
    const rows = [
      ["Task", item.display_value || item.label || "-"],
      ["Tier", tierText],
    ];
    if (item.points !== null && item.points !== undefined) {
      rows.push(["Points", String(item.points)]);
    }
    if (item.npc) {
      rows.push(["NPC", String(item.npc)]);
    }
    if (item.rerolls_remaining !== null && item.rerolls_remaining !== undefined) {
      rows.push(["Rerolls Left", String(item.rerolls_remaining)]);
    }
    rows.push(["Description", taskDescription]);
    return rows;
  }

  const rows = [
    ["Reward", item.display_value || item.label || "-"],
    ["Tier", tierText],
  ];
  if (item.kind) {
    rows.push(["Type", String(item.kind).toUpperCase()]);
  }
  return rows;
}

function readTranslateX(node) {
  if (!node || typeof window === "undefined") {
    return 0;
  }
  const transform = window.getComputedStyle(node).transform;
  if (!transform || transform === "none") {
    return 0;
  }
  try {
    if ("DOMMatrixReadOnly" in window) {
      return new window.DOMMatrixReadOnly(transform).m41;
    }
  } catch (_error) {
    // continue to parser fallback
  }

  const match = transform.match(/matrix(3d)?\(([^)]+)\)/i);
  if (!match) {
    return 0;
  }
  const values = match[2].split(",").map((value) => Number.parseFloat(value.trim()));
  if (match[1] === "3d") {
    return Number.isFinite(values[12]) ? values[12] : 0;
  }
  return Number.isFinite(values[4]) ? values[4] : 0;
}

function buildStartEdgeThresholds(strip, pointerX, startTx, targetTx) {
  if (!strip || !Number.isFinite(pointerX) || !Number.isFinite(startTx) || !Number.isFinite(targetTx)) {
    return [];
  }
  if (targetTx === startTx) {
    return [];
  }

  const movingLeft = targetTx < startTx;
  const minTx = Math.min(startTx, targetTx);
  const maxTx = Math.max(startTx, targetTx);
  const thresholds = [];

  for (const element of strip.children) {
    const cardOffset = movingLeft ? element.offsetLeft + element.clientWidth : element.offsetLeft;
    const thresholdTx = pointerX - cardOffset;
    if (!Number.isFinite(thresholdTx)) {
      continue;
    }
    if (thresholdTx < minTx || thresholdTx > maxTx) {
      continue;
    }
    thresholds.push(thresholdTx);
  }

  thresholds.sort((a, b) => (movingLeft ? b - a : a - b));
  return thresholds;
}

function randomLandingOffsetWithinCard(cardWidth) {
  if (!Number.isFinite(cardWidth) || cardWidth <= 0) {
    return 0;
  }

  const minByRatio = cardWidth * LANDING_INNER_MIN_RATIO;
  const maxByRatio = cardWidth * LANDING_INNER_MAX_RATIO;

  let minOffset = Math.max(LANDING_EDGE_GUTTER_PX, minByRatio);
  let maxOffset = Math.min(cardWidth - LANDING_EDGE_GUTTER_PX, maxByRatio);

  if (!(maxOffset > minOffset)) {
    minOffset = Math.max(0, cardWidth * 0.3);
    maxOffset = Math.min(cardWidth, cardWidth * 0.7);
    if (!(maxOffset > minOffset)) {
      return cardWidth / 2;
    }
  }

  return minOffset + Math.random() * (maxOffset - minOffset);
}

function CaseCard({ item, selected = false, revealed = false }) {
  const cardClass = ["card", item?.card_class || "white", selected ? "selected" : "", revealed ? "opened" : ""]
    .filter(Boolean)
    .join(" ");
  const imageUrl = item?.image_url || "";

  return (
    <article className={cardClass}>
      {imageUrl ? (
        <img className="card-image" src={imageUrl} alt="" />
      ) : (
        <div className="card-image-fallback" />
      )}
    </article>
  );
}

export default function App() {
  const [caseMode, setCaseMode] = useState(initialMode);
  const [inputKey, setInputKey] = useState(initialKey);
  const [statusMessage, setStatusMessage] = useState("");
  const [statusKind, setStatusKind] = useState("");
  const [loading, setLoading] = useState(false);
  const [payload, setPayload] = useState(null);
  const [spinState, setSpinState] = useState("idle");
  const [resultMode, setResultMode] = useState(null);

  const viewportRef = useRef(null);
  const stripRef = useRef(null);
  const spinTimerRef = useRef(null);
  const spinSoundFrameRef = useRef(null);
  const audioContextRef = useRef(null);

  const payloadMode = payload?.__mode || caseMode;
  const payloadModeConfig = useMemo(() => MODE_CONFIG[payloadMode] || MODE_CONFIG.reward, [payloadMode]);
  const selectedItem = payload?.[payloadModeConfig.itemField] || null;
  const selectedKey = payload?.[payloadModeConfig.keyField] || "";
  const resultDetails = useMemo(() => buildResultDetails(payloadMode, selectedItem), [payloadMode, selectedItem]);
  const taskUrl = useMemo(() => {
    if (payloadMode !== "task" || !selectedItem) {
      return "";
    }
    return String(selectedItem.task_url || "").trim();
  }, [payloadMode, selectedItem]);
  const bossUrl = useMemo(() => {
    if (payloadMode !== "task" || !selectedItem) {
      return "";
    }
    return String(selectedItem.boss_url || selectedItem.npc_url || "").trim();
  }, [payloadMode, selectedItem]);

  const ensureAudioContext = () => {
    if (typeof window === "undefined") {
      return null;
    }
    const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
    if (!AudioContextCtor) {
      return null;
    }
    if (!audioContextRef.current) {
      try {
        audioContextRef.current = new AudioContextCtor();
      } catch (_error) {
        return null;
      }
    }
    const context = audioContextRef.current;
    if (context.state === "suspended") {
      void context.resume().catch(() => {});
    }
    return context;
  };

  const playTickSound = () => {
    const context = audioContextRef.current;
    if (!context || context.state !== "running") {
      return;
    }
    try {
      const now = context.currentTime;
      const highPass = context.createBiquadFilter();
      highPass.type = "highpass";
      highPass.frequency.setValueAtTime(1400, now);
      highPass.Q.setValueAtTime(1.4, now);

      const gain = context.createGain();
      gain.gain.setValueAtTime(0.0001, now);
      gain.gain.exponentialRampToValueAtTime(0.09, now + 0.0015);
      gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.02);

      const body = context.createOscillator();
      body.type = "square";
      body.frequency.setValueAtTime(2600, now);
      body.frequency.exponentialRampToValueAtTime(1500, now + 0.014);

      const bite = context.createOscillator();
      bite.type = "triangle";
      bite.frequency.setValueAtTime(3800, now);
      bite.frequency.exponentialRampToValueAtTime(2200, now + 0.01);

      body.connect(highPass);
      bite.connect(highPass);
      highPass.connect(gain);
      gain.connect(context.destination);

      body.start(now);
      bite.start(now);
      body.stop(now + 0.022);
      bite.stop(now + 0.018);
    } catch (_error) {
      // Ignore audio failures so spins still work.
    }
  };

  const playLandingThunk = () => {
    const context = audioContextRef.current;
    if (!context || context.state !== "running") {
      return;
    }
    try {
      const now = context.currentTime;

      const thumpFilter = context.createBiquadFilter();
      thumpFilter.type = "lowpass";
      thumpFilter.frequency.setValueAtTime(240, now);
      thumpFilter.Q.setValueAtTime(1.2, now);

      const thumpGain = context.createGain();
      thumpGain.gain.setValueAtTime(0.0001, now);
      thumpGain.gain.exponentialRampToValueAtTime(0.18, now + 0.006);
      thumpGain.gain.exponentialRampToValueAtTime(0.0001, now + 0.24);

      const low = context.createOscillator();
      low.type = "sine";
      low.frequency.setValueAtTime(165, now);
      low.frequency.exponentialRampToValueAtTime(72, now + 0.2);

      const mid = context.createOscillator();
      mid.type = "triangle";
      mid.frequency.setValueAtTime(120, now);
      mid.frequency.exponentialRampToValueAtTime(60, now + 0.18);

      low.connect(thumpFilter);
      mid.connect(thumpFilter);
      thumpFilter.connect(thumpGain);
      thumpGain.connect(context.destination);

      const clickGain = context.createGain();
      clickGain.gain.setValueAtTime(0.0001, now);
      clickGain.gain.exponentialRampToValueAtTime(0.045, now + 0.001);
      clickGain.gain.exponentialRampToValueAtTime(0.0001, now + 0.028);

      const click = context.createOscillator();
      click.type = "square";
      click.frequency.setValueAtTime(1300, now);
      click.frequency.exponentialRampToValueAtTime(420, now + 0.025);

      click.connect(clickGain);
      clickGain.connect(context.destination);

      low.start(now);
      mid.start(now);
      click.start(now);
      low.stop(now + 0.26);
      mid.stop(now + 0.24);
      click.stop(now + 0.03);
    } catch (_error) {
      // Ignore audio failures so spins still work.
    }
  };

  useEffect(() => {
    return () => {
      if (spinTimerRef.current) {
        window.clearTimeout(spinTimerRef.current);
      }
      if (spinSoundFrameRef.current) {
        window.cancelAnimationFrame(spinSoundFrameRef.current);
        spinSoundFrameRef.current = null;
      }
      if (audioContextRef.current) {
        void audioContextRef.current.close().catch(() => {});
        audioContextRef.current = null;
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
    void ensureAudioContext();

    let soundActive = false;
    let previousTranslateX = 0;
    let soundRaf = 0;
    let thresholds = [];
    let thresholdIndex = 0;
    let movingLeft = false;
    let movingRight = false;

    const runTickSound = () => {
      if (!soundActive) {
        return;
      }
      const currentTranslateX = readTranslateX(strip);
      while (thresholdIndex < thresholds.length) {
        const thresholdTx = thresholds[thresholdIndex];
        const crossed = movingLeft
          ? previousTranslateX >= thresholdTx && currentTranslateX <= thresholdTx
          : movingRight
            ? previousTranslateX <= thresholdTx && currentTranslateX >= thresholdTx
            : false;
        if (!crossed) {
          break;
        }
        playTickSound();
        thresholdIndex += 1;
      }
      previousTranslateX = currentTranslateX;
      if (thresholdIndex >= thresholds.length) {
        soundActive = false;
        spinSoundFrameRef.current = null;
        return;
      }
      soundRaf = window.requestAnimationFrame(runTickSound);
      spinSoundFrameRef.current = soundRaf;
    };

    const rafA = window.requestAnimationFrame(() => {
      const rafB = window.requestAnimationFrame(() => {
        const selected = strip.children[payload.selected_index];
        if (!selected) {
          return;
        }
        const startTranslateX = readTranslateX(strip);
        const landingOffset = randomLandingOffsetWithinCard(selected.clientWidth);
        const pointerX = viewport.clientWidth / 2;
        const targetX = pointerX - (selected.offsetLeft + landingOffset);
        strip.style.transition = `transform ${SPIN_DURATION_MS / 1000}s ${SPIN_EASING}`;
        strip.style.transform = `translateX(${targetX}px)`;
        movingLeft = targetX < startTranslateX;
        movingRight = targetX > startTranslateX;
        thresholds = buildStartEdgeThresholds(
          strip,
          viewport.clientWidth / 2,
          startTranslateX,
          targetX,
        );
        thresholdIndex = 0;
        if (thresholds.length > 0 && (movingLeft || movingRight)) {
          soundActive = true;
          previousTranslateX = startTranslateX;
          soundRaf = window.requestAnimationFrame(runTickSound);
          spinSoundFrameRef.current = soundRaf;
        }
      });
      strip.dataset.rafB = String(rafB);
    });

    spinTimerRef.current = window.setTimeout(() => {
      const successConfig = MODE_CONFIG[payload.__mode] || MODE_CONFIG.reward;
      playLandingThunk();
      setSpinState("finished");
      setStatusMessage(payload.message || successConfig.successMessage);
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
      soundActive = false;
      if (soundRaf) {
        window.cancelAnimationFrame(soundRaf);
      }
      if (spinSoundFrameRef.current) {
        window.cancelAnimationFrame(spinSoundFrameRef.current);
        spinSoundFrameRef.current = null;
      }
    };
  }, [payload, resultMode, spinState]);

  async function handleSubmit(event) {
    event.preventDefault();
    if (loading || spinState === "spinning") {
      return;
    }
    const normalizedKey = inputKey.trim().toUpperCase();
    if (!normalizedKey) {
      setStatusMessage("Enter key here.");
      setStatusKind("error");
      return;
    }

    const requestMode = detectModeFromKey(normalizedKey);
    const requestConfig = MODE_CONFIG[requestMode] || MODE_CONFIG.reward;
    void ensureAudioContext();
    setCaseMode(requestMode);
    setLoading(true);
    setStatusMessage(requestConfig.openStatus);
    setStatusKind("");
    setPayload(null);
    setResultMode(null);
    setSpinState("idle");

    try {
      const response = await fetch(requestConfig.endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ [requestConfig.inputName]: normalizedKey }),
      });
      const nextPayload = await response.json();
      const selected = nextPayload?.[requestConfig.itemField] || null;
      updateUrlWithKey(requestMode, normalizedKey);

      if (nextPayload.status === "ok") {
        setPayload({ ...nextPayload, __mode: requestMode });
        setResultMode("ok");
        setSpinState("spinning");
        return;
      }

      if (nextPayload.status === "used" && selected) {
        setPayload({ ...nextPayload, __mode: requestMode });
        setResultMode("used");
        setSpinState("finished");
        setStatusMessage(nextPayload.message || requestConfig.usedMessage);
        setStatusKind("error");
        return;
      }

      setPayload(null);
      setResultMode(null);
      setSpinState("idle");
      setStatusMessage(nextPayload.message || requestConfig.errorMessage);
      setStatusKind("error");
    } catch (_error) {
      setPayload(null);
      setResultMode(null);
      setSpinState("idle");
      setStatusMessage(requestConfig.networkErrorMessage);
      setStatusKind("error");
    } finally {
      setLoading(false);
    }
  }

  const showCaseWrap = Boolean(payload);
  const reel = payload?.reel || [];
  const selectedIndex = payload?.selected_index ?? 0;
  const stripClass = spinState === "finished" ? "strip finished" : spinState === "spinning" ? "strip spinning" : "strip";
  const rolling = loading || spinState === "spinning";
  const resultClass = ["result", "active", selectedItem?.card_class || "white"].filter(Boolean).join(" ");
  const renderUsefulLinks = () => {
    if (!taskUrl && !bossUrl) {
      return null;
    }
    return (
      <div className="result-links">
        <div className="result-links-title">Useful links</div>
        <div className="result-links-items">
          {bossUrl ? (
            <a className="result-link" href={bossUrl} target="_blank" rel="noreferrer">
              Boss Wiki
            </a>
          ) : null}
          {taskUrl ? (
            <a className="result-link" href={taskUrl} target="_blank" rel="noreferrer">
              Task Wiki
            </a>
          ) : null}
        </div>
      </div>
    );
  };

  return (
    <main className="shell">
      <section className="hero">
        <h1>RNG Street Case Roller</h1>
        <p className="sub">Enter any key here (reward or task).</p>
      </section>

      <section className="content">
        <form className="form" onSubmit={handleSubmit}>
          <input
            id="case-key"
            name="case_key"
            type="text"
            placeholder="Enter key here"
            autoComplete="off"
            value={inputKey}
            onChange={(event) => setInputKey(event.target.value.toUpperCase())}
            disabled={rolling}
            required
          />
          <button type="submit" disabled={rolling}>
            {rolling ? "Rolling..." : "Open"}
          </button>
        </form>

        <div className={`status${statusKind ? ` ${statusKind}` : ""}`}>{statusMessage}</div>

        {showCaseWrap ? (
          <section className="case-wrap active">
            <div className="case-header">
              <span>{payloadModeConfig.sectionLabel}</span>
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
                  <div className={resultClass}>
                    {selectedItem.image_url ? (
                      <div className="result-media">
                        <img className="result-image" src={selectedItem.image_url} alt="" />
                      </div>
                    ) : null}
                    {resultDetails.length ? (
                      <div className="result-details">
                        {resultDetails.map(([label, value]) => (
                          <div className={`result-row${label === "Description" ? " long" : ""}`} key={label}>
                            <span className="result-label">{label}</span>
                            <span className="result-value">{value}</span>
                          </div>
                        ))}
                      </div>
                    ) : null}
                    {renderUsefulLinks()}
                  </div>
                ) : null}
              </>
            ) : (
              <div className={resultClass}>
                {selectedItem?.image_url ? (
                  <div className="result-media">
                    <img className="result-image" src={selectedItem.image_url} alt="" />
                  </div>
                ) : null}
                {resultDetails.length ? (
                  <div className="result-details">
                    {resultDetails.map(([label, value]) => (
                      <div className={`result-row${label === "Description" ? " long" : ""}`} key={label}>
                        <span className="result-label">{label}</span>
                        <span className="result-value">{value}</span>
                      </div>
                    ))}
                  </div>
                ) : null}
                {renderUsefulLinks()}
              </div>
            )}
          </section>
        ) : null}
      </section>
    </main>
  );
}
