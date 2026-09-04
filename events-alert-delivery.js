// Alert delivery queue (events overlay).
// Pass 2: one FIFO queue + one worker.
// Pass 3: monotonic generation gate so stale async cannot mutate a newer alert.
// Pass 5: clampDurationMs shared with overlay; queue survives WS reconnect.
(function (root) {
  /**
   * Monotonic play generation. Call begin() when an accepted play starts
   * (e.g. when hideAll clears the stage for a new alert). After each await /
   * in timer callbacks, isCurrent(gen) must be true before mutating DOM/audio.
   *
   * @returns {{ begin: () => number, isCurrent: (gen: number) => boolean }}
   */
  function createGenerationGate() {
    let current = 0;

    function begin() {
      current += 1;
      return current;
    }

    function isCurrent(gen) {
      return gen === current;
    }

    return { begin, isCurrent };
  }

  /** Overlay display duration bounds (ms). Matches events-overlay showAlert. */
  const DURATION_MS_MIN = 800;
  const DURATION_MS_MAX = 30000;

  /**
   * Clamp alert display duration to overlay bounds [800, 30000] ms.
   * @param {unknown} ms
   * @returns {number}
   */
  function clampDurationMs(ms) {
    const n = Number(ms);
    if (Number.isNaN(n)) return DURATION_MS_MIN;
    return Math.max(DURATION_MS_MIN, Math.min(DURATION_MS_MAX, n));
  }

  /** Default max waiting alerts (not including the active play). Drop-newest. */
  const MAX_PENDING = 32;

  /**
   * @param {{
   *   playOne: (alert: unknown) => Promise<unknown> | unknown,
   *   maxPending?: number,
   *   onDrop?: (info: { reason: string, eventType?: unknown, pending: number, alert: unknown }) => void,
   * }} options
   * @returns {{ enqueue: (alert: unknown) => Promise<unknown> }}
   */
  function createDelivery({ playOne, maxPending = MAX_PENDING, onDrop } = {}) {
    if (typeof playOne !== "function") {
      throw new TypeError("createDelivery requires playOne(alert)");
    }
    const limit =
      typeof maxPending === "number" && Number.isFinite(maxPending) && maxPending >= 0
        ? Math.floor(maxPending)
        : MAX_PENDING;

    /** @type {{ alert: unknown, resolve: (v: unknown) => void, reject: (e: unknown) => void }[]} */
    const pending = [];
    let busy = false;

    function enqueue(alert) {
      return new Promise((resolve, reject) => {
        // Drop-newest: never interrupt the active play; only reject when the
        // waiting queue is already full.
        if (pending.length >= limit) {
          const eventType =
            alert && typeof alert === "object" && "eventType" in alert
              ? /** @type {{ eventType?: unknown }} */ (alert).eventType
              : undefined;
          const info = {
            reason: "queue_full_drop_newest",
            eventType,
            pending: pending.length,
            alert,
          };
          console.warn(
            "queue_full_drop_newest",
            info.reason,
            info.eventType,
            info.pending
          );
          if (typeof onDrop === "function") {
            try {
              onDrop(info);
            } catch {
              /* ignore test/hook errors */
            }
          }
          const err = new Error("queue_full_drop_newest");
          err.name = "QueueFullDropNewest";
          reject(err);
          return;
        }
        pending.push({ alert, resolve, reject });
        drain();
      });
    }

    function drain() {
      if (busy) return;
      const next = pending.shift();
      if (!next) return;
      busy = true;
      Promise.resolve(playOne(next.alert)).then(
        (value) => {
          next.resolve(value);
          busy = false;
          drain();
        },
        (err) => {
          next.reject(err);
          busy = false;
          drain();
        }
      );
    }

    return { enqueue };
  }

  root.StreamSyncAlertDelivery = {
    createDelivery,
    createGenerationGate,
    clampDurationMs,
  };
})(typeof window !== "undefined" ? window : globalThis);
