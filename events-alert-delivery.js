// Alert delivery queue (events overlay).
// Pass 2: one FIFO queue + one worker.
// Pass 3: monotonic generation gate so stale async cannot mutate a newer alert.
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

  /**
   * @param {{ playOne: (alert: unknown) => Promise<unknown> | unknown }} options
   * @returns {{ enqueue: (alert: unknown) => Promise<unknown> }}
   */
  function createDelivery({ playOne }) {
    if (typeof playOne !== "function") {
      throw new TypeError("createDelivery requires playOne(alert)");
    }

    /** @type {{ alert: unknown, resolve: (v: unknown) => void, reject: (e: unknown) => void }[]} */
    const pending = [];
    let busy = false;

    function enqueue(alert) {
      return new Promise((resolve, reject) => {
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
  };
})(typeof window !== "undefined" ? window : globalThis);
