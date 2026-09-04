// Alert delivery queue (events overlay). Pass 2: one FIFO queue + one worker.
// enqueue never starts playOne while another playOne is in flight.
(function (root) {
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
  };
})(typeof window !== "undefined" ? window : globalThis);
