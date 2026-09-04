// Alert delivery queue surface (events overlay). Pass 1 models today's overlap:
// enqueue starts playOne immediately without waiting for a prior play to finish.
(function (root) {
  /**
   * @param {{ playOne: (alert: unknown) => Promise<unknown> | unknown }} options
   * @returns {{ enqueue: (alert: unknown) => Promise<unknown> }}
   */
  function createDelivery({ playOne }) {
    if (typeof playOne !== "function") {
      throw new TypeError("createDelivery requires playOne(alert)");
    }

    function enqueue(alert) {
      // Mirror events-overlay.js WS handler: call showAlert / play without await,
      // so rapid alerts start concurrently and clobber each other.
      return Promise.resolve(playOne(alert));
    }

    return { enqueue };
  }

  root.StreamSyncAlertDelivery = {
    createDelivery,
  };
})(typeof window !== "undefined" ? window : globalThis);
