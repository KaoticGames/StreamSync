// Shared privileged HTTP helper for Stream Sync UI.
// Capability stays module-private — never put on window, never log, never put in URLs.
(function (global) {
  const CONTROL_HEADER = "x-streamsync-control";
  let privateCapability = "";
  let capabilityPromise = null;

  function overlayBase() {
    if (global.STREAMSYNC_OVERLAY && global.STREAMSYNC_OVERLAY.baseUrl) {
      return String(global.STREAMSYNC_OVERLAY.baseUrl).replace(/\/$/, "");
    }
    if (global.streamSyncOverlay && global.streamSyncOverlay.baseUrl) {
      return String(global.streamSyncOverlay.baseUrl).replace(/\/$/, "");
    }
    if (global.location && /^https?:/.test(global.location.protocol)) {
      return global.location.origin;
    }
    return "http://127.0.0.1:4040";
  }

  async function resolveCapability() {
    if (privateCapability) return privateCapability;
    if (capabilityPromise) return capabilityPromise;
    capabilityPromise = (async () => {
      try {
        if (global.electronAPI && typeof global.electronAPI.getControlCapability === "function") {
          const token = await global.electronAPI.getControlCapability();
          if (token && String(token).length >= 32) {
            privateCapability = String(token);
            return privateCapability;
          }
        }
      } catch (_) {}
      return privateCapability;
    })();
    try {
      return await capabilityPromise;
    } finally {
      capabilityPromise = null;
    }
  }

  async function privilegedHeaders(extra) {
    const headers = Object.assign({}, extra || {});
    const token = await resolveCapability();
    if (token) headers[CONTROL_HEADER] = token;
    return headers;
  }

  async function privilegedFetch(path, options) {
    const opts = Object.assign({}, options || {});
    const baseHeaders = opts.headers || {};
    // Do not force Content-Type for FormData / multipart.
    const isForm =
      typeof FormData !== "undefined" && opts.body && opts.body instanceof FormData;
    const headers = await privilegedHeaders(baseHeaders);
    if (!isForm && opts.body && !headers["Content-Type"] && !headers["content-type"]) {
      headers["Content-Type"] = "application/json";
    }
    opts.headers = headers;
    const url = path.startsWith("http") ? path : `${overlayBase()}${path}`;
    const res = await fetch(url, opts);
    if (res.status === 401) {
      const err = new Error("unauthorized");
      err.code = "unauthorized";
      err.status = 401;
      throw err;
    }
    return res;
  }

  global.streamSyncControlApi = {
    overlayBase,
    privilegedFetch,
    privilegedHeaders,
    resolveCapability,
    /** Test hook: never exposes the raw token value. */
    hasCapability() {
      return privateCapability.length >= 32;
    },
  };
})(typeof window !== "undefined" ? window : globalThis);
