// overlay-client-config.js
// Single source for overlay-server URL in the Electron UI (and browser dev).
// Set OVERLAY_PORT + STREAMSYNC_EXTERNAL_OVERLAY before `npm start` for Rust A/B.

(function (global) {
  function readPort() {
    if (global.streamSyncOverlay && global.streamSyncOverlay.port) {
      return Number(global.streamSyncOverlay.port);
    }
    return 4040;
  }

  function readBaseUrl() {
    if (global.streamSyncOverlay && global.streamSyncOverlay.baseUrl) {
      return String(global.streamSyncOverlay.baseUrl).replace(/\/$/, "");
    }
    // Prefer the page origin when the UI is served from the overlay server
    // (127.0.0.1 vs localhost is a different origin for ES module imports).
    if (global.location && /^https?:/i.test(global.location.protocol || "")) {
      const host = global.location.hostname;
      if (host === "127.0.0.1" || host === "localhost") {
        return global.location.origin.replace(/\/$/, "");
      }
    }
    const port = readPort();
    return `http://127.0.0.1:${port}`;
  }

  function wsBaseUrl() {
    return readBaseUrl().replace(/^http/i, "ws");
  }

  global.STREAMSYNC_OVERLAY = {
    get port() {
      return readPort();
    },
    get baseUrl() {
      return readBaseUrl();
    },
    get externalServer() {
      return !!(global.streamSyncOverlay && global.streamSyncOverlay.externalServer);
    },
    wsFeedUrl(profileId) {
      const p = encodeURIComponent(profileId || "default");
      return `${wsBaseUrl()}/ws/feed?profile=${p}`;
    },
    path(relativePath) {
      const rel = String(relativePath || "").replace(/^\//, "");
      return `${readBaseUrl()}/${rel}`;
    },
  };
})(typeof window !== "undefined" ? window : global);
