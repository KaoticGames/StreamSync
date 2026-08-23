// Twitch connect/disconnect via overlay-server HTTP API.
// Works in Tauri (external localhost webview), Electron, and browser dev without IPC.
(function (global) {
  const SCRIPT_VERSION = "control-plane-1";
  console.log("[connections-api] loaded", SCRIPT_VERSION);

  function overlayBase() {
    if (global.STREAMSYNC_OVERLAY && global.STREAMSYNC_OVERLAY.baseUrl) {
      return String(global.STREAMSYNC_OVERLAY.baseUrl).replace(/\/$/, "");
    }
    if (global.location && /^https?:/.test(global.location.protocol)) {
      return global.location.origin;
    }
    return "http://127.0.0.1:4040";
  }

  function privilegedHeaders(extra) {
    const headers = Object.assign({ "Content-Type": "application/json" }, extra || {});
    if (global.STREAMSYNC_CONTROL_TOKEN) {
      headers["x-streamsync-control"] = global.STREAMSYNC_CONTROL_TOKEN;
    }
    return headers;
  }

  async function privilegedFetch(path, options) {
    const base = overlayBase();
    const opts = Object.assign({}, options || {});
    const headers = privilegedHeaders(opts.headers || {});
    opts.headers = headers;
    return fetch(`${base}${path}`, opts);
  }

  async function fetchAuthUrl() {
    const res = await privilegedFetch("/api/twitch/auth-url", { method: "GET", headers: {} });
    if (!res.ok) {
      const data = await res.json().catch(() => null);
      const detail =
        (data && (data.error || data.message)) ||
        (await res.text().catch(() => ""));
      throw new Error(
        detail
          ? String(detail)
          : `Auth URL failed: HTTP ${res.status}`
      );
    }
    const data = await res.json();
    const url = data && (data.url || data.authUrl);
    if (!url) throw new Error("No auth URL returned from overlay server");
    return url;
  }

  /** Open OAuth in the system browser (Tauri/Electron) or a new tab (browser dev). */
  async function openAuthUrl(url) {
    if (global.electronAPI?.openExternal) {
      await global.electronAPI.openExternal(url);
      return;
    }
    const opened = global.open(url, "_blank", "noopener,noreferrer");
    if (!opened) {
      throw new Error("Could not open your browser for sign-in. Try again.");
    }
  }

  async function connect() {
    const url = await fetchAuthUrl();
    await openAuthUrl(url);
  }

  async function reconnect() {
    return connect();
  }

  async function disconnect() {
    const res = await privilegedFetch("/api/twitch/disconnect", { method: "POST" });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`Disconnect failed: HTTP ${res.status} ${text}`.trim());
    }
  }

  async function kickFetchAuthUrl() {
    const res = await privilegedFetch("/api/kick/auth-url", { method: "GET", headers: {} });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.url) {
      throw new Error(data.error || data.message || `Kick auth URL failed: HTTP ${res.status}`);
    }
    return data.url;
  }

  async function kickConnect() {
    if (global.electronAPI?.kickConnect) {
      return global.electronAPI.kickConnect();
    }
    const url = await kickFetchAuthUrl();
    await openAuthUrl(url);
  }

  async function kickDisconnect() {
    const res = await privilegedFetch("/api/kick/disconnect", { method: "POST" });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`Kick disconnect failed: HTTP ${res.status} ${text}`.trim());
    }
  }

  async function useConnection(mode) {
    const res = await privilegedFetch("/api/twitch/use-connection", {
      method: "POST",
      body: JSON.stringify({ mode: String(mode || "").trim() }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.ok === false) {
      throw new Error(data.message || data.error || `HTTP ${res.status}`);
    }
    return data;
  }

  async function removeConnection(mode) {
    const res = await privilegedFetch("/api/twitch/remove-connection", {
      method: "POST",
      body: JSON.stringify({ mode: String(mode || "").trim() }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.ok === false) {
      throw new Error(data.message || data.error || `HTTP ${res.status}`);
    }
    return data;
  }

  async function connectWithKey(key) {
    const base = overlayBase();
    console.log("[connections-api] POST connection-key →", base);
    const res = await privilegedFetch("/api/twitch/connection-key", {
      method: "POST",
      body: JSON.stringify({ key: String(key || "").trim() }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.ok === false) {
      throw new Error(data.message || data.error || `HTTP ${res.status}`);
    }
    return data;
  }

  function showKeyError(msg) {
    const el = document.getElementById("twitch-connection-key-error");
    if (el) {
      el.textContent = msg || "";
      el.style.display = msg ? "block" : "none";
    } else if (msg) {
      alert(msg);
    }
  }

  let inflight = false;

  /**
   * Entry point for Connect-with-key. Used by document click delegation and Enter key.
   */
  async function streamSyncConnectWithKey(event) {
    if (event && typeof event.preventDefault === "function") event.preventDefault();
    if (inflight) {
      console.log("[Connections] Connect with key ignored (already in flight)");
      return;
    }
    console.log("[Connections] Connect with key clicked");
    showKeyError("");
    const input = document.getElementById("twitch-connection-key");
    const btn = document.getElementById("btn-twitch-connection-key");
    const key = input ? String(input.value || "").trim() : "";
    if (!key) {
      showKeyError("Paste a connection key first.");
      return;
    }
    inflight = true;
    if (btn) btn.disabled = true;
    try {
      const data = await connectWithKey(key);
      console.log("[Connections] Connection key OK", data);
      if (input) input.value = "";
      showKeyError("");
      if (typeof global.__streamSyncRefreshTwitchStatus === "function") {
        global.__streamSyncRefreshTwitchStatus();
      }
    } catch (err) {
      console.error("[Connections] Connection key failed:", err);
      showKeyError(err && err.message ? err.message : String(err));
    } finally {
      inflight = false;
      if (btn) btn.disabled = false;
    }
  }

  async function seGetSession() {
    const base = overlayBase();
    const res = await fetch(`${base}/api/streamelements/session`, { cache: "no-store" });
    if (!res.ok) throw new Error(`Session check failed: HTTP ${res.status}`);
    return res.json();
  }

  async function seSaveSession(accountId, jwt) {
    const res = await privilegedFetch("/api/streamelements/session", {
      method: "POST",
      body: JSON.stringify({ accountId, jwt }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `Save failed: HTTP ${res.status}`);
    return data;
  }

  async function seDisconnect() {
    const res = await privilegedFetch("/api/streamelements/session", { method: "DELETE", headers: {} });
    if (!res.ok) throw new Error(`Disconnect failed: HTTP ${res.status}`);
    return res.json().catch(() => ({}));
  }

  async function seOpenAccountPage() {
    if (global.electronAPI?.openSeAccountPage) {
      return global.electronAPI.openSeAccountPage();
    }
    const url = "https://streamelements.com/dashboard/account/channels";
    const opened = global.open(url, "_blank", "noopener,noreferrer");
    if (!opened) throw new Error("Popup blocked — allow popups for Stream Sync.");
  }

  // Capture-phase delegation: works for buttons injected later by renderer.js.
  // Does not depend on inline onclick or Connections view init.
  if (typeof document !== "undefined" && !global.__streamSyncKeyClickBound) {
    global.__streamSyncKeyClickBound = true;
    document.addEventListener(
      "click",
      function (ev) {
        const t = ev.target;
        if (!t || typeof t.closest !== "function") return;
        const btn = t.closest("#btn-twitch-connection-key");
        if (!btn) return;
        ev.preventDefault();
        ev.stopPropagation();
        streamSyncConnectWithKey(ev);
      },
      true
    );
    document.addEventListener(
      "keydown",
      function (ev) {
        if (ev.key !== "Enter") return;
        const t = ev.target;
        if (!t || t.id !== "twitch-connection-key") return;
        ev.preventDefault();
        streamSyncConnectWithKey(ev);
      },
      true
    );
  }

  global.streamSyncConnections = {
    overlayBase,
    connect,
    reconnect,
    disconnect,
    useConnection,
    removeConnection,
    connectWithKey,
    kickConnect,
    kickDisconnect,
    seGetSession,
    seSaveSession,
    seDisconnect,
    seOpenAccountPage,
  };
  global.streamSyncConnectWithKey = streamSyncConnectWithKey;
})(typeof window !== "undefined" ? window : global);
