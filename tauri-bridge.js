// Tauri desktop bridge — same surface as Electron preload `electronAPI` + `streamSyncOverlay`.
(function () {
  function getInvoke() {
    const tauri = window.__TAURI__;
    if (!tauri) return null;
    if (tauri.core && typeof tauri.core.invoke === "function") {
      return tauri.core.invoke.bind(tauri.core);
    }
    if (typeof tauri.invoke === "function") {
      return tauri.invoke.bind(tauri);
    }
    return null;
  }

  let cachedPort = 4040;
  let cachedBase = "http://127.0.0.1:4040";

  function installElectronApi(invoke) {
    if (!invoke || window.electronAPI) return;

    async function refreshOverlayInfo() {
      try {
        cachedPort = await invoke("get_overlay_port");
        cachedBase = await invoke("get_overlay_base_url");
      } catch (e) {
        console.warn("[tauri-bridge] overlay info:", e);
        if (window.location && /^https?:/.test(window.location.protocol)) {
          cachedBase = window.location.origin;
          try {
            const u = new URL(cachedBase);
            cachedPort = Number(u.port) || 4040;
          } catch (_) {}
        }
      }
    }

    refreshOverlayInfo();

    window.streamSyncOverlay = {
      get port() {
        return cachedPort;
      },
      get baseUrl() {
        return cachedBase;
      },
      externalServer: false,
    };

    async function twitchConnect() {
      if (window.streamSyncConnections?.connect) {
        return window.streamSyncConnections.connect();
      }
      throw new Error("Stream Sync connections API unavailable");
    }

    async function twitchReconnect() {
      if (window.streamSyncConnections?.reconnect) {
        return window.streamSyncConnections.reconnect();
      }
      throw new Error("Stream Sync connections API unavailable");
    }

    async function twitchDisconnect() {
      if (window.streamSyncConnections?.disconnect) {
        return window.streamSyncConnections.disconnect();
      }
      throw new Error("Stream Sync connections API unavailable");
    }

    async function kickConnect() {
      if (window.streamSyncConnections?.kickConnect) {
        return window.streamSyncConnections.kickConnect();
      }
      throw new Error("Stream Sync connections API unavailable");
    }

    window.electronAPI = {
      getOverlayBaseUrl: () => cachedBase,
      getOverlayPort: () => cachedPort,
      isExternalOverlayServer: () => false,
      openExternal: (url) => invoke("open_external", { url }),
      openLogsFolder: () => invoke("open_logs_folder"),
      openDiscord: () => invoke("open_discord"),
      getSettings: async () => ({}),
      getTwitchStatus: async () => {
        const api = window.streamSyncControlApi;
        if (!api || typeof api.privilegedFetch !== "function") {
          throw new Error("Stream Sync control API unavailable");
        }
        const res = await api.privilegedFetch("/api/status", { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      },
      twitchConnect,
      twitchReconnect,
      twitchDisconnect,
      kickConnect,
      purgeLogs: () => invoke("purge_logs"),
      checkForUpdates: () => invoke("check_for_updates"),
      openSeAccountPage: (flow) => invoke("open_se_account_page", { flow }),
      exportBackup: () => invoke("export_backup"),
    };

    console.log("[tauri-bridge] Stream Sync desktop APIs ready", cachedBase);
  }

  function tryInstall() {
    const invoke = getInvoke();
    if (invoke) {
      installElectronApi(invoke);
      return true;
    }
    return false;
  }

  if (!tryInstall()) {
    let attempts = 0;
    const timer = setInterval(() => {
      attempts += 1;
      if (tryInstall() || attempts >= 80) {
        clearInterval(timer);
      }
    }, 50);
  }
})();
