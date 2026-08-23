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
  let controlTokenPromise = null;

  async function ensureControlCapability(invoke) {
    if (window.STREAMSYNC_CONTROL_TOKEN) {
      return window.STREAMSYNC_CONTROL_TOKEN;
    }
    if (!controlTokenPromise) {
      controlTokenPromise = invoke("get_control_capability")
        .then((token) => {
          if (token) {
            window.STREAMSYNC_CONTROL_TOKEN = String(token);
          }
          return window.STREAMSYNC_CONTROL_TOKEN || "";
        })
        .catch((e) => {
          console.warn("[tauri-bridge] control capability:", e);
          return "";
        });
    }
    return controlTokenPromise;
  }

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
    ensureControlCapability(invoke);

    window.streamSyncOverlay = {
      get port() {
        return cachedPort;
      },
      get baseUrl() {
        return cachedBase;
      },
      externalServer: false,
    };

    async function twitchViaIpc(cmd) {
      return invoke(cmd);
    }

    async function twitchConnect() {
      try {
        await twitchViaIpc("twitch_connect");
      } catch (e) {
        console.warn("[tauri-bridge] IPC twitch_connect failed, using HTTP:", e);
        if (window.streamSyncConnections?.connect) {
          return window.streamSyncConnections.connect();
        }
        throw e;
      }
    }

    async function twitchReconnect() {
      try {
        await twitchViaIpc("twitch_reconnect");
      } catch (e) {
        console.warn("[tauri-bridge] IPC twitch_reconnect failed, using HTTP:", e);
        if (window.streamSyncConnections?.reconnect) {
          return window.streamSyncConnections.reconnect();
        }
        throw e;
      }
    }

    async function twitchDisconnect() {
      try {
        await twitchViaIpc("twitch_disconnect");
      } catch (e) {
        console.warn("[tauri-bridge] IPC twitch_disconnect failed, using HTTP:", e);
        if (window.streamSyncConnections?.disconnect) {
          return window.streamSyncConnections.disconnect();
        }
        throw e;
      }
    }

    async function kickConnect() {
      try {
        await invoke("kick_connect");
      } catch (e) {
        console.warn("[tauri-bridge] IPC kick_connect failed, using HTTP:", e);
        if (window.streamSyncConnections?.kickConnect) {
          return window.streamSyncConnections.kickConnect();
        }
        throw e;
      }
    }

    window.electronAPI = {
      getOverlayBaseUrl: () => cachedBase,
      getOverlayPort: () => cachedPort,
      getControlCapability: () => ensureControlCapability(invoke),
      isExternalOverlayServer: () => false,
      openExternal: (url) => invoke("open_external", { url }),
      openLogsFolder: () => invoke("open_logs_folder"),
      openDiscord: () => invoke("open_discord"),
      getSettings: async () => ({}),
      getTwitchStatus: async () => {
        const res = await fetch(`${cachedBase}/api/status`, { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      },
      twitchConnect,
      twitchReconnect,
      twitchDisconnect,
      kickConnect,
      purgeLogs: () => invoke("purge_logs"),
      checkForUpdates: () => invoke("check_for_updates"),
      openSeAccountPage: () => invoke("open_se_account_page"),
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
