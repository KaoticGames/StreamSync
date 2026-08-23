// renderer.js

document.addEventListener("DOMContentLoaded", () => {
  const appRoot = document.getElementById("app-root");
  const navButtons = document.querySelectorAll(".nav-btn");

  const OVERLAY_BASE_URL = (() => {
    const fromCfg =
      window.STREAMSYNC_OVERLAY && window.STREAMSYNC_OVERLAY.baseUrl
        ? String(window.STREAMSYNC_OVERLAY.baseUrl).replace(/\/$/, "")
        : "";
    if (fromCfg) return fromCfg;
    if (window.location && /^https?:/i.test(window.location.protocol || "")) {
      return window.location.origin.replace(/\/$/, "");
    }
    return "http://127.0.0.1:4040";
  })();

  function showAbBannerIfNeeded() {
    if (!window.streamSyncOverlay || !window.streamSyncOverlay.externalServer) {
      return;
    }
    if (document.getElementById("streamsync-ab-banner")) return;
    const banner = document.createElement("div");
    banner.id = "streamsync-ab-banner";
    banner.style.cssText =
      "background:#1e3a5f;color:#e2e8f0;padding:8px 16px;font-size:13px;text-align:center;border-bottom:1px solid #334155;";
    banner.textContent = `A/B mode: UI → ${OVERLAY_BASE_URL} (external overlay server — start with npm run rust:server)`;
    const container = document.getElementById("app-container");
    const header = document.getElementById("app-header");
    if (container && header) {
      container.insertBefore(banner, header);
    }
  }
  showAbBannerIfNeeded();

  const dockCredentialPromises = new Map();

  function patchIntegrationUrlInputs() {
    const base = OVERLAY_BASE_URL;

    async function privilegedDockUrl(path, platform, profileId) {
      const key = `${platform}:${profileId}`;
      if (!dockCredentialPromises.has(key)) {
        dockCredentialPromises.set(
          key,
          window.streamSyncControlApi
            .privilegedFetch("/api/dock/issue-credential", {
              method: "POST",
              body: JSON.stringify({ platform, profileId }),
            })
            .then(async (res) => {
              const data = await res.json().catch(() => ({}));
              const dockToken = String(data.token || "");
              if (!res.ok || !data.ok || !dockToken.startsWith("ssd_")) {
                throw new Error(data.error || `Dock credential failed: HTTP ${res.status}`);
              }
              return dockToken;
            })
            .catch((err) => {
              dockCredentialPromises.delete(key);
              throw err;
            })
        );
      }
      const dockToken = await dockCredentialPromises.get(key);
      return `${base}${path}#control=${encodeURIComponent(dockToken)}`;
    }

    const tasks = [];
    document.querySelectorAll("[data-overlay-url-kind]").forEach((el) => {
      const kind = el.getAttribute("data-overlay-url-kind");
      const profile =
        el.getAttribute("data-overlay-profile") || "chat-default";
      if (kind === "chat-dock") {
        tasks.push(
          privilegedDockUrl("/dock/chat", "twitch", "chat-default").then((url) => {
            el.value = url;
          })
        );
        return;
      }
      if (kind === "kick-chat-dock") {
        tasks.push(
          privilegedDockUrl("/dock/kick-chat", "kick", "chat-default").then((url) => {
            el.value = url;
          })
        );
        return;
      }
      if (kind === "events-dock") {
        tasks.push(
          privilegedDockUrl(
            "/dock/events",
            "twitch",
            el.getAttribute("data-overlay-profile") || "default"
          ).then((url) => {
            el.value = url;
          })
        );
        return;
      }
      if (kind === "kick-events-dock") {
        tasks.push(
          privilegedDockUrl(
            "/dock/kick-events",
            "kick",
            el.getAttribute("data-overlay-profile") || "default"
          ).then((url) => {
            el.value = url;
          })
        );
        return;
      }
      if (kind === "chat-overlay") {
        el.value = `${base}/overlay/chat?profile=${encodeURIComponent(profile)}`;
      }
      if (kind === "events-overlay") {
        el.value = `${base}/overlay/events?profile=${encodeURIComponent(
          el.getAttribute("data-overlay-profile") || "default"
        )}`;
      }
      if (kind === "kick-chat-overlay") el.value = `${base}/overlay/kick-chat`;
      if (kind === "kick-events-overlay") el.value = `${base}/overlay/kick-events`;
    });
    return Promise.all(tasks);
  }

  // Map top-level views to their HTML partials
  const viewMap = {
    chat: "views/chat.html",
    events: "views/events.html",
    connections: "views/connections.html",
    help: "views/help.html",
    about: "views/about.html",
  };

  let currentView = null;
  let twitchStatusPollTimer = null;

  // Load a top-level view into #app-root
  async function loadView(viewName) {
    if (viewName === currentView) return;
    currentView = viewName;

    if (twitchStatusPollTimer) {
      clearInterval(twitchStatusPollTimer);
      twitchStatusPollTimer = null;
    }

    // Update active nav styling
    navButtons.forEach((btn) => {
      const btnView = btn.getAttribute("data-view");
      btn.classList.toggle("active", btnView === viewName);
    });

    const path = viewMap[viewName] || viewMap.chat;

    try {
      const res = await fetch(path, { cache: "no-cache" });
      if (!res.ok) throw new Error(`HTTP ${res.status} loading ${path}`);
      const html = await res.text();
      appRoot.innerHTML = html;
      await patchIntegrationUrlInputs();

      // Run per-view init
      switch (viewName) {
        case "chat":
          initChatView();
          break;
        case "events":
          initEventsView();
          break;
        case "connections":
          initConnectionsView();
          break;
        case "help":
          initHelpView();
          break;
        case "about":
          initAboutView();
          break;
        default:
          break;
      }
    } catch (err) {
      console.error("Failed to load view:", viewName, err);
      appRoot.innerHTML = `
        <div class="view-error">
          <h2>Failed to load "${viewName}"</h2>
          <p>${String(err)}</p>
        </div>
      `;
    }
  }

  // ---------- GLOBAL: Copy buttons for ANY partial ----------
  // Any element with data-copy-target="#selector" will copy the value of that input.
  document.addEventListener("click", (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;

    const copyBtn = target.closest("[data-copy-target]");
    if (!copyBtn) return;

    const selector = copyBtn.getAttribute("data-copy-target");
    if (!selector) return;

    const input = appRoot.querySelector(selector);
    if (!input || !(input instanceof HTMLInputElement)) return;

    input.select();
    input.setSelectionRange(0, 99999);

    try {
      document.execCommand("copy");
      // Optional: visual feedback added here
      const original = copyBtn.textContent || "Copy";
      copyBtn.textContent = "Copied!";
      setTimeout(() => {
        copyBtn.textContent = original;
      }, 1200);
    } catch (err) {
      console.warn("Copy failed", err);
    }
  });

  // ---------- CHAT VIEW INIT ----------
  function initChatView() {
    const viewEl = document.querySelector(".view-chat");
    if (!viewEl) return;

    const subnavBtns = viewEl.querySelectorAll(".subnav-btn");
    const subviews = viewEl.querySelectorAll(".subview");

    // Track whether we've initialized these once *for this injected view*
    let didInitDock = false;
    let didInitOverlay = false;

    function ensureDockInit() {
      if (didInitDock) return;
      didInitDock = true;

      if (window.initChatDockConfig) {
        try {
          window.initChatDockConfig();
        } catch (e) {
          console.error("[renderer] initChatDockConfig threw:", e);
        }
      } else {
        console.warn("[renderer] initChatDockConfig not found on window");
      }
    }

    function ensureOverlayInit() {
      if (didInitOverlay) {
        // Even if overlay is already initialized, keep integrations dropdown fresh
        // (profiles can change while staying in the app).
        if (typeof window.refreshChatIntegrationsProfiles === "function") {
          try {
            window.refreshChatIntegrationsProfiles();
          } catch (e) {
            console.warn("[renderer] refreshChatIntegrationsProfiles threw:", e);
          }
        }
        return;
      }

      if (!window.initChatOverlayConfig) {
        console.warn("[renderer] initChatOverlayConfig not found on window");
        return;
      }

      // Some overlay code uses binding guards. Re-calling is safe in most cases.
      // But we still treat this as "initialized" for the injected view.
      Promise.resolve()
        .then(() => window.initChatOverlayConfig())
        .then(() => {
          didInitOverlay = true;

          // Once overlay init exists, it should populate integrations dropdown too.
          // But we also offer a refresh hook for later.
          if (typeof window.refreshChatIntegrationsProfiles === "function") {
            try {
              window.refreshChatIntegrationsProfiles();
            } catch (_) {}
          }
        })
        .catch((e) => {
          console.error("[renderer] initChatOverlayConfig failed:", e);
        });
    }

    function showChatSubview(targetId) {
      subviews.forEach((sv) => {
        const isTarget = sv.id === targetId;
        sv.classList.toggle("active", isTarget);
        sv.classList.toggle("hidden", !isTarget);
      });

      subnavBtns.forEach((btn) => {
        const subViewName = btn.getAttribute("data-subview");
        btn.classList.toggle("active", subViewName === targetId);
      });

      // Lazy-init when the panel becomes relevant/visible
      if (targetId === "chat-dock") ensureDockInit();

      // NOTE:
      // - Integrations needs the profile dropdown populated too
      // - Overlay tab obviously needs it
      if (targetId === "chat-overlay" || targetId === "chat-integrations") {
        ensureOverlayInit();
      }
    }

    subnavBtns.forEach((btn) => {
      btn.addEventListener("click", () => {
        const target = btn.getAttribute("data-subview");
        if (target) showChatSubview(target);
      });
    });

    // Default subview
    showChatSubview("chat-integrations");

    // Dock init is cheap; optionally init immediately:
    // ensureDockInit();
  }

  // ---------- EVENTS VIEW INIT ----------
  function initEventsView() {
    const viewEl = document.querySelector(".view-events");
    if (!viewEl) return;

    const subnavBtns = viewEl.querySelectorAll(".subnav-events .subnav-btn");
    const subviewContainer = viewEl.querySelector(".events-subview-container");

    const subviewMap = {
      "events-integrations": "views/events/events-integrations.html",
      "events-dock": "views/events/events-dock.html",
      "events-overlay": "views/events/events-overlay.html",
    };

    async function loadEventsSubview(subviewId, opts = {}) {
      const path = subviewMap[subviewId];
      if (!path) return;

      if (opts.profileId) {
        window.__STREAMSYNC_EVENTS_PROFILE__ = String(opts.profileId).trim();
      }

      // Update subnav active state
      subnavBtns.forEach((btn) => {
        const btnId = btn.getAttribute("data-subview");
        btn.classList.toggle("active", btnId === subviewId);
      });

      try {
        const res = await fetch(path, { cache: "no-cache" });
        if (!res.ok) throw new Error(`HTTP ${res.status} loading ${path}`);
        const html = await res.text();
        subviewContainer.innerHTML = html;
        await patchIntegrationUrlInputs();

        // Run per-subview init
        const profileId =
          (opts.profileId || window.__STREAMSYNC_EVENTS_PROFILE__ || "default").trim() ||
          "default";

        if (subviewId === "events-integrations") {
          if (window.initEventsIntegrationsConfig) {
            window.initEventsIntegrationsConfig();
          } else {
            console.warn("[renderer] initEventsIntegrationsConfig not found on window");
          }
          if (opts.profileId && window.streamSyncSelectEventsProfile) {
            setTimeout(() => window.streamSyncSelectEventsProfile(profileId), 0);
          }
        }
        if (subviewId === "events-dock") {
          if (window.initEventsDockConfig) {
            window.initEventsDockConfig();
          } else {
            console.warn("[renderer] initEventsDockConfig not found on window");
          }
        }
        if (subviewId === "events-overlay") {
          if (window.initEventsOverlayStudio) {
            window.initEventsOverlayStudio(profileId);
          } else {
            console.warn("[renderer] initEventsOverlayStudio not found on window");
          }
        }
      } catch (err) {
        console.error("Failed to load events subview", subviewId, err);
        subviewContainer.innerHTML = `
          <div class="view-error">
            <h3>Failed to load panel</h3>
            <p>${String(err)}</p>
          </div>
        `;
      }
    }

    // Wire subnav buttons
    subnavBtns.forEach((btn) => {
      btn.addEventListener("click", () => {
        const target = btn.getAttribute("data-subview");
        if (target) loadEventsSubview(target);
      });
    });

    if (window.initEventsSeImport) {
      window.initEventsSeImport();
    } else {
      console.warn("[renderer] initEventsSeImport not found on window");
    }

    window.__streamSyncLoadEventsSubview = loadEventsSubview;

    // Default to Integrations
    loadEventsSubview("events-integrations");
  }

  // ---------- CONNECTIONS VIEW INIT ----------
  async function initConnectionsView() {
    const viewEl = document.querySelector(".view-connections");
    if (!viewEl) return;

    const container = viewEl.querySelector(".connections-platforms-container");
    if (!container) return;

    // Stop any previous status polling when switching views
    if (twitchStatusPollTimer) {
      clearInterval(twitchStatusPollTimer);
      twitchStatusPollTimer = null;
    }

    // For now we only have Twitch. Later we can add other platforms here.
    const platformPartials = [
      "views/connections/twitch-connection.html",
      "views/connections/streamelements-connection.html",
    ];

    async function loadPlatforms() {
      container.innerHTML = ""; // clear

      for (const path of platformPartials) {
        try {
          const res = await fetch(path, { cache: "no-cache" });
          if (!res.ok) throw new Error(`HTTP ${res.status} loading ${path}`);
          const html = await res.text();

          const wrapper = document.createElement("div");
          wrapper.innerHTML = html.trim();
          const root = wrapper.firstElementChild;
          if (root) container.appendChild(root);
        } catch (err) {
          console.error("Failed to load connection partial:", path, err);
          const errorDiv = document.createElement("div");
          errorDiv.className = "view-error";
          errorDiv.innerHTML = `
            <h3>Failed to load connection card</h3>
            <p>${String(err)}</p>
          `;
          container.appendChild(errorDiv);
        }
      }
    }

    await loadPlatforms();

    // After partials are in the DOM, wire Twitch controls and start status polling
    const statusValue = document.getElementById("twitch-status-value");
    const btnConnect = document.getElementById("btn-twitch-connect");
    const btnReconnect = document.getElementById("btn-twitch-reconnect");
    const btnDisconnect = document.getElementById("btn-twitch-disconnect");
    const keyInput = document.getElementById("twitch-connection-key");
    const btnKeyConnect = document.getElementById("btn-twitch-connection-key");
    const keyError = document.getElementById("twitch-connection-key-error");
    const takeoverWarning = document.getElementById("twitch-takeover-warning");
    const savedConnections = document.getElementById("twitch-saved-connections");
    const accountLocal = document.getElementById("twitch-account-local");
    const accountDelegated = document.getElementById("twitch-account-delegated");
    const accountLocalTitle = document.getElementById("twitch-account-local-title");
    const accountLocalMeta = document.getElementById("twitch-account-local-meta");
    const accountDelegatedTitle = document.getElementById("twitch-account-delegated-title");
    const accountDelegatedMeta = document.getElementById("twitch-account-delegated-meta");
    const btnUseLocal = document.getElementById("btn-twitch-use-local");
    const btnUseDelegated = document.getElementById("btn-twitch-use-delegated");
    const btnRemoveLocal = document.getElementById("btn-twitch-remove-local");
    const btnRemoveDelegated = document.getElementById("btn-twitch-remove-delegated");

    function applyTwitchStatus(status) {
      if (!statusValue) return;

      const twitch = status && status.twitch ? status.twitch : {};
      const accounts = twitch.accounts || {};
      const localAcc = accounts.local || {};
      const delegatedAcc = accounts.delegated || {};
      const connected = !!twitch.connected;
      const login = twitch.login || twitch.channel || "";
      const takeover = !!twitch.takeover || twitch.mode === "delegated";
      const label = twitch.label ? ` · ${twitch.label}` : "";
      const localSaved = !!localAcc.saved;
      const delegatedSaved = !!delegatedAcc.saved;

      if (connected) {
        statusValue.textContent = login
          ? takeover
            ? `Connected to ${login} (takeover)${label}`
            : `Connected to ${login}`
          : takeover
            ? "Connected (takeover)"
            : "Connected";
      } else if (localSaved || delegatedSaved) {
        statusValue.textContent = "Saved — pick a connection below";
      } else {
        statusValue.textContent = "Not connected";
      }

      if (takeoverWarning) {
        takeoverWarning.style.display = connected && takeover ? "block" : "none";
      }

      if (savedConnections) {
        savedConnections.style.display =
          localSaved || delegatedSaved ? "block" : "none";
      }
      if (accountLocal) {
        accountLocal.style.display = localSaved ? "flex" : "none";
        if (localSaved) {
          const name = localAcc.login || "Your Twitch account";
          if (accountLocalTitle) {
            accountLocalTitle.textContent = localAcc.active
              ? `${name} · In use`
              : name;
          }
          if (accountLocalMeta) {
            accountLocalMeta.textContent = "Personal Twitch login";
          }
        }
      }
      if (accountDelegated) {
        accountDelegated.style.display = delegatedSaved ? "flex" : "none";
        if (delegatedSaved) {
          const name =
            delegatedAcc.display_name ||
            delegatedAcc.login ||
            "Takeover channel";
          const keyLabel = delegatedAcc.label ? ` · ${delegatedAcc.label}` : "";
          if (accountDelegatedTitle) {
            accountDelegatedTitle.textContent = delegatedAcc.active
              ? `${name}${keyLabel} · In use`
              : `${name}${keyLabel}`;
          }
          if (accountDelegatedMeta) {
            accountDelegatedMeta.textContent = "Connection key (takeover)";
          }
        }
      }
      if (btnUseLocal) {
        btnUseLocal.style.display =
          localSaved && !localAcc.active ? "inline-flex" : "none";
      }
      if (btnUseDelegated) {
        btnUseDelegated.style.display =
          delegatedSaved && !delegatedAcc.active ? "inline-flex" : "none";
      }

      // Connect adds/refreshes personal OAuth; allow while on takeover so both can be saved.
      if (btnConnect) {
        btnConnect.disabled = connected && !takeover;
      }
      if (btnReconnect) {
        btnReconnect.disabled = takeover || (!connected && !localSaved);
      }
      // Disconnect removes the active identity (falls back to the other if saved).
      if (btnDisconnect) {
        btnDisconnect.disabled = !connected && !localSaved && !delegatedSaved;
      }
      if (btnKeyConnect) {
        btnKeyConnect.disabled = false;
      }
      if (keyInput) {
        keyInput.disabled = false;
      }
    }

    const kickStatusValue = document.getElementById("kick-status-value");
    const btnKickConnect = document.getElementById("btn-kick-connect");
    const btnKickReconnect = document.getElementById("btn-kick-reconnect");
    const btnKickDisconnect = document.getElementById("btn-kick-disconnect");

    function applyKickStatus(status) {
      if (!kickStatusValue) return;
      const kick = (status && status.kick) || {};
      const connected = !!kick.connected;
      const login = kick.login || "";
      const viaTakeover = !!kick.viaTakeover;
      if (connected) {
        kickStatusValue.textContent = login
          ? viaTakeover
            ? `Connected to ${login} (takeover)`
            : `Connected to ${login}`
          : viaTakeover
            ? "Connected (takeover)"
            : "Connected";
      } else {
        kickStatusValue.textContent = "Not connected";
      }
      if (btnKickConnect) btnKickConnect.disabled = connected && !viaTakeover;
      if (btnKickReconnect) btnKickReconnect.disabled = !connected && !kick.personalSaved;
      if (btnKickDisconnect) btnKickDisconnect.disabled = !kick.personalSaved;
    }

    async function fetchTwitchStatusOnce() {
      try {
        const res = await window.streamSyncControlApi.privilegedFetch("/api/status", {
          cache: "no-cache",
        });
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const data = await res.json();
        applyTwitchStatus(data);
        applyKickStatus(data);
      } catch (err) {
        console.warn("[Connections] Failed to fetch Twitch status:", err);
        // On failure, keep whatever UI state we had.
      }
    }

    async function runTwitchAction(label, fn) {
      try {
        await fn();
      } catch (err) {
        console.error(`[Connections] ${label} failed:`, err);
        alert(`${label} failed: ${err?.message || err}`);
      }
    }

    async function twitchConnectAction() {
      if (window.electronAPI?.twitchConnect) {
        return window.electronAPI.twitchConnect();
      }
      if (window.streamSyncConnections?.connect) {
        return window.streamSyncConnections.connect();
      }
      throw new Error("No connect handler (overlay API unavailable)");
    }

    async function twitchReconnectAction() {
      if (window.electronAPI?.twitchReconnect) {
        return window.electronAPI.twitchReconnect();
      }
      if (window.streamSyncConnections?.reconnect) {
        return window.streamSyncConnections.reconnect();
      }
      throw new Error("No reconnect handler (overlay API unavailable)");
    }

    async function twitchDisconnectAction() {
      if (window.electronAPI?.twitchDisconnect) {
        await window.electronAPI.twitchDisconnect();
      } else if (window.streamSyncConnections?.disconnect) {
        await window.streamSyncConnections.disconnect();
      } else {
        throw new Error("No disconnect handler (overlay API unavailable)");
      }
      setTimeout(fetchTwitchStatusOnce, 1000);
    }

    async function twitchUseConnection(mode) {
      if (!window.streamSyncConnections?.useConnection) {
        throw new Error("Switch connection API unavailable");
      }
      await window.streamSyncConnections.useConnection(mode);
      setTimeout(fetchTwitchStatusOnce, 400);
    }

    async function twitchRemoveConnection(mode) {
      if (!window.streamSyncConnections?.removeConnection) {
        throw new Error("Remove connection API unavailable");
      }
      const label =
        mode === "delegated" ? "takeover connection" : "personal Twitch login";
      if (!confirm(`Remove the saved ${label}?`)) return;
      await window.streamSyncConnections.removeConnection(mode);
      setTimeout(fetchTwitchStatusOnce, 400);
    }

    if (btnConnect) {
      btnConnect.addEventListener("click", () => {
        runTwitchAction("Connect", twitchConnectAction);
      });
    }

    if (btnReconnect) {
      btnReconnect.addEventListener("click", () => {
        runTwitchAction("Reconnect", twitchReconnectAction);
      });
    }

    if (btnDisconnect) {
      btnDisconnect.addEventListener("click", () => {
        runTwitchAction("Disconnect", twitchDisconnectAction);
      });
    }

    if (btnUseLocal) {
      btnUseLocal.addEventListener("click", () => {
        runTwitchAction("Switch to personal", () => twitchUseConnection("local"));
      });
    }
    if (btnUseDelegated) {
      btnUseDelegated.addEventListener("click", () => {
        runTwitchAction("Switch to takeover", () =>
          twitchUseConnection("delegated")
        );
      });
    }
    if (btnRemoveLocal) {
      btnRemoveLocal.addEventListener("click", () => {
        runTwitchAction("Remove personal", () => twitchRemoveConnection("local"));
      });
    }
    if (btnRemoveDelegated) {
      btnRemoveDelegated.addEventListener("click", () => {
        runTwitchAction("Remove takeover", () =>
          twitchRemoveConnection("delegated")
        );
      });
    }

    if (btnKeyConnect) {
      // Primary path is capture-phase click in connections-api.js.
      // Do not add a second click listener here — it would double-submit.
    }

    if (keyInput) {
      keyInput.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter") {
          ev.preventDefault();
          if (typeof window.streamSyncConnectWithKey === "function") {
            window.streamSyncConnectWithKey(ev);
          }
        }
      });
    }

    window.__streamSyncRefreshTwitchStatus = () => {
      setTimeout(fetchTwitchStatusOnce, 300);
    };

    async function kickConnectAction() {
      if (window.electronAPI?.kickConnect) {
        await window.electronAPI.kickConnect();
      } else if (window.streamSyncConnections?.kickConnect) {
        await window.streamSyncConnections.kickConnect();
      } else {
        throw new Error("Kick connect is unavailable");
      }
      setTimeout(fetchTwitchStatusOnce, 1500);
    }

    async function kickDisconnectAction() {
      if (!window.streamSyncConnections?.kickDisconnect) {
        throw new Error("Kick disconnect is unavailable");
      }
      await window.streamSyncConnections.kickDisconnect();
      setTimeout(fetchTwitchStatusOnce, 400);
    }

    if (btnKickConnect) {
      btnKickConnect.addEventListener("click", () => {
        runTwitchAction("Kick connect", kickConnectAction);
      });
    }
    if (btnKickReconnect) {
      btnKickReconnect.addEventListener("click", () => {
        runTwitchAction("Kick reconnect", kickConnectAction);
      });
    }
    if (btnKickDisconnect) {
      btnKickDisconnect.addEventListener("click", () => {
        runTwitchAction("Kick disconnect", kickDisconnectAction);
      });
    }

    console.log("[Connections] Twitch card wired", {
      hasKeyButton: !!btnKeyConnect,
      hasKeyInput: !!keyInput,
      hasConnectWithKey: typeof window.streamSyncConnectWithKey === "function",
    });

    // Initial status fetch + polling
    await fetchTwitchStatusOnce();

    twitchStatusPollTimer = setInterval(() => {
      fetchTwitchStatusOnce();
    }, 5000);

    // StreamElements connection card
    const seStatusValue = document.getElementById("se-status-value");
    const seAccountInput = document.getElementById("se-account-id");
    const seJwtInput = document.getElementById("se-jwt-token");
    const btnSeOpen = document.getElementById("btn-se-open-account");
    const btnSeSave = document.getElementById("btn-se-save");
    const btnSeDisconnect = document.getElementById("btn-se-disconnect");

    function applySeStatus(sess) {
      if (!seStatusValue) return;
      const connected = !!(sess && (sess.connected || sess.accountId));
      if (connected) {
        const label =
          (sess.username && String(sess.username).trim()) ||
          (sess.displayName && String(sess.displayName).trim()) ||
          "";
        seStatusValue.textContent = label ? `Connected (${label})` : "Connected";
      } else {
        seStatusValue.textContent = "Not connected";
      }
      if (btnSeDisconnect) btnSeDisconnect.disabled = !connected;
      if (btnSeSave) btnSeSave.disabled = false;
    }

    async function fetchSeSessionOnce() {
      try {
        if (window.streamSyncConnections?.seGetSession) {
          const sess = await window.streamSyncConnections.seGetSession();
          applySeStatus(sess);
          return;
        }
        const res = await window.streamSyncControlApi.privilegedFetch(
          "/api/streamelements/session",
          {
            cache: "no-cache",
          }
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        applySeStatus(await res.json());
      } catch (err) {
        console.warn("[Connections] Failed to fetch SE session:", err);
      }
    }

    async function runSeAction(label, fn) {
      try {
        await fn();
        await fetchSeSessionOnce();
        if (window.refreshSeImportButton) window.refreshSeImportButton();
      } catch (err) {
        console.error(`[Connections] ${label} failed:`, err);
        alert(`${label} failed: ${err?.message || err}`);
      }
    }

    const SE_ACCOUNT_URL = "https://streamelements.com/dashboard/account/channels";

    async function openSeAccountPageAction() {
      if (window.electronAPI?.openSeAccountPage) {
        try {
          return await window.electronAPI.openSeAccountPage();
        } catch (err) {
          console.warn("[Connections] openSeAccountPage IPC failed:", err);
        }
      }
      if (typeof window.streamSyncConnections?.seOpenAccountPage === "function") {
        return window.streamSyncConnections.seOpenAccountPage();
      }
      if (window.electronAPI?.openExternal) {
        return window.electronAPI.openExternal(SE_ACCOUNT_URL);
      }
      const opened = window.open(SE_ACCOUNT_URL, "_blank", "noopener,noreferrer");
      if (!opened) {
        throw new Error("Popup blocked — allow popups for Stream Sync and try again.");
      }
    }

    if (btnSeOpen) {
      btnSeOpen.addEventListener("click", () => {
        runSeAction("Open account page", openSeAccountPageAction);
      });
    }

    if (btnSeSave) {
      btnSeSave.addEventListener("click", () => {
        const accountId = seAccountInput?.value?.trim() || "";
        const jwt = seJwtInput?.value?.trim() || "";
        if (!accountId || !jwt) {
          alert("Enter both Account ID and JWT from StreamElements Account → Channels.");
          return;
        }
        runSeAction("Save connection", () => {
          if (window.streamSyncConnections?.seSaveSession) {
            return window.streamSyncConnections.seSaveSession(accountId, jwt);
          }
          return window.streamSyncControlApi.privilegedFetch(
            "/api/streamelements/session",
            {
              method: "POST",
              body: JSON.stringify({ accountId, jwt }),
            }
          ).then((res) => {
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
          });
        });
      });
    }

    if (btnSeDisconnect) {
      btnSeDisconnect.addEventListener("click", () => {
        runSeAction("Disconnect", () => {
          if (seAccountInput) seAccountInput.value = "";
          if (seJwtInput) seJwtInput.value = "";
          if (window.streamSyncConnections?.seDisconnect) {
            return window.streamSyncConnections.seDisconnect();
          }
          return window.streamSyncControlApi
            .privilegedFetch("/api/streamelements/session", {
              method: "DELETE",
            })
            .then((res) => {
              if (!res.ok) throw new Error(`HTTP ${res.status}`);
            });
        });
      });
    }

    await fetchSeSessionOnce();
  }

  function initHelpView() {
    const viewEl = document.querySelector(".view-help");
    if (!viewEl) return;

    const btnDiscord = viewEl.querySelector("#btn-open-discord");
    const btnLogs = viewEl.querySelector("#btn-open-logs-folder");
    const btnPurge = viewEl.querySelector("#btn-purge-logs");

    const btnUpdates = viewEl.querySelector("#btn-check-updates");
    const btnExport = viewEl.querySelector("#btn-export-backup");

    if (btnDiscord) {
      btnDiscord.addEventListener("click", () => {
        if (window.electronAPI?.openDiscord) {
          window.electronAPI.openDiscord();
        } else {
          console.log("Open Discord clicked");
        }
      });
    }

    if (btnLogs) {
      btnLogs.addEventListener("click", () => {
        if (window.electronAPI?.openLogsFolder) {
          window.electronAPI.openLogsFolder();
        } else {
          console.log("Open logs folder clicked");
        }
      });
    }

    if (btnPurge) {
      btnPurge.addEventListener("click", async () => {
        const ok = confirm(
          "This will delete all log files except the current day's log.\n\nContinue?"
        );
        if (!ok) return;

        try {
          if (!window.electronAPI?.purgeLogs) {
            alert("Purge logs is not available (electronAPI.purgeLogs missing).");
            return;
          }

          const res = await window.electronAPI.purgeLogs();
          if (!res?.ok) {
            alert("Failed to purge logs:\n" + (res?.error || "Unknown error"));
            return;
          }

          alert(`Logs purged.\nDeleted: ${res.deleted}\nKept: ${res.kept}`);
        } catch (err) {
          alert("Failed to purge logs:\n" + (err?.message || String(err)));
        }
      });
    }

    if (btnExport) {
      btnExport.addEventListener("click", async () => {
        const prevLabel = btnExport.textContent;
        btnExport.disabled = true;
        btnExport.textContent = "Exporting…";
        try {
          if (!window.electronAPI?.exportBackup) {
            alert(
              "Export is only available in the Stream Sync desktop app (exportBackup API missing)."
            );
            return;
          }
          const res = await window.electronAPI.exportBackup();
          if (res?.cancelled) return;
          if (!res?.ok) {
            alert("Export failed:\n" + (res?.error || "Unknown error"));
            return;
          }
          const mb = res.bytes ? (res.bytes / (1024 * 1024)).toFixed(2) : "?";
          alert(`Backup saved.\n\n${res.path}\n\nSize: ${mb} MB`);
        } catch (err) {
          alert("Export failed:\n" + (err?.message || String(err)));
        } finally {
          btnExport.disabled = false;
          btnExport.textContent = prevLabel;
        }
      });
    }

    if (btnUpdates) {
      btnUpdates.addEventListener("click", async () => {
        try {
          if (!window.electronAPI?.checkForUpdates) {
            alert(
              "Update check is not available (electronAPI.checkForUpdates missing)."
            );
            return;
          }

          const res = await window.electronAPI.checkForUpdates();
          if (!res?.ok) {
            alert(
              "Failed to open update page:\n" + (res?.error || "Unknown error")
            );
          }
          // Success opens external browser; no UI needed here.
        } catch (err) {
          alert("Failed to check for updates:\n" + (err?.message || String(err)));
        }
      });
    }
  }

  // ---------- ABOUT VIEW INIT ----------
  function initAboutView() {
    const viewEl = document.querySelector(".view-about");
    if (!viewEl) return;

    const btnSyndicate = viewEl.querySelector("#btn-about-syndicate-site");
    const btnKaotic = viewEl.querySelector("#btn-about-kaotic-site");
    const btnDiscord = viewEl.querySelector("#btn-about-discord");
    const btnTwitch = viewEl.querySelector("#btn-about-twitch");

    const URLS = {
      syndicate: "https://syndicateai.net",
      kaotic: "https://kaoticgames.com",
      twitch: "https://twitch.tv/fukiplays",
      // Discord is handled via electronAPI.openDiscord() so it stays consistent with Help.
    };

    if (btnSyndicate) {
      btnSyndicate.addEventListener("click", () => {
        if (window.electronAPI?.openExternal) {
          window.electronAPI.openExternal(URLS.syndicate);
        } else {
          console.log("Open external:", URLS.syndicate);
        }
      });
    }

    if (btnKaotic) {
      btnKaotic.addEventListener("click", () => {
        if (window.electronAPI?.openExternal) {
          window.electronAPI.openExternal(URLS.kaotic);
        } else {
          console.log("Open external:", URLS.kaotic);
        }
      });
    }

    if (btnDiscord) {
      btnDiscord.addEventListener("click", () => {
        if (window.electronAPI?.openDiscord) {
          window.electronAPI.openDiscord();
        } else if (window.electronAPI?.openExternal) {
          // Fallback: if you ever remove openDiscord, you can hardcode an invite here.
          console.log(
            "openDiscord not available; add a Discord invite URL fallback if needed."
          );
        } else {
          console.log("Open Discord clicked");
        }
      });
    }

    if (btnTwitch) {
      btnTwitch.addEventListener("click", () => {
        if (window.electronAPI?.openExternal) {
          window.electronAPI.openExternal(URLS.twitch);
        } else {
          console.log("Open external:", URLS.twitch);
        }
      });
    }
  }

  // ---------- MAIN NAV WIRING ----------
  navButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const viewName = btn.getAttribute("data-view");
      if (viewName) loadView(viewName);
    });
  });

  // Initial view
  loadView("chat");

  // ---------- SET FOOTER YEAR ----------
  const yearSpan = document.getElementById("footer-year");
  if (yearSpan) {
    const year = new Date().getFullYear();
    yearSpan.textContent = String(year);
  } else {
    console.warn("footer-year element not found");
  }

  // ---------- POWERED BY PILL CLICK ----------
  const poweredPill = document.getElementById("powered-pill");
  if (poweredPill) {
    poweredPill.addEventListener("click", () => {
      const url = "https://syndicateai.net/";
      if (window.electronAPI?.openExternal) {
        window.electronAPI.openExternal(url);
      } else {
        window.open(url, "_blank");
      }
    });
  }
});
