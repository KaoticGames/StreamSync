// events-overlay-config.js
// Stream Sync – Events Overlay panel controller
// Wires up alert template config + Events Studio iframe
//
// Adds window.initEventsIntegrationsConfig() to power the Events → Integrations
// "Events Overlay" URL profile selector (ties to existing overlay profiles by
// scanning localStorage keys created by this editor).

(() => {
  const VIEW_ID = "events-overlay";

  // Profiles for Events Overlay are implied by keys like:
  // streamsync.events.overlay.profile.<profileId>
  const EVENTS_PROFILE_KEY_PREFIX = "streamsync.events.overlay.profile.";

  // Optional: remember which profile was last selected in Integrations tab
  const EVENTS_INTEGRATIONS_ACTIVE_PROFILE_KEY =
    "streamsync.events.integrations.overlayUrlProfile.v1";

  function overlayBaseUrl() {
    return (
      (window.STREAMSYNC_OVERLAY && window.STREAMSYNC_OVERLAY.baseUrl) ||
      "http://localhost:4040"
    );
  }

  function eventsOverlayPageBase() {
    return `${overlayBaseUrl()}/overlay/events`;
  }

  function updateEventsDockUrlField() {
    const input = document.getElementById("events-dock-url");
    if (!input) return;
    input.value = `${overlayBaseUrl()}/dock/events`;
  }

  // ───────────────────────────────────────────────
  // Integrations: profile dropdown + URL updater
  // ───────────────────────────────────────────────

  function listEventsOverlayProfilesFromStorage() {
    const ids = new Set(["default"]);

    try {
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (!key) continue;
        if (!key.startsWith(EVENTS_PROFILE_KEY_PREFIX)) continue;

        const id = key.slice(EVENTS_PROFILE_KEY_PREFIX.length).trim();
        if (id) ids.add(id);
      }
    } catch (_) {
      // ignore
    }

    const arr = Array.from(ids);
    arr.sort((a, b) => {
      if (a === "default") return -1;
      if (b === "default") return 1;
      return a.localeCompare(b);
    });

    // Events profiles currently display as ids (default, profile-2, etc)
    return arr.map((id) => ({ id, name: id }));
  }

  function updateEventsOverlayUrlField(profileId) {
    const input = document.getElementById("events-overlay-url");
    if (!input) return;
    const id = (profileId || "default").trim() || "default";
    input.value = `${eventsOverlayPageBase()}?profile=${encodeURIComponent(id)}`;
  }

  function loadSavedIntegrationsProfileId(profiles) {
    try {
      const saved =
        (localStorage.getItem(EVENTS_INTEGRATIONS_ACTIVE_PROFILE_KEY) || "").trim();
      if (saved && profiles.some((p) => p.id === saved)) return saved;
    } catch (_) {}
    return "default";
  }

  function saveIntegrationsProfileId(profileId) {
    try {
      localStorage.setItem(
        EVENTS_INTEGRATIONS_ACTIVE_PROFILE_KEY,
        String(profileId || "default")
      );
    } catch (_) {}
  }

  async function fetchServerEventsProfiles() {
    try {
      const res = await fetch(`${overlayBaseUrl()}/api/events/overlay-profiles`, {
        cache: "no-cache",
      });
      const data = res && res.ok ? await res.json() : null;
      if (data && data.ok && Array.isArray(data.profiles) && data.profiles.length) {
        return data.profiles;
      }
    } catch (_) {}
    return listEventsOverlayProfilesFromStorage();
  }

  window.streamSyncSelectEventsProfile = function streamSyncSelectEventsProfile(profileId) {
    const pid = (profileId || "default").trim() || "default";
    window.__STREAMSYNC_EVENTS_PROFILE__ = pid;
    saveIntegrationsProfileId(pid);

    const selectRoot = document.getElementById("events-overlay-url-profile");
    if (selectRoot) {
      selectRoot.dataset.selected = pid;
      if (selectRoot.__ssWidget) {
        selectRoot.__ssWidget.setSelected(pid);
      }
    }
    updateEventsOverlayUrlField(pid);

    const urlInput = document.getElementById("events-overlay-url");
    if (urlInput) urlInput.setAttribute("data-overlay-profile", pid);
  };

  const STUDIO_PROFILE_INDEX_KEY = "streamsync.events.overlay.profiles";

  window.streamSyncRegisterProfiles = function streamSyncRegisterProfiles(profileIds) {
    const ids = (Array.isArray(profileIds) ? profileIds : [profileIds])
      .map((x) => String(x || "").trim())
      .filter(Boolean);
    if (!ids.length) return;
    try {
      const raw = localStorage.getItem(STUDIO_PROFILE_INDEX_KEY);
      const base = raw ? JSON.parse(raw) : [];
      const list = Array.isArray(base) ? base.slice() : [];
      ids.forEach((id) => {
        if (!list.includes(id)) list.push(id);
      });
      localStorage.setItem(STUDIO_PROFILE_INDEX_KEY, JSON.stringify(list));
    } catch (_) {}

    const frame = document.getElementById("studioFrame");
    if (frame && frame.contentWindow) {
      try {
        frame.contentWindow.postMessage(
          { type: "streamsync-se-imported", profileIds: ids },
          "*"
        );
      } catch (_) {}
    }
  };

  window.streamSyncRefreshEventsProfiles = async function streamSyncRefreshEventsProfiles() {
    let serverProfiles = [];
    try {
      serverProfiles = await fetchServerEventsProfiles();
      if (serverProfiles.length) {
        window.streamSyncRegisterProfiles(serverProfiles.map((p) => p.id));
      }
    } catch (_) {}

    const selectRoot = document.getElementById("events-overlay-url-profile");
    if (selectRoot && selectRoot.__ssWidget && serverProfiles.length) {
      selectRoot.__ssWidget.setItems(serverProfiles);
      return;
    }
    if (typeof window.refreshEventsIntegrationsProfiles === "function") {
      await window.refreshEventsIntegrationsProfiles();
      return;
    }
    if (selectRoot && selectRoot.dataset.bound !== "1") {
      window.initEventsIntegrationsConfig?.();
    }
  };

  window.initEventsOverlayStudio = function initEventsOverlayStudio(profileId) {
    const profile =
      (profileId || "").trim() ||
      (window.__STREAMSYNC_EVENTS_PROFILE__ || "").trim() ||
      "default";
    window.__STREAMSYNC_EVENTS_PROFILE__ = profile;

    const root = document.getElementById("events-overlay");
    if (!root) return;

    const label = root.querySelector("#profileLabel");
    if (label) label.textContent = profile;

    const studioUrl =
      `${overlayBaseUrl()}/events-studio.html?profile=` + encodeURIComponent(profile);
    const frame = root.querySelector("#studioFrame");
    const btnReload = root.querySelector("#btnReloadStudio");

    if (frame) {
      frame.src = studioUrl;
      if (btnReload && btnReload.dataset.studioWired !== "1") {
        btnReload.dataset.studioWired = "1";
        btnReload.addEventListener("click", () => {
          if (frame.contentWindow) frame.contentWindow.location.reload();
          else frame.src = studioUrl;
        });
      }
    }

    window.streamSyncSelectEventsProfile(profile);
  };

  window.streamSyncOpenEventsProfile = function streamSyncOpenEventsProfile(profileId) {
    const pid = (profileId || "default").trim() || "default";
    window.__STREAMSYNC_EVENTS_PROFILE__ = pid;
    saveIntegrationsProfileId(pid);

    const eventsNav = document.querySelector('.nav-btn[data-view="events"]');
    if (eventsNav && !eventsNav.classList.contains("active")) {
      eventsNav.click();
    }

    const openOverlay = () => {
      window.streamSyncRefreshEventsProfiles?.().then(() => {
        window.streamSyncSelectEventsProfile(pid);
      });
      if (typeof window.__streamSyncLoadEventsSubview === "function") {
        window.__streamSyncLoadEventsSubview("events-overlay", { profileId: pid });
        return;
      }
      const overlayBtn = document.querySelector(
        '.subnav-events .subnav-btn[data-subview="events-overlay"]'
      );
      if (overlayBtn) overlayBtn.click();
      setTimeout(() => window.initEventsOverlayStudio?.(pid), 100);
    };

    setTimeout(openOverlay, 80);
  };

  // Called by renderer.js AFTER events-integrations.html is injected
  window.initEventsIntegrationsConfig = function initEventsIntegrationsConfig() {
    const selectRoot = document.getElementById("events-overlay-url-profile");
    const urlEl = document.getElementById("events-overlay-url");

    updateEventsDockUrlField();

    // Integrations partial might not be loaded; bail safely.
    if (!selectRoot || !urlEl) return;

    // Avoid double-binding if the partial is re-injected
    if (selectRoot.dataset.bound === "1") return;
    selectRoot.dataset.bound = "1";

    function mountSyndicateSelect(rootEl, items, selectedId, onChange) {
      const btn = rootEl.querySelector(".ss-select__btn");
      const menu = rootEl.querySelector(".ss-select__menu");
      if (!btn || !menu) return null;

      function render() {
        const selected = items.find((i) => i.id === selectedId) || items[0];
        btn.textContent = selected ? selected.name : "Select…";
        rootEl.dataset.selected = selectedId || "";

        menu.innerHTML = "";
        items.forEach((it) => {
          const div = document.createElement("div");
          div.className =
            "ss-select__item" + (it.id === selectedId ? " selected" : "");
          div.textContent = it.name;
          div.dataset.id = it.id;
          div.addEventListener("click", () => {
            selectedId = it.id;
            menu.classList.remove("open");
            render();
            onChange(it.id);
          });
          menu.appendChild(div);
        });
      }

      btn.addEventListener("click", (e) => {
        e.preventDefault();
        menu.classList.toggle("open");
      });

      document.addEventListener("click", (e) => {
        if (!rootEl.contains(e.target)) menu.classList.remove("open");
      });

      render();

      return {
        setItems(nextItems) {
          items = Array.isArray(nextItems) ? nextItems : [];
          render();
        },
        setSelected(id) {
          selectedId = id;
          render();
        },
        getSelected() {
          return selectedId;
        },
      };
    }

    async function renderProfiles() {
      let profiles = [];

      // Prefer overlay-server as source of truth (covers profiles that exist even if
      // the editor hasn't been opened this session)
      try {
        const res = await fetch(`${(window.STREAMSYNC_OVERLAY && window.STREAMSYNC_OVERLAY.baseUrl) || "http://localhost:4040"}/api/events/overlay-profiles`, {
          cache: "no-cache",
        });
        const data = res && res.ok ? await res.json() : null;
        if (data && data.ok && Array.isArray(data.profiles) && data.profiles.length) {
          profiles = data.profiles;
        }
      } catch (_) {}

      // Fallback: scan localStorage keys created by the editor
      if (!profiles.length) profiles = listEventsOverlayProfilesFromStorage();

      // keep current if valid, else fallback to last saved
      const current = (selectRoot.dataset.selected || "").trim();
      const desired =
        current && profiles.some((p) => p.id === current)
          ? current
          : loadSavedIntegrationsProfileId(profiles);

      if (!selectRoot.__ssWidget) {
        selectRoot.__ssWidget = mountSyndicateSelect(
          selectRoot,
          profiles,
          profiles.some((p) => p.id === desired) ? desired : "default",
          (id) => {
            const pid = (id || "default").trim() || "default";
            saveIntegrationsProfileId(pid);
            updateEventsOverlayUrlField(pid);
          }
        );
      } else {
        selectRoot.__ssWidget.setItems(profiles);
        selectRoot.__ssWidget.setSelected(
          profiles.some((p) => p.id === desired) ? desired : "default"
        );
      }

      if (selectRoot.__ssWidget) {
        updateEventsOverlayUrlField(selectRoot.__ssWidget.getSelected());
      }
    }

    window.refreshEventsIntegrationsProfiles = renderProfiles;
    renderProfiles();

    // Allow the embedded Events Studio iframe to notify us when profiles change.
    // (The Studio runs on http://localhost:4040 and cannot share localStorage with the Electron UI.)
    window.addEventListener("message", (ev) => {
      try {
        if (ev && ev.data && ev.data.type === "streamsync-events-profiles-changed") {
          renderProfiles();
        }
      } catch (_) {}
    });

    // Native <select> change handler removed (custom dropdown calls onChange)

    // Refresh when returning to this tab/window (helps after adding profiles elsewhere)
    window.addEventListener("focus", renderProfiles);
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) renderProfiles();
    });
  };

  // ───────────────────────────────────────────────
  // Existing Events Overlay editor logic
  // ───────────────────────────────────────────────

  function initOverlayConfig(root) {
    // Avoid double-binding if view is re-injected
    if (root.dataset.overlayBound === "1") return;
    root.dataset.overlayBound = "1";

    const qs = new URLSearchParams(location.search);
    const profile = (qs.get("profile") || "default").trim();

    const profileLabel = root.querySelector("#profileLabel");
    if (profileLabel) profileLabel.textContent = profile;

    const STORAGE_KEY = `streamsync.events.overlay.profile.${profile}`;

    function cryptoRandomId() {
      return (Math.random().toString(16).slice(2) + Date.now().toString(16)).slice(0, 18);
    }

    function defaultVariation(name) {
      return {
        id: cryptoRandomId(),
        name,
        image: { type: "url", value: "" },
        sound: { type: "url", value: "", volume: 100 },
        animation: {
          in: "fade",
          out: "fade",
          animSpeed: 50,
          slideInDir: "down",
          slideOutDir: "down",
        },
        layout: "textSide",
        message: "[name] triggered an alert!",
        durationSec: 6,
        text: {
          fontFamily:
            "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
          fontSize: 54,
          fontWeight: 800,
          color: "#ffffff",
          strokeColor: "rgba(0,0,0,0.75)",
          strokeWidth: 0,
        },
        placement: {
          image: { x: 120, y: 140, w: 320, h: 320 },
          text: { x: 500, y: 170, w: 520, h: 180 },
        },
      };
    }

    const DEFAULTS = () => ({
      version: 1,
      stage: { w: 1280, h: 720, grid: true, zoom: 1 },
      events: {
        follow: { variations: [defaultVariation("Base Alert")] },
        sub: { variations: [defaultVariation("Base Alert")] },
        resub: { variations: [defaultVariation("Base Alert")] },
        gift: { variations: [defaultVariation("Base Alert")] },
        cheer: { variations: [defaultVariation("Base Alert")] },
        raid: { variations: [defaultVariation("Base Alert")] },
      },
    });

    function migrate(cfg) {
      const base = DEFAULTS();
      if (!cfg || typeof cfg !== "object") return base;
      cfg.version = cfg.version || 1;
      cfg.stage = { ...base.stage, ...(cfg.stage || {}) };
      cfg.events = cfg.events || base.events;

      for (const k of Object.keys(base.events)) {
        if (!cfg.events[k]) cfg.events[k] = { variations: [defaultVariation("Base Alert")] };
        if (!Array.isArray(cfg.events[k].variations) || cfg.events[k].variations.length === 0) {
          cfg.events[k].variations = [defaultVariation("Base Alert")];
        }
        cfg.events[k].variations.forEach((v) => {
          if (!v.id) v.id = cryptoRandomId();
          v.animation = v.animation || {
            in: "fade",
            out: "fade",
            animSpeed: 50,
            slideInDir: "down",
            slideOutDir: "down",
          };
          let animIn = String(v.animation.in || "fade").trim().toLowerCase();
          let animOut = String(v.animation.out || "fade").trim().toLowerCase();
          if (animIn === "pop" || animIn === "flash") animIn = "none";
          if (animOut === "pop" || animOut === "flash") animOut = "none";
          const allowedAnim = ["none", "fade", "slide", "zoom"];
          if (!allowedAnim.includes(animIn)) animIn = "fade";
          if (!allowedAnim.includes(animOut)) animOut = "fade";
          v.animation.in = animIn;
          v.animation.out = animOut;
          const allowedDirs = ["up", "down", "left", "right"];
          if (!allowedDirs.includes(String(v.animation.slideInDir || "down").toLowerCase())) {
            v.animation.slideInDir = "down";
          }
          if (!allowedDirs.includes(String(v.animation.slideOutDir || "down").toLowerCase())) {
            v.animation.slideOutDir = "down";
          }
          if (Number.isFinite(v.animation.animSpeed)) {
            v.animation.animSpeed = Math.max(0, Math.min(100, Math.round(v.animation.animSpeed)));
          } else if (Number.isFinite(v.animation.speedPct)) {
            v.animation.animSpeed = Math.max(
              0,
              Math.min(100, 100 - Math.round((Number(v.animation.speedPct) - 25) / 2.75))
            );
          } else {
            v.animation.animSpeed = 50;
          }
          delete v.animation.speedPct;
        });
      }
      return cfg;
    }

    function loadConfig() {
      try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (!raw) return DEFAULTS();
        return migrate(JSON.parse(raw));
      } catch {
        return DEFAULTS();
      }
    }

    function saveConfig(cfg) {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(cfg));
    }

    // Studio iframe
    const studioUrl =
      `${(window.STREAMSYNC_OVERLAY && window.STREAMSYNC_OVERLAY.baseUrl) || "http://localhost:4040"}/events-studio.html?profile=` +
      encodeURIComponent(profile);
    const frame = root.querySelector("#studioFrame");
    const btnReloadStudio = root.querySelector("#btnReloadStudio");

    if (frame) {
      frame.src = studioUrl;
      if (btnReloadStudio) {
        btnReloadStudio.addEventListener("click", () => {
          frame.contentWindow ? frame.contentWindow.location.reload() : (frame.src = studioUrl);
        });
      }
    }

    // Form refs
    const eventTypeSel = root.querySelector("#eventType");

    const imageUrl = root.querySelector("#imageUrl");
    const soundUrl = root.querySelector("#soundUrl");

    const imgModeToggle = root.querySelector("#imgModeToggle");
    const sndModeToggle = root.querySelector("#sndModeToggle");

    const imgUrlRow = root.querySelector("#imgUrlRow");
    const imgLocalRow = root.querySelector("#imgLocalRow");
    const sndUrlRow = root.querySelector("#sndUrlRow");
    const sndLocalRow = root.querySelector("#sndLocalRow");

    const imageFile = root.querySelector("#imageFile");
    const soundFile = root.querySelector("#soundFile");

    const imgClearBtn = root.querySelector("#imgClearBtn");
    const sndClearBtn = root.querySelector("#sndClearBtn");
    const imgRemoveLocalBtn = root.querySelector("#imgRemoveLocalBtn");
    const sndRemoveLocalBtn = root.querySelector("#sndRemoveLocalBtn");

    const layout = root.querySelector("#layout");
    const duration = root.querySelector("#duration");
    const message = root.querySelector("#message");
    const fontFamily = root.querySelector("#fontFamily");
    const fontWeight = root.querySelector("#fontWeight");
    const fontSize = root.querySelector("#fontSize");
    const textColor = root.querySelector("#textColor");
    const strokeColor = root.querySelector("#strokeColor");
    const strokeWidth = root.querySelector("#strokeWidth");

    if (!eventTypeSel || !frame) {
      // View isn't fully rendered yet; bail.
      return;
    }

    function currentVariation(cfg, eventKey) {
      const ev = cfg.events[eventKey];
      if (!ev.variations || !ev.variations.length) {
        ev.variations = [defaultVariation("Base Alert")];
      }
      return ev.variations[0]; // Base Alert only for now
    }

    /* -------- toggle helpers ---------- */

    function setToggle(btn, on) {
      if (!btn) return;
      btn.classList.toggle("on", !!on);
      btn.setAttribute("aria-pressed", on ? "true" : "false");
    }

    function getModeLocal(btn) {
      return !!btn && btn.getAttribute("aria-pressed") === "true";
    }

    function setImgModeLocal(isLocal) {
      setToggle(imgModeToggle, isLocal);
      if (imgUrlRow) imgUrlRow.style.display = isLocal ? "none" : "flex";
      if (imgLocalRow) imgLocalRow.style.display = isLocal ? "flex" : "none";
    }

    function setSndModeLocal(isLocal) {
      setToggle(sndModeToggle, isLocal);
      if (sndUrlRow) sndUrlRow.style.display = isLocal ? "none" : "flex";
      if (sndLocalRow) sndLocalRow.style.display = isLocal ? "flex" : "none";
    }

    function fileToDataUrl(file) {
      return new Promise((resolve, reject) => {
        const r = new FileReader();
        r.onload = () => resolve(r.result);
        r.onerror = () => reject(new Error("Failed to read file"));
        r.readAsDataURL(file);
      });
    }

    /* -------- sync <-> config ---------- */

    function syncFieldsFromVariation(v) {
      const imgIsLocal = v.image?.type === "data";
      const sndIsLocal = v.sound?.type === "data";

      setImgModeLocal(imgIsLocal);
      setSndModeLocal(sndIsLocal);

      if (imageUrl) {
        imageUrl.value = !imgIsLocal && v.image?.type === "url" ? v.image.value || "" : "";
      }
      if (soundUrl) {
        soundUrl.value = !sndIsLocal && v.sound?.type === "url" ? v.sound.value || "" : "";
      }

      if (layout) layout.value = v.layout || "textSide";
      if (duration) duration.value = v.durationSec ?? 6;
      if (message) message.value = v.message ?? "";

      if (fontFamily) fontFamily.value = v.text?.fontFamily || fontFamily.value;
      if (fontWeight) fontWeight.value = String(v.text?.fontWeight || "800");
      if (fontSize) fontSize.value = v.text?.fontSize ?? 54;
      if (textColor) textColor.value = v.text?.color ?? "#ffffff";
      if (strokeColor) strokeColor.value = v.text?.strokeColor ?? "rgba(0,0,0,0.75)";
      if (strokeWidth) strokeWidth.value = v.text?.strokeWidth ?? 0;
    }

    function applyFieldsToVariation(v) {
      const imgLocal = getModeLocal(imgModeToggle);
      const sndLocal = getModeLocal(sndModeToggle);

      if (!imgLocal) {
        v.image = { type: "url", value: (imageUrl?.value || "").trim() };
      } else {
        v.image = v.image && v.image.type === "data" ? v.image : { type: "data", value: "" };
      }

      if (!sndLocal) {
        v.sound = { type: "url", value: (soundUrl?.value || "").trim() };
      } else {
        v.sound = v.sound && v.sound.type === "data" ? v.sound : { type: "data", value: "" };
      }

      if (layout) v.layout = layout.value;
      if (duration) v.durationSec = Math.max(1, Math.min(60, Number(duration.value || 6)));
      if (message) v.message = message.value || "";

      v.text = v.text || {};
      if (fontFamily) v.text.fontFamily = fontFamily.value;
      if (fontWeight) v.text.fontWeight = Number(fontWeight.value || 800);
      if (fontSize) v.text.fontSize = Math.max(10, Math.min(160, Number(fontSize.value || 54)));
      if (textColor) v.text.color = textColor.value || "#ffffff";
      if (strokeColor) v.text.strokeColor = strokeColor.value || "rgba(0,0,0,0.75)";
      if (strokeWidth) v.text.strokeWidth = Math.max(0, Math.min(24, Number(strokeWidth.value || 0)));
    }

    function writeFromForm() {
      const cfg = loadConfig();
      const key = eventTypeSel.value;
      const v = currentVariation(cfg, key);
      applyFieldsToVariation(v);
      saveConfig(cfg);
      // Studio iframe will auto-refresh via its storage listener
    }

    function loadIntoForm() {
      const cfg = loadConfig();
      const key = eventTypeSel.value;
      const v = currentVariation(cfg, key);
      syncFieldsFromVariation(v);
    }

    function notifyStudioEventType() {
      frame.contentWindow?.postMessage(
        { type: "streamsync-events-studio-select-event", eventType: eventTypeSel.value },
        "*"
      );
    }

    // Init
    loadIntoForm();
    notifyStudioEventType();
    frame.addEventListener("load", notifyStudioEventType);

    // Event type change
    eventTypeSel.addEventListener("change", () => {
      loadIntoForm();
      notifyStudioEventType();
    });

    // Generic bindings
    [imageUrl, soundUrl, layout, duration, message, fontFamily, fontWeight, fontSize, textColor, strokeColor, strokeWidth]
      .forEach((el) => {
        if (!el) return;
        el.addEventListener("input", writeFromForm);
        el.addEventListener("change", writeFromForm);
      });

    // Toggle handlers
    if (imgModeToggle) {
      imgModeToggle.addEventListener("click", () => {
        const next = !getModeLocal(imgModeToggle);
        setImgModeLocal(next);
        writeFromForm();
      });
    }

    if (sndModeToggle) {
      sndModeToggle.addEventListener("click", () => {
        const next = !getModeLocal(sndModeToggle);
        setSndModeLocal(next);
        writeFromForm();
      });
    }

    // Clear URL buttons
    if (imgClearBtn && imageUrl) {
      imgClearBtn.addEventListener("click", () => {
        imageUrl.value = "";
        writeFromForm();
      });
    }

    if (sndClearBtn && soundUrl) {
      sndClearBtn.addEventListener("click", () => {
        soundUrl.value = "";
        writeFromForm();
      });
    }

    // Local uploads -> Data URLs
    if (imageFile) {
      imageFile.addEventListener("change", async () => {
        const f = imageFile.files?.[0];
        if (!f) return;

        const cfg = loadConfig();
        const key = eventTypeSel.value;
        const v = currentVariation(cfg, key);

        const data = await fileToDataUrl(f);
        v.image = { type: "data", value: data };

        saveConfig(cfg);

        setImgModeLocal(true);
        if (imageUrl) imageUrl.value = "";
        imageFile.value = "";

        loadIntoForm();
      });
    }

    if (soundFile) {
      soundFile.addEventListener("change", async () => {
        const f = soundFile.files?.[0];
        if (!f) return;

        const cfg = loadConfig();
        const key = eventTypeSel.value;
        const v = currentVariation(cfg, key);

        const data = await fileToDataUrl(f);
        v.sound = { type: "data", value: data };

        saveConfig(cfg);

        setSndModeLocal(true);
        if (soundUrl) soundUrl.value = "";
        soundFile.value = "";

        loadIntoForm();
      });
    }

    // Remove local assets
    if (imgRemoveLocalBtn) {
      imgRemoveLocalBtn.addEventListener("click", () => {
        const cfg = loadConfig();
        const key = eventTypeSel.value;
        const v = currentVariation(cfg, key);

        v.image = { type: "data", value: "" };
        saveConfig(cfg);
        loadIntoForm();
      });
    }

    if (sndRemoveLocalBtn) {
      sndRemoveLocalBtn.addEventListener("click", () => {
        const cfg = loadConfig();
        const key = eventTypeSel.value;
        const v = currentVariation(cfg, key);

        v.sound = { type: "data", value: "" };
        saveConfig(cfg);
        loadIntoForm();
      });
    }
  }

  function tryInit() {
    const root = document.getElementById(VIEW_ID);
    if (root) initOverlayConfig(root);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", tryInit);
  } else {
    tryInit();
  }

  // In case views are swapped dynamically after initial load
  const container = document.querySelector(".events-subview-container") || document.body;
  if (container && window.MutationObserver) {
    const mo = new MutationObserver(() => tryInit());
    mo.observe(container, { childList: true, subtree: true });
  }


})();
