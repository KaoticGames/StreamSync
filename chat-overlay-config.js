// chat-overlay-config.js
// Stream Sync – Chat Overlay config controller
// - Multi-profile system with branded dropdown in Overlay tab
// - Integrations tab: profile dropdown that updates Browser Source URL
// - No window.prompt() (Electron blocks it) — uses a small in-app modal instead

function overlayBaseUrl() {
  return (window.STREAMSYNC_OVERLAY && window.STREAMSYNC_OVERLAY.baseUrl) || "http://localhost:4040";
}

// Profile index + active profile keys
const CHAT_OVERLAY_PROFILES_KEY = "streamsync.chatOverlay.profiles.v1";
const CHAT_OVERLAY_ACTIVE_PROFILE_KEY = "streamsync.chatOverlay.activeProfile.v1";

// Per-profile settings key prefix
const CHAT_OVERLAY_SETTINGS_PREFIX = "streamsync.chatOverlay.settings.v1.";

(function () {
  // ───────────────────────────────────────────────
  // Defaults
  // ───────────────────────────────────────────────

  const defaultSettings = {
    showTimestamps: true,
    showBadges: true,
    fontSource: "google", // "google" | "local"
    googleFont: "Montserrat",
    localFontName: "system-ui",
    localFontFamily: "system-ui",
    localFontUrl: null,
    fontSize: 18,
    textRotate: 0,
    textSkew: 0,
    feedDirection: "up-down", // up-down | down-up | left-right | right-left
    messageStyle: "bubble", // bubble | plain
    bubbleRadius: 18,
    bubbleColorMode: "fixed", // fixed | user
    bubbleColor: "#1f2933",
    bubbleAlpha: 1, // 0–1 internal
    strokeEnabled: false,
    strokeColor: "#000000",
    strokeWidth: 0, // supports decimals
    bgMode: "transparent", // transparent | solid
    bgColor: "#000000",
    displayMode: "solid", // solid | popup
    popupDuration: 8,
    popupExitStyle: "fade",
  };

  function cryptoRandomId() {
    return (Math.random().toString(16).slice(2) + Date.now().toString(16))
      .slice(0, 18);
  }

  // ───────────────────────────────────────────────
  // Modal prompt (no window.prompt)
  // ───────────────────────────────────────────────

  function modalPrompt({ title, label, placeholder, initialValue }) {
    return new Promise((resolve) => {
      const overlay = document.createElement("div");
      overlay.style.position = "fixed";
      overlay.style.inset = "0";
      overlay.style.background = "rgba(0,0,0,0.65)";
      overlay.style.display = "flex";
      overlay.style.alignItems = "center";
      overlay.style.justifyContent = "center";
      overlay.style.zIndex = "9999";
      overlay.style.padding = "16px";

      const card = document.createElement("div");
      card.className = "card";
      card.style.width = "min(520px, 96vw)";

      const h = document.createElement("div");
      h.className = "card-title";
      h.textContent = title || "Enter value";

      const p = document.createElement("div");
      p.className = "card-subtitle";
      p.style.marginTop = "4px";
      p.textContent = label || "";

      const fieldWrap = document.createElement("div");
      fieldWrap.className = "config-section";
      fieldWrap.style.marginTop = "12px";

      const input = document.createElement("input");
      input.type = "text";
      input.className = "input";
      input.placeholder = placeholder || "";
      input.value = initialValue || "";

      const actions = document.createElement("div");
      actions.style.display = "flex";
      actions.style.justifyContent = "flex-end";
      actions.style.gap = "8px";
      actions.style.marginTop = "12px";

      const btnCancel = document.createElement("button");
      btnCancel.type = "button";
      btnCancel.className = "btn btn-secondary";
      btnCancel.textContent = "Cancel";

      const btnOk = document.createElement("button");
      btnOk.type = "button";
      btnOk.className = "btn btn-primary";
      btnOk.textContent = "Create";

      function cleanup(val) {
        try {
          document.body.removeChild(overlay);
        } catch (_) {}
        resolve(val);
      }

      btnCancel.addEventListener("click", () => cleanup(null));
      btnOk.addEventListener("click", () => cleanup(input.value.trim() || null));

      overlay.addEventListener("click", (e) => {
        if (e.target === overlay) cleanup(null);
      });

      document.addEventListener(
        "keydown",
        function onKey(e) {
          if (e.key === "Escape") {
            document.removeEventListener("keydown", onKey);
            cleanup(null);
          }
          if (e.key === "Enter") {
            document.removeEventListener("keydown", onKey);
            cleanup(input.value.trim() || null);
          }
        },
        { once: false }
      );

      actions.appendChild(btnCancel);
      actions.appendChild(btnOk);

      fieldWrap.appendChild(input);
      card.appendChild(h);
      if (label) card.appendChild(p);
      card.appendChild(fieldWrap);
      card.appendChild(actions);

      overlay.appendChild(card);
      document.body.appendChild(overlay);

      setTimeout(() => {
        input.focus();
        input.select();
      }, 0);
    });
  }

  // ───────────────────────────────────────────────
  // Profiles storage
  // ───────────────────────────────────────────────

  function loadProfiles() {
    try {
      const raw = window.localStorage.getItem(CHAT_OVERLAY_PROFILES_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return null;
      return parsed.filter(
        (p) => p && typeof p.id === "string" && typeof p.name === "string"
      );
    } catch {
      return null;
    }
  }

  function saveProfiles(profiles) {
    try {
      window.localStorage.setItem(
        CHAT_OVERLAY_PROFILES_KEY,
        JSON.stringify(profiles)
      );
    } catch (err) {
      console.warn("[ChatOverlayProfiles] Failed to save profiles:", err);
    }
  }

  function normalizeOverlayProfileId(id) {
    const v = (id || "").trim();
    return !v || v === "default" ? "chat-default" : v;
  }

  function ensureProfiles() {
    let profiles = loadProfiles();
    const DEFAULT_ID = "chat-default";

    // Fresh install / missing index
    if (!profiles || profiles.length === 0) {
      profiles = [{ id: DEFAULT_ID, name: "Default" }];
      saveProfiles(profiles);
      return profiles;
    }

    // Migration: older builds used "default" for the built-in profile id
    const hasOldDefault = profiles.some((p) => p.id === "default");
    const hasNewDefault = profiles.some((p) => p.id === DEFAULT_ID);
    if (hasOldDefault && !hasNewDefault) {
      profiles = profiles.map((p) =>
        p.id === "default" ? { ...p, id: DEFAULT_ID, name: p.name || "Default" } : p
      );
      saveProfiles(profiles);
    }

    if (!profiles.some((p) => p.id === DEFAULT_ID)) {
      profiles.unshift({ id: DEFAULT_ID, name: "Default" });
      saveProfiles(profiles);
    }

    return profiles;
  }

  function loadActiveProfileId(profiles) {
    try {
      const raw = window.localStorage.getItem(CHAT_OVERLAY_ACTIVE_PROFILE_KEY);
      const id = (raw || "").trim() || "chat-default";
      if (profiles.some((p) => p.id === id)) return id;
      return "chat-default";
    } catch {
      return "chat-default";
    }
  }

  function saveActiveProfileId(profileId) {
    try {
      window.localStorage.setItem(
        CHAT_OVERLAY_ACTIVE_PROFILE_KEY,
        String(profileId || "chat-default")
      );
    } catch (err) {
      console.warn("[ChatOverlayProfiles] Failed to save active profile:", err);
    }
  }

  // ───────────────────────────────────────────────
  // Integrations URL helpers
  // ───────────────────────────────────────────────

  function chatOverlayUrlFor(profileId) {
    const base = `${overlayBaseUrl()}/overlay/chat`;
    return `${base}?profile=${encodeURIComponent(profileId || "chat-default")}`;
  }

  function updateOverlayUrlField(profileId) {
    // Integrations input (chat)
    const urlEl = document.getElementById("chat-overlay-url");
    if (urlEl) urlEl.value = chatOverlayUrlFor(profileId);

    // Optional: also keep overlay tab’s own URL field if you ever add one
    const maybe = document.getElementById("chat-overlay-url-overlaytab");
    if (maybe) maybe.value = chatOverlayUrlFor(profileId);
  }

  function initChatIntegrationsDropdown(state) {
    const selectRoot = document.getElementById("chat-overlay-url-profile");
    const urlEl = document.getElementById("chat-overlay-url");

    // If user hasn't added it yet, just keep URL updated via updateOverlayUrlField()
    if (!selectRoot || !urlEl) return;

    // If already bound, just ask the existing renderer (if any) to refresh.
    if (selectRoot.dataset.bound === "1") {
      try {
        if (typeof selectRoot.__renderProfiles === "function") selectRoot.__renderProfiles();
      } catch (_) {}
      return;
    }
    selectRoot.dataset.bound = "1";

    function mountSyndicateSelect(rootEl, items, selectedId, onChange) {
      const btn = rootEl.querySelector(".ss-select__btn");
      const menu = rootEl.querySelector(".ss-select__menu");

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

    async function render() {
      // We keep TWO sources of truth:
      // 1) Local profile index (contains the *display names* the user typed)
      // 2) Server profile list (contains what the overlay endpoint can actually load)
      // The integrations dropdown should reflect profiles the user created in the editor,
      // even if the server hasn't persisted a config for them yet.

      const localIndex = ensureProfiles();

      let serverProfiles = [];
      try {
        const res = await fetch(`${overlayBaseUrl()}/api/chat/overlay-profiles`, {
          cache: "no-cache",
        });
        const data = res && res.ok ? await res.json() : null;
        if (data && data.ok && Array.isArray(data.profiles)) {
          serverProfiles = data.profiles;
        }
      } catch (_) {}

      // Merge: start with local (so names are always correct), then overlay server-only ids
      // that also exist in local index (prevents stale random ids from leaking into Integrations).
      const nameMap = new Map(localIndex.map((p) => [p.id, p.name]));
      const allowed = new Set(localIndex.map((p) => p.id));

      const merged = [];
      const pushUniq = (p) => {
        const id = String(p?.id || "").trim();
        if (!id) return;
        if (merged.some((x) => x.id === id)) return;
        merged.push({ id, name: String(p?.name || nameMap.get(id) || id) });
      };

      // Always include local profiles first
      localIndex.forEach((p) => pushUniq({ id: p.id, name: p.name }));

      // Include server profiles only if they're in the local index (prevents stale ids)
      serverProfiles.forEach((p) => {
        const id = String(p?.id || "").trim();
        if (!id) return;
        if (id === "chat-default" || allowed.has(id)) {
          pushUniq({ id, name: nameMap.get(id) || p.name || id });
        }
      });

      // Final list
      let profiles = merged;


      const active = state?.activeProfileId || "chat-default";

      // Mount or refresh the custom dropdown
      if (!selectRoot.__ssWidget) {
        selectRoot.__ssWidget = mountSyndicateSelect(
          selectRoot,
          profiles,
          profiles.some((p) => p.id === active) ? active : "chat-default",
          (id) => {
            const nextId = (id || "chat-default").trim() || "chat-default";
            state.activeProfileId = nextId;
            saveActiveProfileId(nextId);

            // also drive overlay tab select change path if present
            const overlaySelect = document
              .getElementById("chat-overlay")
              ?.querySelector("#chat-overlay-profile-select");
            if (overlaySelect) {
              overlaySelect.value = nextId;
              overlaySelect.dispatchEvent(new Event("change", { bubbles: true }));
            }

            updateOverlayUrlField(nextId);
          }
        );
      } else {
        selectRoot.__ssWidget.setItems(profiles);
        selectRoot.__ssWidget.setSelected(
          profiles.some((p) => p.id === active) ? active : "chat-default"
        );
      }

      updateOverlayUrlField(selectRoot.__ssWidget.getSelected());
    }

    // Allow other parts of the UI to force-refresh this dropdown when profiles change.
    selectRoot.__renderProfiles = render;
    window.addEventListener("streamsync-chat-profiles-changed", render);

    render();

    // Native <select> change handler removed (custom dropdown calls onChange)

    window.addEventListener("focus", render);
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) render();
    });
  }

  // ───────────────────────────────────────────────
  // Storage helpers (per profile)
  // ───────────────────────────────────────────────

  function getSettingsKey(profileId) {
    return CHAT_OVERLAY_SETTINGS_PREFIX + (profileId || "chat-default");
  }

  function loadSettings(profileId) {
    const key = getSettingsKey(profileId);
    try {
      const raw = window.localStorage.getItem(key);
      if (!raw) return { ...defaultSettings };
      const parsed = JSON.parse(raw);
      return {
        ...defaultSettings,
        ...parsed,
        localFontFamily:
          parsed.localFontFamily || parsed.localFontName || "system-ui",
      };
    } catch (err) {
      console.warn("[ChatOverlayConfig] Failed to load settings:", err);
      return { ...defaultSettings };
    }
  }

  function saveSettingsLocal(profileId, settings) {
    const key = getSettingsKey(profileId);
    try {
      window.localStorage.setItem(key, JSON.stringify(settings));
    } catch (err) {
      console.warn("[ChatOverlayConfig] Failed to save settings:", err);
    }
  }

  // ───────────────────────────────────────────────
  // Server sync + mapping
  // ───────────────────────────────────────────────

  function applyServerConfigToUiSettings(uiSettings, serverCfg) {
    if (!serverCfg || typeof serverCfg !== "object") return uiSettings;

    const s = { ...uiSettings };

    if (typeof serverCfg.showTimestamps === "boolean")
      s.showTimestamps = serverCfg.showTimestamps;
    if (typeof serverCfg.showBadges === "boolean")
      s.showBadges = serverCfg.showBadges;

    if (Number.isFinite(Number(serverCfg.fontSize)))
      s.fontSize = Number(serverCfg.fontSize) || s.fontSize;

    if (typeof serverCfg.fontFamily === "string" && serverCfg.fontFamily.trim()) {
      let fam = serverCfg.fontFamily.trim();
      // Strip CSS stacks / quotes accidentally persisted as fontFamily
      if (fam.includes(",")) fam = fam.split(",")[0].trim();
      fam = fam.replace(/^["']|["']$/g, "");
      if (fam === "Source Sans Pro") fam = "Source Sans 3";
      if (fam === "system-ui" || /^OverlayLocal_/i.test(fam)) {
        // keep local name separately; google default stays Montserrat unless local
        if (/^OverlayLocal_/i.test(fam)) {
          s.localFontName = fam;
        } else {
          fam = "Montserrat";
        }
      }
      if (!/^OverlayLocal_/i.test(fam)) {
        s.googleFont = normalizeGoogleFontName(fam);
      } else {
        s.googleFont = s.googleFont || "Montserrat";
      }
    }

    const url = serverCfg.localFontUrl || serverCfg.fontUrl || null;
    if (url) {
      s.fontSource = "local";
      s.localFontUrl = url;
    } else {
      s.fontSource = "google";
      s.localFontUrl = null;
    }

    if (typeof serverCfg.textRotate !== "undefined")
      s.textRotate = Number(serverCfg.textRotate) || 0;
    if (typeof serverCfg.textSkew !== "undefined")
      s.textSkew = Number(serverCfg.textSkew) || 0;

    if (typeof serverCfg.feedDirection === "string")
      s.feedDirection = serverCfg.feedDirection;
    if (typeof serverCfg.messageStyle === "string")
      s.messageStyle = serverCfg.messageStyle;

    if (Number.isFinite(Number(serverCfg.bubbleRadius)))
      s.bubbleRadius = Number(serverCfg.bubbleRadius) || 0;
    if (typeof serverCfg.bubbleColorMode === "string")
      s.bubbleColorMode = serverCfg.bubbleColorMode;
    if (typeof serverCfg.bubbleColor === "string")
      s.bubbleColor = serverCfg.bubbleColor;

    if (typeof serverCfg.bubbleAlpha === "number")
      s.bubbleAlpha = Math.min(Math.max(serverCfg.bubbleAlpha, 0), 1);

    if (typeof serverCfg.strokeEnabled === "boolean")
      s.strokeEnabled = serverCfg.strokeEnabled;
    if (typeof serverCfg.strokeColor === "string")
      s.strokeColor = serverCfg.strokeColor;
    if (Number.isFinite(Number(serverCfg.strokeWidth)))
      s.strokeWidth = Number(serverCfg.strokeWidth) || 0;

    if (typeof serverCfg.bgMode === "string") s.bgMode = serverCfg.bgMode;
    if (typeof serverCfg.bgColor === "string") s.bgColor = serverCfg.bgColor;

    if (typeof serverCfg.displayMode === "string")
      s.displayMode = serverCfg.displayMode;
    if (Number.isFinite(Number(serverCfg.popupDuration)))
      s.popupDuration = Number(serverCfg.popupDuration) || 8;

    return s;
  }

  async function fetchServerConfig(profileId) {
    try {
      const res = await fetch(
        `${overlayBaseUrl()}/api/chat/overlay-config?profile=${encodeURIComponent(
          normalizeOverlayProfileId(profileId)
        )}`,
        { cache: "no-cache" }
      );
      if (!res.ok) return null;
      return await res.json();
    } catch (err) {
      console.warn("[ChatOverlayConfig] Failed to fetch server config:", err);
      return null;
    }
  }

  function syncSettingsToServer(profileId, settings) {
    try {
      const payload = {
        profileId: normalizeOverlayProfileId(profileId),

        showTimestamps: !!settings.showTimestamps,
        showBadges: !!settings.showBadges,
        fontSize: Number(settings.fontSize) || 18,

        fontFamily:
          settings.fontSource === "google"
            ? settings.googleFont || "system-ui"
            : settings.localFontName || "system-ui",

        // Clear local font URL when using Google so the overlay stops applying @font-face
        localFontUrl:
          settings.fontSource === "local" ? settings.localFontUrl || null : null,
        fontUrl:
          settings.fontSource === "local" ? settings.localFontUrl || null : null,

        textRotate: Number(settings.textRotate) || 0,
        textSkew: Number(settings.textSkew) || 0,
        feedDirection: settings.feedDirection || "up-down",
        messageStyle: settings.messageStyle || "plain",
        bubbleRadius: Number(settings.bubbleRadius) || 0,
        bubbleColorMode: settings.bubbleColorMode || "fixed",
        bubbleColor: settings.bubbleColor || "#000000",
        bubbleAlpha:
          typeof settings.bubbleAlpha === "number" ? settings.bubbleAlpha : 1,

        strokeEnabled: !!settings.strokeEnabled,
        strokeColor: settings.strokeColor || "#000000",
        strokeWidth: Number(settings.strokeWidth) || 0,

        bgMode: settings.bgMode || "transparent",
        bgColor: settings.bgColor || "#000000",

        displayMode: settings.displayMode || "solid",
        popupDuration: Number(settings.popupDuration) || 8,
        popupExitStyle: settings.popupExitStyle || "fade",
      };

      fetch(`${overlayBaseUrl()}/api/chat/overlay-config`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }).catch((err) => {
        console.warn("[ChatOverlayConfig] Failed to sync overlay settings:", err);
      });
    } catch (err) {
      console.warn("[ChatOverlayConfig] Unexpected error syncing:", err);
    }
  }

  // ───────────────────────────────────────────────
  // UI helpers
  // ───────────────────────────────────────────────

  function toRgba(color, alpha) {
    const a = typeof alpha === "number" ? Math.min(Math.max(alpha, 0), 1) : 1;

    if (/^rgba\(/i.test(color)) {
      return color.replace(/rgba\(([^)]+)\)/i, (_, inner) => {
        const parts = inner.split(",").map((p) => p.trim());
        const [r, g, b] = parts;
        return `rgba(${r}, ${g}, ${b}, ${a})`;
      });
    }

    if (/^rgb\(/i.test(color)) {
      return color.replace(/rgb\(([^)]+)\)/i, (_, inner) => `rgba(${inner.trim()}, ${a})`);
    }

    if (/^#([0-9a-f]{3,8})$/i.test(color)) {
      let hex = color.slice(1);
      if (hex.length === 3 || hex.length === 4) {
        hex = hex.split("").map((ch) => ch + ch).join("");
      }
      if (hex.length === 6 || hex.length === 8) {
        const r = parseInt(hex.slice(0, 2), 16);
        const g = parseInt(hex.slice(2, 4), 16);
        const b = parseInt(hex.slice(4, 6), 16);
        return `rgba(${r}, ${g}, ${b}, ${a})`;
      }
    }

    return color;
  }

  function parseColorRgb(color) {
    if (!color) return null;

    if (/^rgba?\(/i.test(color)) {
      const m = color.match(/rgba?\(\s*([^)]+)\s*\)/i);
      if (!m) return null;
      const parts = m[1].split(",").map((p) => p.trim());
      const r = Number(parts[0]);
      const g = Number(parts[1]);
      const b = Number(parts[2]);
      if ([r, g, b].every((n) => Number.isFinite(n))) return { r, g, b };
      return null;
    }

    if (/^#([0-9a-f]{3,8})$/i.test(color)) {
      let hex = color.slice(1);
      if (hex.length === 3 || hex.length === 4) {
        hex = hex.split("").map((ch) => ch + ch).join("");
      }
      if (hex.length >= 6) {
        return {
          r: parseInt(hex.slice(0, 2), 16),
          g: parseInt(hex.slice(2, 4), 16),
          b: parseInt(hex.slice(4, 6), 16),
        };
      }
    }

    return null;
  }

  function userColorToBubbleBackground(userColor, alpha) {
    const rgb = parseColorRgb(userColor);
    if (!rgb) return toRgba(userColor, alpha);
    const shade = 0.42;
    const r = Math.round(rgb.r * shade);
    const g = Math.round(rgb.g * shade);
    const b = Math.round(rgb.b * shade);
    return toRgba(`rgb(${r}, ${g}, ${b})`, alpha);
  }

  function setToggleState(button, isOn) {
    if (!button) return;
    const on = !!isOn;
    button.classList.toggle("on", on);
    button.setAttribute("aria-pressed", on ? "true" : "false");
    const labelEl = button.querySelector(".toggle-label");
    if (labelEl) labelEl.textContent = on ? "On" : "Off";
  }

  function getOrientationFromFeedDirection(fd) {
    if (fd === "left-right" || fd === "right-left") return "horizontal";
    return "vertical";
  }

  function applyBubbleVisibility(overlaySection, settings) {
    if (!overlaySection) return;
    const isBubble = (settings.messageStyle || "plain") === "bubble";

    ["#overlay-bubble-shape-row", "#overlay-bubble-color-row", "#overlay-bubble-radius-row"].forEach(
      (sel) => {
        const el = overlaySection.querySelector(sel);
        if (el) el.classList.toggle("hidden", !isBubble);
      }
    );
  }

  const GOOGLE_FONTS = [
    "Montserrat",
    "Poppins",
    "Inter",
    "Roboto",
    "Roboto Condensed",
    "Open Sans",
    "Lato",
    "Nunito",
    "Raleway",
    "Oswald",
    "Bebas Neue",
    "Playfair Display",
    "Cinzel",
    "Merriweather",
    "Source Sans 3",
    "Noto Sans",
    "Noto Serif",
    "Ubuntu",
    "Rubik",
    "Work Sans",
    "PT Sans",
    "Fira Sans",
    "Josefin Sans",
    "Comfortaa",
  ];

  function normalizeGoogleFontName(name) {
    let fam = String(name || "").trim();
    if (!fam || fam === "system-ui") return "Montserrat";
    if (fam === "Source Sans Pro") return "Source Sans 3";
    if (!GOOGLE_FONTS.includes(fam)) return "Montserrat";
    return fam;
  }

  function ensureGoogleFontLoaded(family) {
    const fam = String(family || "").trim();
    if (!fam || fam === "system-ui" || /^OverlayLocal_/i.test(fam)) return;

    const id = "gf-chat-control-" + fam.toLowerCase().replace(/[^a-z0-9]+/g, "-");
    if (document.getElementById(id)) return;

    const link = document.createElement("link");
    link.id = id;
    link.rel = "stylesheet";
    link.href = "/google-fonts.css?family=" + encodeURIComponent(fam);
    document.head.appendChild(link);
  }

  function fillOverlayGoogleFontOptions(select, selectedName) {
    if (!select) return "Montserrat";
    const selected = normalizeGoogleFontName(
      selectedName || select.value || "Montserrat"
    );

    // Prefer keeping a full static <option> list from chat.html. Only append
    // any missing names — never wipe down to a single option.
    const existing = new Set(
      Array.prototype.map.call(select.options, (o) => o.value)
    );
    for (let i = 0; i < GOOGLE_FONTS.length; i++) {
      const name = GOOGLE_FONTS[i];
      if (existing.has(name)) continue;
      const opt = document.createElement("option");
      opt.value = name;
      opt.textContent = name;
      select.appendChild(opt);
    }

    select.value = selected;
    if (select.value !== selected) {
      for (let i = 0; i < select.options.length; i++) {
        select.options[i].selected = select.options[i].value === selected;
      }
    }
    return selected;
  }

  function syncOverlayGoogleFontDisplay(overlaySection, fontName) {
    const root = overlaySection || document.getElementById("chat-overlay");
    if (!root) return;
    const select = root.querySelector("#overlay-google-font");
    if (!select) return;

    const fam = fillOverlayGoogleFontOptions(select, fontName);
    // Avoid styling the <select> itself with the Google family — WebView2 often
    // renders the closed control blank until the face is ready.
    select.style.fontFamily = "";
    ensureGoogleFontLoaded(fam);
  }

  // Module-level handler so the <select> works even if wireOverlayEvents is late
  let googleFontChangeHandler = null;

  function commitGoogleFontSelection(fontName) {
    const fam = normalizeGoogleFontName(fontName);
    if (typeof googleFontChangeHandler === "function") {
      googleFontChangeHandler(fam);
      return;
    }

    // Fallback when overlay controls are not fully wired yet
    try {
      const profiles = ensureProfiles();
      const activeId = loadActiveProfileId(profiles);
      const settings = loadSettings(activeId);
      settings.googleFont = fam;
      settings.fontSource = "google";
      settings.localFontUrl = null;
      saveSettingsLocal(activeId, settings);
      syncSettingsToServer(activeId, settings);
      applySettingsToUI(settings);
      applySettingsToPreview(settings, activeId);
    } catch (err) {
      console.warn("[ChatOverlayConfig] commitGoogleFontSelection failed:", err);
    }
  }

  function buildOverlayGoogleFontMenu(overlaySection, onFontChange) {
    const root = overlaySection || document.getElementById("chat-overlay");
    if (!root) return;
    const select = root.querySelector("#overlay-google-font");
    if (!select) {
      console.warn("[ChatOverlayConfig] #overlay-google-font select missing");
      return;
    }

    syncOverlayGoogleFontDisplay(root, select.value || "Montserrat");

    // Used by the document-level change listener below
    if (typeof onFontChange === "function") {
      googleFontChangeHandler = onFontChange;
    }
  }

  // Always listen — does not depend on initChatOverlayConfig succeeding
  if (!window.__ssOverlayGoogleFontChangeBound) {
    window.__ssOverlayGoogleFontChangeBound = true;
    document.addEventListener(
      "change",
      (e) => {
        const el = e.target;
        if (!el || el.id !== "overlay-google-font") return;
        commitGoogleFontSelection(el.value);
      },
      true
    );
  }

  function applySettingsToUI(settings) {
    const overlaySection = document.getElementById("chat-overlay");
    if (!overlaySection) return;

    setToggleState(
      overlaySection.querySelector("#overlay-toggle-timestamps"),
      settings.showTimestamps
    );
    setToggleState(
      overlaySection.querySelector("#overlay-toggle-badges"),
      settings.showBadges
    );

    const googleRadio = overlaySection.querySelector("#overlay-font-source-google");
    const localRadio = overlaySection.querySelector("#overlay-font-source-local");
    const googleFontRow = overlaySection.querySelector("#overlay-google-font-row");
    const localFontRow = overlaySection.querySelector("#overlay-local-font-row");
    const googleSelect = overlaySection.querySelector("#overlay-google-font");

    if (googleRadio) googleRadio.checked = settings.fontSource === "google";
    if (localRadio) localRadio.checked = settings.fontSource === "local";

    const useGoogle = settings.fontSource === "google";
    if (googleFontRow) googleFontRow.classList.toggle("hidden", !useGoogle);
    if (localFontRow) localFontRow.classList.toggle("hidden", useGoogle);
    if (googleSelect) {
      const fam = normalizeGoogleFontName(settings.googleFont || "Montserrat");
      settings.googleFont = fam;
      syncOverlayGoogleFontDisplay(overlaySection, fam);
    }

    const fontSizeInput = overlaySection.querySelector("#overlay-font-size");
    if (fontSizeInput) fontSizeInput.value = String(settings.fontSize);

    const rotateInput = overlaySection.querySelector("#overlay-text-rotate");
    const skewInput = overlaySection.querySelector("#overlay-text-skew");
    if (rotateInput) rotateInput.value = String(settings.textRotate);
    if (skewInput) skewInput.value = String(settings.textSkew);

    const strokeWidthRange = overlaySection.querySelector("#overlay-stroke-width");
    const strokeWidthValue = overlaySection.querySelector("#overlay-stroke-width-value");
    const strokeColorInput = overlaySection.querySelector("#overlay-stroke-color");

    const strokeWidth = typeof settings.strokeWidth === "number" ? settings.strokeWidth : 0;
    if (strokeWidthRange) strokeWidthRange.value = String(strokeWidth);
    if (strokeWidthValue) strokeWidthValue.value = String(strokeWidth);
    if (strokeColorInput) strokeColorInput.value = settings.strokeColor || "#000000";

    const orientationSelect = overlaySection.querySelector("#overlay-feed-orientation");
    const orientation = getOrientationFromFeedDirection(settings.feedDirection);
    if (orientationSelect) orientationSelect.value = orientation;

    const stylePlain = overlaySection.querySelector("#overlay-style-plain");
    const styleBubble = overlaySection.querySelector("#overlay-style-bubble");
    if (stylePlain) stylePlain.checked = settings.messageStyle === "plain";
    if (styleBubble) styleBubble.checked = settings.messageStyle === "bubble";

    applyBubbleVisibility(overlaySection, settings);

    const bubbleRadiusInput = overlaySection.querySelector("#overlay-bubble-radius");
    if (bubbleRadiusInput) bubbleRadiusInput.value = String(settings.bubbleRadius);

    const bubbleModeFixed = overlaySection.querySelector("#overlay-bubble-color-fixed");
    const bubbleModeUser = overlaySection.querySelector("#overlay-bubble-color-user");
    const bubbleColorInput = overlaySection.querySelector("#overlay-bubble-color");
    if (bubbleModeFixed) bubbleModeFixed.checked = settings.bubbleColorMode === "fixed";
    if (bubbleModeUser) bubbleModeUser.checked = settings.bubbleColorMode === "user";
    if (bubbleColorInput) bubbleColorInput.value = settings.bubbleColor || "#1f2933";

    const bubbleOpacityRange = overlaySection.querySelector("#overlay-bubble-opacity");
    const bubbleOpacityValue = overlaySection.querySelector("#overlay-bubble-opacity-value");
    const alphaVal = typeof settings.bubbleAlpha === "number" ? settings.bubbleAlpha : 1;
    const percent = Math.round(alphaVal * 100);
    if (bubbleOpacityRange) bubbleOpacityRange.value = String(percent);
    if (bubbleOpacityValue) bubbleOpacityValue.value = String(percent);

    const bgTransparent = overlaySection.querySelector("#overlay-bg-transparent");
    const bgSolid = overlaySection.querySelector("#overlay-bg-solid");
    const bgColorInputEl = overlaySection.querySelector("#overlay-bg-color");
    const bgColorRow = overlaySection.querySelector("#overlay-bg-color-row");

    if (bgTransparent) bgTransparent.checked = settings.bgMode === "transparent";
    if (bgSolid) bgSolid.checked = settings.bgMode === "solid";
    if (bgColorInputEl) bgColorInputEl.value = settings.bgColor || "#000000";
    if (bgColorRow) bgColorRow.classList.toggle("hidden", settings.bgMode === "transparent");

    const displaySolid = overlaySection.querySelector("#overlay-display-solid");
    const displayPopup = overlaySection.querySelector("#overlay-display-popup");
    const popupDurationRow = overlaySection.querySelector("#overlay-popup-duration-row");
    const popupDurationInput = overlaySection.querySelector("#overlay-popup-duration");

    if (displaySolid) displaySolid.checked = settings.displayMode === "solid";
    if (displayPopup) displayPopup.checked = settings.displayMode === "popup";
    if (popupDurationInput) popupDurationInput.value = String(settings.popupDuration);
    if (popupDurationRow)
      popupDurationRow.classList.toggle("hidden", settings.displayMode !== "popup");
  }

  function previewFrameUrl(profileId) {
    return `/overlay/chat?preview=1&profile=${encodeURIComponent(
      normalizeOverlayProfileId(profileId)
    )}&v=font-preview-parity-14`;
  }

  function ensurePreviewFrame(profileId) {
    const frame = document.getElementById("chat-overlay-preview-frame");
    if (!frame) return null;

    const nextSrc = previewFrameUrl(profileId);
    if (frame.getAttribute("src") !== nextSrc) {
      frame.setAttribute("src", nextSrc);
    }
    return frame;
  }

  // Preview loads the same overlay document as OBS and listens for the same
  // `/api/chat/overlay-config` + `overlay-config-updated` WebSocket path.
  // This only keeps the iframe pointed at the active profile.
  function applySettingsToPreview(_settings, profileId) {
    ensurePreviewFrame(profileId || "chat-default");
  }

  // ───────────────────────────────────────────────
  // Local font upload (unchanged behavior)
  // ───────────────────────────────────────────────

  async function loadLocalFont(file, profileId, update) {
    try {
      const contentBase64 = await new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onerror = () => reject(reader.error || new Error("read-failed"));
        reader.onload = () => {
          try {
            const res = String(reader.result || "");
            const idx = res.indexOf(",");
            resolve(idx >= 0 ? res.slice(idx + 1) : res);
          } catch (e) {
            reject(e);
          }
        };
        reader.readAsDataURL(file);
      });

      const uploadRes = await fetch(`${overlayBaseUrl()}/api/chat/upload-font`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          profileId: normalizeOverlayProfileId(profileId),
          fileName: file.name || "font.ttf",
          contentBase64,
        }),
      });

      if (!uploadRes.ok) throw new Error(`upload-failed:${uploadRes.status}`);

      const uploaded = await uploadRes.json();
      const fontFamily = uploaded.fontFamily || "OverlayLocalFont";
      const fontUrl = uploaded.fontUrl || null;

      try {
        const blobUrl = URL.createObjectURL(file);
        const ff = new FontFace(fontFamily, `url(${blobUrl})`);
        await ff.load();
        document.fonts.add(ff);
      } catch (e) {
        console.warn("[ChatOverlayConfig] Preview FontFace load failed:", e);
      }

      update((s) => {
        s.localFontName = fontFamily;
        s.fontSource = "local";
        s.localFontUrl = fontUrl;
      });
    } catch (err) {
      console.warn("[ChatOverlayConfig] Failed to load local font:", err);
    }
  }

  // ───────────────────────────────────────────────
  // Overlay controls wiring
  // ───────────────────────────────────────────────

  function wireOverlayEvents(getActiveProfileId, getSettings, setSettings) {
    const overlaySection = document.getElementById("chat-overlay");
    if (!overlaySection) return;

    buildOverlayGoogleFontMenu(overlaySection, (fontName) => {
      update((s) => {
        s.googleFont = normalizeGoogleFontName(fontName);
        s.fontSource = "google";
        s.localFontUrl = null;
      });
    });

    function update(mutator) {
      const profileId = getActiveProfileId();
      const settings = getSettings();

      if (typeof mutator === "function") mutator(settings);

      if (typeof settings.strokeWidth === "number") {
        settings.strokeEnabled = settings.strokeWidth > 0;
      }

      saveSettingsLocal(profileId, settings);
      syncSettingsToServer(profileId, settings);

      applySettingsToUI(settings);
      applySettingsToPreview(settings, profileId);

      setSettings(settings);

      updateOverlayUrlField(profileId);
    }

    const toggleTimestamps = overlaySection.querySelector("#overlay-toggle-timestamps");
    const toggleBadges = overlaySection.querySelector("#overlay-toggle-badges");

    if (toggleTimestamps)
      toggleTimestamps.addEventListener("click", () =>
        update((s) => (s.showTimestamps = !s.showTimestamps))
      );
    if (toggleBadges)
      toggleBadges.addEventListener("click", () =>
        update((s) => (s.showBadges = !s.showBadges))
      );

    const googleRadio = overlaySection.querySelector("#overlay-font-source-google");
    const localRadio = overlaySection.querySelector("#overlay-font-source-local");
    const localUploadInput = overlaySection.querySelector("#overlay-font-upload");

    if (googleRadio)
      googleRadio.addEventListener("change", () => {
        if (googleRadio.checked) {
          update((s) => {
            s.fontSource = "google";
            // Drop local URL so sync clears it server-side
            s.localFontUrl = null;
          });
        }
      });

    if (localRadio)
      localRadio.addEventListener("change", () => {
        if (localRadio.checked) update((s) => (s.fontSource = "local"));
      });

    if (localUploadInput) {
      localUploadInput.addEventListener("change", () => {
        const file = localUploadInput.files && localUploadInput.files[0];
        if (!file) return;

        const profileId = getActiveProfileId();
        loadLocalFont(file, profileId, (mut) => update(mut));
      });
    }

    const fontSizeInput = overlaySection.querySelector("#overlay-font-size");
    if (fontSizeInput) {
      fontSizeInput.addEventListener("input", () => {
        const value = Number(fontSizeInput.value);
        if (!Number.isFinite(value)) return;
        const clamped = Math.min(Math.max(value, 10), 48);
        update((s) => (s.fontSize = clamped));
      });
    }

    const rotateInput = overlaySection.querySelector("#overlay-text-rotate");
    const skewInput = overlaySection.querySelector("#overlay-text-skew");
    if (rotateInput)
      rotateInput.addEventListener("input", () =>
        update((s) => (s.textRotate = Number(rotateInput.value) || 0))
      );
    if (skewInput)
      skewInput.addEventListener("input", () =>
        update((s) => (s.textSkew = Number(skewInput.value) || 0))
      );

    const strokeWidthRange = overlaySection.querySelector("#overlay-stroke-width");
    const strokeWidthValue = overlaySection.querySelector("#overlay-stroke-width-value");
    const strokeColorInput = overlaySection.querySelector("#overlay-stroke-color");

    function clampStroke(val) {
      const num = Number(val);
      if (!Number.isFinite(num) || num < 0) return 0;
      return Math.min(num, 10);
    }

    if (strokeWidthRange) {
      strokeWidthRange.step = "0.5";
      strokeWidthRange.addEventListener("input", () => {
        const normalized = clampStroke(strokeWidthRange.value);
        if (strokeWidthValue) strokeWidthValue.value = String(normalized);
        update((s) => (s.strokeWidth = normalized));
      });
    }

    if (strokeWidthValue) {
      strokeWidthValue.step = "0.5";
      strokeWidthValue.addEventListener("input", () => {
        const normalized = clampStroke(strokeWidthValue.value);
        if (strokeWidthRange) strokeWidthRange.value = String(normalized);
        update((s) => (s.strokeWidth = normalized));
      });
    }

    if (strokeColorInput)
      strokeColorInput.addEventListener("input", () =>
        update((s) => (s.strokeColor = strokeColorInput.value || "#000000"))
      );

    const orientationSelect = overlaySection.querySelector("#overlay-feed-orientation");
    if (orientationSelect) {
      orientationSelect.addEventListener("change", () => {
        const orientation = orientationSelect.value || "vertical";
        update((s) => {
          s.feedDirection = orientation === "horizontal" ? "left-right" : "up-down";
        });
      });
    }

    const stylePlain = overlaySection.querySelector("#overlay-style-plain");
    const styleBubble = overlaySection.querySelector("#overlay-style-bubble");

    if (stylePlain)
      stylePlain.addEventListener("change", () => {
        if (stylePlain.checked) update((s) => (s.messageStyle = "plain"));
      });

    if (styleBubble)
      styleBubble.addEventListener("change", () => {
        if (styleBubble.checked) update((s) => (s.messageStyle = "bubble"));
      });

    const bubbleRadiusInput = overlaySection.querySelector("#overlay-bubble-radius");
    if (bubbleRadiusInput)
      bubbleRadiusInput.addEventListener("input", () =>
        update((s) => (s.bubbleRadius = Number(bubbleRadiusInput.value) || 0))
      );

    const bubbleModeFixed = overlaySection.querySelector("#overlay-bubble-color-fixed");
    const bubbleModeUser = overlaySection.querySelector("#overlay-bubble-color-user");
    const bubbleColorInput = overlaySection.querySelector("#overlay-bubble-color");

    if (bubbleModeFixed)
      bubbleModeFixed.addEventListener("change", () => {
        if (bubbleModeFixed.checked) update((s) => (s.bubbleColorMode = "fixed"));
      });

    if (bubbleModeUser)
      bubbleModeUser.addEventListener("change", () => {
        if (bubbleModeUser.checked) update((s) => (s.bubbleColorMode = "user"));
      });

    if (bubbleColorInput)
      bubbleColorInput.addEventListener("input", () =>
        update((s) => (s.bubbleColor = bubbleColorInput.value || "#1f2933"))
      );

    const bubbleOpacityRange = overlaySection.querySelector("#overlay-bubble-opacity");
    const bubbleOpacityValue = overlaySection.querySelector("#overlay-bubble-opacity-value");

    function alphaFromPercent(v) {
      const n = Number(v);
      if (!Number.isFinite(n)) return 1;
      return Math.min(Math.max(n, 0), 100) / 100;
    }

    if (bubbleOpacityRange) {
      bubbleOpacityRange.step = "1";
      bubbleOpacityRange.addEventListener("input", () => {
        const fraction = alphaFromPercent(bubbleOpacityRange.value);
        if (bubbleOpacityValue) bubbleOpacityValue.value = String(Math.round(fraction * 100));
        update((s) => (s.bubbleAlpha = fraction));
      });
    }

    if (bubbleOpacityValue) {
      bubbleOpacityValue.step = "1";
      bubbleOpacityValue.addEventListener("input", () => {
        const fraction = alphaFromPercent(bubbleOpacityValue.value);
        if (bubbleOpacityRange) bubbleOpacityRange.value = String(Math.round(fraction * 100));
        update((s) => (s.bubbleAlpha = fraction));
      });
    }

    const bgTransparent = overlaySection.querySelector("#overlay-bg-transparent");
    const bgSolid = overlaySection.querySelector("#overlay-bg-solid");
    const bgColorInput = overlaySection.querySelector("#overlay-bg-color");

    if (bgTransparent)
      bgTransparent.addEventListener("change", () => {
        if (bgTransparent.checked) update((s) => (s.bgMode = "transparent"));
      });

    if (bgSolid)
      bgSolid.addEventListener("change", () => {
        if (bgSolid.checked) update((s) => (s.bgMode = "solid"));
      });

    if (bgColorInput)
      bgColorInput.addEventListener("input", () =>
        update((s) => (s.bgColor = bgColorInput.value || "#000000"))
      );

    const displaySolid = overlaySection.querySelector("#overlay-display-solid");
    const displayPopup = overlaySection.querySelector("#overlay-display-popup");
    const popupDurationInput = overlaySection.querySelector("#overlay-popup-duration");

    if (displaySolid)
      displaySolid.addEventListener("change", () => {
        if (displaySolid.checked) update((s) => (s.displayMode = "solid"));
      });

    if (displayPopup)
      displayPopup.addEventListener("change", () => {
        if (displayPopup.checked) update((s) => (s.displayMode = "popup"));
      });

    if (popupDurationInput) {
      popupDurationInput.addEventListener("input", () => {
        const value = Number(popupDurationInput.value);
        if (!Number.isFinite(value)) return;
        const clamped = Math.min(Math.max(value, 2), 30);
        update((s) => (s.popupDuration = clamped));
      });
    }
  }

  // ───────────────────────────────────────────────
  // Overlay tab: branded profile dropdown
  // ───────────────────────────────────────────────

  function getDropdownEls(overlaySection) {
    return {
      select: overlaySection.querySelector("#chat-overlay-profile-select"),
      button: overlaySection.querySelector("#chat-overlay-profile-button"),
      valueEl: overlaySection.querySelector("#chat-overlay-profile-value"),
      menu: overlaySection.querySelector("#chat-overlay-profile-menu"),
      root: overlaySection.querySelector("#chatProfileDropdown"),
      addBtn: overlaySection.querySelector("#chat-overlay-profile-add"),
    };
  }

  function setDropdownOpen(overlaySection, open) {
    const { button, menu } = getDropdownEls(overlaySection);
    if (!button || !menu) return;
    button.setAttribute("aria-expanded", open ? "true" : "false");
    menu.hidden = !open;
  }

  function renderOverlayProfileSelect(profiles, activeId, overlaySection) {
    const { select, valueEl, menu, button, root } = getDropdownEls(overlaySection);
    if (!select || !valueEl || !menu || !button || !root) return;

    select.innerHTML = "";
    profiles.forEach((p) => {
      const opt = document.createElement("option");
      opt.value = p.id;
      opt.textContent = p.name;
      select.appendChild(opt);
    });
    select.value = activeId || "chat-default";

    const active =
      profiles.find((p) => p.id === (activeId || "chat-default")) ||
      profiles.find((p) => p.id === "default") ||
      profiles[0];

    valueEl.textContent = active?.name || "Default";

    // menu
    menu.innerHTML = "";
    profiles.forEach((p) => {
      const row = document.createElement("div");
      row.className = "profile-item" + (p.id === activeId ? " is-selected" : "");
      row.setAttribute("role", "option");
      row.setAttribute("aria-selected", p.id === activeId ? "true" : "false");

      const name = document.createElement("div");
      name.className = "profile-item-name";
      name.textContent = p.name;

      // delete button is separate (not full-row red overlay)
      const del = document.createElement("button");
      del.type = "button";
      del.className = "profile-item-delete";
      del.textContent = "×";
      del.disabled = p.id === "default";
      del.title = p.id === "default" ? "Default cannot be deleted" : "Delete profile";

      row.addEventListener("click", (e) => {
        const t = e.target;
        if (t && t.closest && t.closest(".profile-item-delete")) return;
        select.value = p.id;
        select.dispatchEvent(new Event("change", { bubbles: true }));
        setDropdownOpen(overlaySection, false);
      });

      del.addEventListener("click", async (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (p.id === "default") return;

        const ok = confirm(`Delete profile "${p.name}"? This cannot be undone.`);
        if (!ok) return;

        const nextProfiles = ensureProfiles().filter((x) => x.id !== p.id);
        saveProfiles(nextProfiles);

        try {
          localStorage.removeItem(getSettingsKey(p.id));
        } catch (_) {}

        const nextActive =
          activeId === p.id
            ? nextProfiles.find((x) => x.id === "default")?.id ||
              nextProfiles[0]?.id ||
              "default"
            : activeId;

        saveActiveProfileId(nextActive);

        // force change path
        select.value = nextActive;
        select.dispatchEvent(new Event("change", { bubbles: true }));
      });

      row.appendChild(name);
      row.appendChild(del);
      menu.appendChild(row);
    });

    if (!root.dataset.bound) {
      root.dataset.bound = "1";

      button.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        const expanded = button.getAttribute("aria-expanded") === "true";
        setDropdownOpen(overlaySection, !expanded);
      });

      document.addEventListener("mousedown", (e) => {
        const t = e.target;
        if (!t || !t.closest) return;
        if (t.closest("#chatProfileDropdown") || t.closest("#chat-overlay-profile-menu"))
          return;
        setDropdownOpen(overlaySection, false);
      });

      document.addEventListener("keydown", (e) => {
        if (e.key === "Escape") setDropdownOpen(overlaySection, false);
      });
    }
  }

  // ───────────────────────────────────────────────
  // Profile loading/switching
  // ───────────────────────────────────────────────

  async function loadProfileIntoUi(profileId, setSettings) {
    let settings = loadSettings(profileId);

    const serverCfg = await fetchServerConfig(profileId);
    if (serverCfg) {
      settings = applyServerConfigToUiSettings(settings, serverCfg);
      saveSettingsLocal(profileId, settings);
    }

    applySettingsToUI(settings);
    applySettingsToPreview(settings, profileId);

    syncSettingsToServer(profileId, settings);
    updateOverlayUrlField(profileId);

    setSettings(settings);
  }

  function wireProfileUi(state) {
    const overlaySection = document.getElementById("chat-overlay");
    if (!overlaySection) return;

    const { select, addBtn } = getDropdownEls(overlaySection);
    if (!select || !addBtn) return;

    // Change handler is THE single source of truth for switching profiles
    select.addEventListener("change", async () => {
      const profiles = ensureProfiles();
      const newId = (select.value || "chat-default").trim() || "chat-default";

      state.activeProfileId = profiles.some((p) => p.id === newId) ? newId : "chat-default";
      saveActiveProfileId(state.activeProfileId);

      renderOverlayProfileSelect(profiles, state.activeProfileId, overlaySection);
      initChatIntegrationsDropdown(state); // keep integrations dropdown synced

      await loadProfileIntoUi(state.activeProfileId, (s) => (state.settings = s));
    });

    // Add profile (modal prompt)
    addBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      e.stopPropagation();

      const name = await modalPrompt({
        title: "New Chat Overlay Profile",
        label: "Profile name",
        placeholder: "e.g. Compact, Big Text, Minimal…",
        initialValue: "New Profile",
      });

      if (!name) return;

      const profiles = ensureProfiles();
      const id = "chat-" + cryptoRandomId();

      profiles.push({ id, name });
      saveProfiles(profiles);

      // Notify Integrations dropdown (and any other listeners) that profiles changed.
      try {
        window.dispatchEvent(new Event("streamsync-chat-profiles-changed"));
      } catch (_) {}

      state.activeProfileId = id;
      saveActiveProfileId(id);

      // Trigger switch path
      select.value = id;
      select.dispatchEvent(new Event("change", { bubbles: true }));
    });

    // initial render
    const profiles = ensureProfiles();
    renderOverlayProfileSelect(profiles, state.activeProfileId, overlaySection);
  }

  // ───────────────────────────────────────────────
  // Public init
  // ───────────────────────────────────────────────

  window.initChatOverlayConfig = async function initChatOverlayConfig() {
    const profiles = ensureProfiles();
    const activeId = loadActiveProfileId(profiles);

    const state = {
      activeProfileId: activeId,
      settings: loadSettings(activeId),
    };

    // Keep integrations URL and dropdown synced
    updateOverlayUrlField(activeId);
    try {
      initChatIntegrationsDropdown(state);
    } catch (err) {
      console.warn(
        "[ChatOverlayConfig] initChatIntegrationsDropdown failed:",
        err
      );
    }

    // Wire overlay controls BEFORE awaiting profile fetch so the Google font
    // <select> is fully populated even if the server request is slow/hangs.
    try {
      wireOverlayEvents(
        () => state.activeProfileId,
        () => state.settings,
        (s) => (state.settings = s)
      );
    } catch (err) {
      console.error("[ChatOverlayConfig] wireOverlayEvents failed:", err);
    }

    // Load profile (with server merge) — applies the saved selection into the select
    await loadProfileIntoUi(activeId, (s) => (state.settings = s));

    // Wire overlay tab profile dropdown + add/delete
    wireProfileUi(state);
  };
})();
