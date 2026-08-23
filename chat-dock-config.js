// chat-dock-config.js
// Handles Chat Dock settings UI + persistence for the Dock subview.

// Simple localStorage key (per-machine, per-user for now)
const CHAT_DOCK_STORAGE_KEY = "streamsync.chatDock.settings";

// Overlay server + profile for the live dock
const DOCK_PROFILE_ID = "chat-default";

(function () {
  const defaultSettings = {
    fontSize: 13,
    showTimestamps: true,
    showBadges: true,
  };

  const FONT_MIN = 8;
  const FONT_MAX = 32;

  function clampFont(n) {
    const v = Number(n);
    if (!Number.isFinite(v)) return defaultSettings.fontSize;
    return Math.min(FONT_MAX, Math.max(FONT_MIN, Math.round(v)));
  }

  function loadSettings() {
    try {
      const raw = window.localStorage.getItem(CHAT_DOCK_STORAGE_KEY);
      if (!raw) return { ...defaultSettings };
      const parsed = JSON.parse(raw);

      return {
        fontSize:
          typeof parsed.fontSize === "number" && parsed.fontSize > 0
            ? clampFont(parsed.fontSize)
            : defaultSettings.fontSize,
        showTimestamps:
          typeof parsed.showTimestamps === "boolean"
            ? parsed.showTimestamps
            : defaultSettings.showTimestamps,
        showBadges:
          typeof parsed.showBadages === "boolean" // legacy typo-safe
            ? parsed.showBadages
            : typeof parsed.showBadges === "boolean"
              ? parsed.showBadges
              : defaultSettings.showBadges,
      };
    } catch (err) {
      console.warn("[ChatDockConfig] Failed to load settings:", err);
      return { ...defaultSettings };
    }
  }

  async function syncSettingsToOverlay(settings) {
    try {
      const payload = {
        profileId: DOCK_PROFILE_ID,
        fontSize: clampFont(settings.fontSize),
        showBadges: !!settings.showBadges,
        showTimestamps: !!settings.showTimestamps,
      };

      await window.streamSyncControlApi.privilegedFetch("/api/chat/dock-config", {
        method: "POST",
        body: JSON.stringify(payload),
      });
    } catch (err) {
      console.warn("[ChatDockConfig] Failed to sync settings to overlay:", err);
    }
  }

  function saveSettings(settings) {
    try {
      window.localStorage.setItem(CHAT_DOCK_STORAGE_KEY, JSON.stringify(settings));
    } catch (err) {
      console.warn("[ChatDockConfig] Failed to save settings:", err);
    }
    syncSettingsToOverlay(settings);
  }

  function setToggleState(button, isOn) {
    if (!button) return;
    button.classList.toggle("on", !!isOn);
    button.setAttribute("aria-pressed", isOn ? "true" : "false");
    const labelEl = button.querySelector(".toggle-label");
    if (labelEl) labelEl.textContent = isOn ? "On" : "Off";
  }

  function applySettingsToUI(settings) {
    const viewEl = document.querySelector(".view-chat");
    if (!viewEl) return;
    const dockSection = viewEl.querySelector("#chat-dock");
    if (!dockSection) return;

    const fontInput = dockSection.querySelector("#chat-dock-font-size");
    const toggleTimestamps = dockSection.querySelector("#chat-dock-toggle-timestamps");
    const toggleBadges = dockSection.querySelector("#chat-dock-toggle-badges");

    if (fontInput) fontInput.value = String(clampFont(settings.fontSize));
    setToggleState(toggleTimestamps, settings.showTimestamps);
    setToggleState(toggleBadges, settings.showBadges);
  }

  function wireEvents(settings) {
    const viewEl = document.querySelector(".view-chat");
    if (!viewEl) return;
    const dockSection = viewEl.querySelector("#chat-dock");
    if (!dockSection) return;

    const fontInput = dockSection.querySelector("#chat-dock-font-size");
    const btnDown = dockSection.querySelector("#chat-dock-font-down");
    const btnUp = dockSection.querySelector("#chat-dock-font-up");

    const toggleTimestamps = dockSection.querySelector("#chat-dock-toggle-timestamps");
    const toggleBadges = dockSection.querySelector("#chat-dock-toggle-badges");

    // Track last committed valid font size so typing doesn't snap to min mid-entry
    let lastCommitted = clampFont(settings.fontSize);

    function commitFontFromInput() {
      if (!fontInput) return;

      const raw = String(fontInput.value || "").trim();

      // If user cleared the field, don't instantly force MIN;
      // restore last committed value on commit.
      if (raw === "") {
        fontInput.value = String(lastCommitted);
        return;
      }

      // Only allow digits while typing; commit requires a valid number
      const parsed = Number(raw);
      if (!Number.isFinite(parsed)) {
        fontInput.value = String(lastCommitted);
        return;
      }

      const clamped = clampFont(parsed);
      lastCommitted = clamped;
      settings.fontSize = clamped;
      fontInput.value = String(clamped);
      saveSettings(settings);
    }

    if (fontInput) {
      // While typing: allow "2" then "24" without clamping/resetting.
      // We only sanitize non-digits lightly.
      fontInput.addEventListener("input", () => {
        const v = String(fontInput.value || "");
        // Keep it numeric-ish (digits only). If you want to allow decimals later, we can adjust.
        const cleaned = v.replace(/[^\d]/g, "");
        if (cleaned !== v) {
          const pos = fontInput.selectionStart || cleaned.length;
          fontInput.value = cleaned;
          try {
            fontInput.setSelectionRange(pos - 1, pos - 1);
          } catch {
            // ignore
          }
        }
      });

      // Commit on blur
      fontInput.addEventListener("blur", () => {
        commitFontFromInput();
      });

      // Commit on Enter
      fontInput.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter") {
          ev.preventDefault();
          commitFontFromInput();
          fontInput.blur();
        }
      });
    }

    // Stepper buttons (these should always commit immediately)
    if (btnDown) {
      btnDown.addEventListener("click", () => {
        const next = clampFont((Number(lastCommitted) || defaultSettings.fontSize) - 1);
        lastCommitted = next;
        settings.fontSize = next;
        if (fontInput) fontInput.value = String(next);
        saveSettings(settings);
      });
    }

    if (btnUp) {
      btnUp.addEventListener("click", () => {
        const next = clampFont((Number(lastCommitted) || defaultSettings.fontSize) + 1);
        lastCommitted = next;
        settings.fontSize = next;
        if (fontInput) fontInput.value = String(next);
        saveSettings(settings);
      });
    }

    if (toggleTimestamps) {
      toggleTimestamps.addEventListener("click", () => {
        settings.showTimestamps = !settings.showTimestamps;
        setToggleState(toggleTimestamps, settings.showTimestamps);
        saveSettings(settings);
      });
    }

    if (toggleBadges) {
      toggleBadges.addEventListener("click", () => {
        settings.showBadges = !settings.showBadges;
        setToggleState(toggleBadges, settings.showBadges);
        saveSettings(settings);
      });
    }
  }

  // Called from renderer.js inside initChatView()
  window.initChatDockConfig = function initChatDockConfig() {
    const viewEl = document.querySelector(".view-chat");
    if (!viewEl) return;
    const dockSection = viewEl.querySelector("#chat-dock");
    if (!dockSection) return;

    const settings = loadSettings();
    applySettingsToUI(settings);
    wireEvents(settings);

    // Ensure overlay profile is hydrated with whatever we just loaded.
    syncSettingsToOverlay(settings);
  };
})();
