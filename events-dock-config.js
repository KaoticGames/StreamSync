// events-dock-config.js
// Stream Sync – Events Dock panel controller
// (Runs from shell.html; binds after renderer injects views/events/events-dock.html)
//
// Goals:
// - "Set it and forget it": autosave on every change (localStorage) + sync to overlay-server.
// - Real-time OBS updates: POST -> server broadcasts {type:"events-dock-config"} over /ws/feed.
// - No "Save" button required.

(() => {
  const STORE_KEY = "streamsync.eventsDock.config.v2";

  const EVT_IDS = [
    "follow",
    "sub",
    "resub",
    "gift",
    "bits",
    "raid",
    "redeem",
    "hypetrain",
    "announce",
  ];

  const DEFAULTS = {
    fontSize: 13,
    showTimestamps: true,
    autoScroll: true,
    density: "comfortable", // compact | comfortable | roomy
    maxItems: 150,
    events: Object.fromEntries(EVT_IDS.map((k) => [k, true])),
  };

  const FONT_MIN = 8;
  const FONT_MAX = 32;
  const FONT_STEP = 1;

  function apiBase() {
    try {
      if (location && (location.protocol === "http:" || location.protocol === "https:")) {
        return location.origin;
      }
    } catch (_) {}
    return (window.STREAMSYNC_OVERLAY && window.STREAMSYNC_OVERLAY.baseUrl) || "http://localhost:4040";
  }

  function clampFont(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return DEFAULTS.fontSize;
    return Math.min(Math.max(n, FONT_MIN), FONT_MAX);
  }

  function clampMaxItems(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return DEFAULTS.maxItems;
    return Math.min(Math.max(Math.floor(n), 5), 300);
  }

  function normalizeDensity(v) {
    const s = typeof v === "string" ? v.toLowerCase() : "";
    return ["compact", "comfortable", "roomy"].includes(s) ? s : DEFAULTS.density;
  }

  function readSaved() {
    try {
      const raw = localStorage.getItem(STORE_KEY);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch (e) {
      console.warn("[events-dock-config] Failed to parse saved config:", e);
      return null;
    }
  }

  function mergeDefaults(saved) {
    const s = saved && typeof saved === "object" ? saved : {};
    const se = s.events && typeof s.events === "object" ? s.events : {};

    return {
      fontSize: typeof s.fontSize === "number" ? clampFont(s.fontSize) : DEFAULTS.fontSize,
      showTimestamps: s.showTimestamps === false ? false : true, // default ON
      autoScroll: s.autoScroll === false ? false : true, // default ON
      density: normalizeDensity(s.density),
      maxItems: typeof s.maxItems === "number" ? clampMaxItems(s.maxItems) : DEFAULTS.maxItems,
      events: Object.fromEntries(
        EVT_IDS.map((k) => [k, se[k] === false ? false : true]) // default ON
      ),
    };
  }

  // ───────────────────────────────────────────────
  // Server sync (debounced)
  // ───────────────────────────────────────────────
  let _syncTimer = null;
  let _syncInFlight = false;
  let _syncQueued = false;
  let _lastSyncedJson = "";

  function buildServerPayload(cfg) {
    return {
      fontSize: cfg.fontSize,
      showTimestamps: cfg.showTimestamps,
      autoScroll: cfg.autoScroll,
      density: cfg.density,
      maxItems: cfg.maxItems,
      events: { ...cfg.events },
    };
  }

  async function syncToServer(cfg) {
    if (_syncInFlight) {
      _syncQueued = true;
      return;
    }

    const payload = buildServerPayload(cfg);
    const json = JSON.stringify(payload);
    if (json === _lastSyncedJson) return;

    _syncInFlight = true;
    try {
      const res = await window.streamSyncControlApi.privilegedFetch(
        "/api/events/dock-config",
        {
          method: "POST",
          body: json,
        }
      );

      if (!res.ok) {
        const t = await res.text().catch(() => "");
        throw new Error("HTTP " + res.status + " " + t);
      }

      _lastSyncedJson = json;
    } catch (e) {
      console.warn("[events-dock-config] Failed to sync to server:", e?.message || e);
    } finally {
      _syncInFlight = false;
      if (_syncQueued) {
        _syncQueued = false;
        try {
          await syncToServer(cfg);
        } catch (_) {}
      }
    }
  }

  function scheduleServerSync(cfg) {
    if (_syncTimer) clearTimeout(_syncTimer);
    _syncTimer = setTimeout(() => syncToServer(cfg), 120);
  }

  function save(cfg) {
    try {
      localStorage.setItem(STORE_KEY, JSON.stringify(cfg));
    } catch (e) {
      console.warn("[events-dock-config] Failed to save config:", e);
    }

    try {
      window.dispatchEvent(
        new CustomEvent("streamsync:eventsDockConfigChanged", { detail: cfg })
      );
    } catch {}

    scheduleServerSync(cfg);
  }

  async function fetchServerConfig() {
    try {
      const res = await fetch(`${apiBase()}/api/events/dock-config`, { cache: "no-store" });
      if (!res.ok) return null;
      const json = await res.json().catch(() => null);
      const cfg = json?.config || null;
      return cfg && typeof cfg === "object" ? cfg : null;
    } catch {
      return null;
    }
  }

  // ───────────────────────────────────────────────
  // UI wiring
  // ───────────────────────────────────────────────
  function hideBadgesToggleIfPresent(panel) {
    const cb = panel.querySelector("#evtDock_showBadges");
    if (!cb) return;

    // Hide the most likely "row" wrapper first, fallback to the checkbox itself.
    const row =
      cb.closest(".row") ||
      cb.closest(".field") ||
      cb.closest("label") ||
      cb.parentElement;

    if (row) row.style.display = "none";
  }

  function getEls(panel) {
    return {
      panel,
      elFont: panel.querySelector("#evtDock_fontSize"),
      btnDown: panel.querySelector("#evtDock_fontDown"),
      btnUp: panel.querySelector("#evtDock_fontUp"),
      elShowTs: panel.querySelector("#evtDock_showTimestamps"),

      // Optional elements (only if your HTML includes them)
      elAutoScroll: panel.querySelector("#evtDock_autoScroll"),
      elDensity: panel.querySelector("#evtDock_density"),
      elMaxItems: panel.querySelector("#evtDock_maxItems"),

      evtEls: Object.fromEntries(
        EVT_IDS.map((k) => [k, panel.querySelector("#evtDock_evt_" + k)])
      ),
    };
  }

  function applyToUI(els, cfg) {
    if (els.elFont) els.elFont.value = String(clampFont(cfg.fontSize));
    if (els.elShowTs) els.elShowTs.checked = cfg.showTimestamps !== false;

    if (els.elAutoScroll) els.elAutoScroll.checked = cfg.autoScroll !== false;
    if (els.elDensity) els.elDensity.value = normalizeDensity(cfg.density);
    if (els.elMaxItems) els.elMaxItems.value = String(clampMaxItems(cfg.maxItems));

    EVT_IDS.forEach((k) => {
      const el = els.evtEls[k];
      if (!el) return;
      el.checked = cfg.events[k] !== false;
    });
  }

  function alreadyBound(panel) {
    return panel.dataset.eventsDockBound === "1";
  }
  function markBound(panel) {
    panel.dataset.eventsDockBound = "1";
  }

  async function bind(panel) {
    if (!panel || alreadyBound(panel)) return;

    hideBadgesToggleIfPresent(panel);

    const els = getEls(panel);

    if (!els.elFont || !els.btnDown || !els.btnUp || !els.elShowTs) {
      return;
    }

    const saved = readSaved();
    let cfg = mergeDefaults(saved);

    const serverCfg = await fetchServerConfig();
    if (serverCfg) {
      cfg = mergeDefaults({
        ...cfg,
        ...serverCfg,
        events: { ...(cfg.events || {}), ...(serverCfg.events || {}) },
      });

      try {
        localStorage.setItem(STORE_KEY, JSON.stringify(cfg));
      } catch (_) {}
    } else if (!saved) {
      save(cfg);
    }

    applyToUI(els, cfg);
    scheduleServerSync(cfg);

    els.elFont.addEventListener("wheel", (e) => e.preventDefault());

    els.elFont.addEventListener("input", () => {
      cfg.fontSize = clampFont(els.elFont.value);
      els.elFont.value = String(cfg.fontSize);
      save(cfg);
    });

    els.elFont.addEventListener("blur", () => {
      cfg.fontSize = clampFont(els.elFont.value);
      els.elFont.value = String(cfg.fontSize);
      save(cfg);
    });

    els.btnDown.addEventListener("click", () => {
      cfg.fontSize = clampFont((cfg.fontSize || DEFAULTS.fontSize) - FONT_STEP);
      els.elFont.value = String(cfg.fontSize);
      save(cfg);
    });

    els.btnUp.addEventListener("click", () => {
      cfg.fontSize = clampFont((cfg.fontSize || DEFAULTS.fontSize) + FONT_STEP);
      els.elFont.value = String(cfg.fontSize);
      save(cfg);
    });

    els.elShowTs.addEventListener("change", () => {
      cfg.showTimestamps = !!els.elShowTs.checked;
      save(cfg);
    });

    if (els.elAutoScroll) {
      els.elAutoScroll.addEventListener("change", () => {
        cfg.autoScroll = !!els.elAutoScroll.checked;
        save(cfg);
      });
    }

    if (els.elDensity) {
      els.elDensity.addEventListener("change", () => {
        cfg.density = normalizeDensity(els.elDensity.value);
        els.elDensity.value = cfg.density;
        save(cfg);
      });
    }

    if (els.elMaxItems) {
      els.elMaxItems.addEventListener("wheel", (e) => e.preventDefault());
      els.elMaxItems.addEventListener("input", () => {
        cfg.maxItems = clampMaxItems(els.elMaxItems.value);
        els.elMaxItems.value = String(cfg.maxItems);
        save(cfg);
      });
      els.elMaxItems.addEventListener("blur", () => {
        cfg.maxItems = clampMaxItems(els.elMaxItems.value);
        els.elMaxItems.value = String(cfg.maxItems);
        save(cfg);
      });
    }

    EVT_IDS.forEach((k) => {
      const el = els.evtEls[k];
      if (!el) return;
      el.addEventListener("change", () => {
        cfg.events[k] = !!el.checked;
        save(cfg);
      });
    });

    markBound(panel);
    console.log("[events-dock-config] Bound. apiBase =", apiBase(), "cfg =", cfg);
  }

  function boot() {
    const root = document.getElementById("app-root");
    if (!root) return;

    const tryBindNow = () => {
      const panel = document.getElementById("eventsDockPanel");
      if (panel) bind(panel);
    };

    tryBindNow();

    const obs = new MutationObserver(() => tryBindNow());
    obs.observe(root, { childList: true, subtree: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
