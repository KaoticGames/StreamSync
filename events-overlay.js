// events-overlay.js
(() => {
  const qs = new URLSearchParams(location.search);
  const profileId = (qs.get("profile") || "default").toString();
  const DOCK_PLATFORM = window.STREAMSYNC_DOCK_PLATFORM || qs.get("platform") || "twitch";
  function eventPlatform(payload) {
    return payload && payload.platform ? String(payload.platform) : "twitch";
  }

  const stage = document.getElementById("stage");
  const imgEl = document.getElementById("imgEl");
  const imgTag = document.getElementById("imgTag");
  const vidTag = document.getElementById("vidTag");
  const textEl = document.getElementById("textEl");

  const wsUrl = (() => {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    return `${proto}://${location.host}/ws/feed?profile=${encodeURIComponent(
      profileId
    )}`;
  })();

  let cfg = null;
  let audio = null;

  // Monotonic play generation — stale async must not mutate a newer alert.
  const alertGen = window.StreamSyncAlertDelivery.createGenerationGate();

  /** Resolves true only if gen is still current when the timer fires. */
  function delayIfCurrent(ms, gen) {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve(alertGen.isCurrent(gen));
      }, ms);
    });
  }

    // -------------------------
  // On-screen debug logger (OBS-friendly)
  // Usage: add &debug=1 to the browser source URL
  // -------------------------
  const DEBUG = (() => {
    try {
      const q = new URLSearchParams(location.search);
      return q.get("debug") === "1" || q.get("debug") === "true";
    } catch {
      return false;
    }
  })();

  let debugEl = null;

  function dlog(...args) {
    if (!DEBUG) return;
    if (!debugEl) {
      debugEl = document.createElement("pre");
      debugEl.style.position = "fixed";
      debugEl.style.left = "8px";
      debugEl.style.bottom = "8px";
      debugEl.style.maxWidth = "60vw";
      debugEl.style.maxHeight = "35vh";
      debugEl.style.overflow = "auto";
      debugEl.style.padding = "8px 10px";
      debugEl.style.borderRadius = "10px";
      debugEl.style.background = "rgba(0,0,0,0.65)";
      debugEl.style.color = "#fff";
      debugEl.style.font = "12px/1.35 ui-monospace, SFMono-Regular, Menlo, monospace";
      debugEl.style.zIndex = "999999";
      debugEl.style.whiteSpace = "pre-wrap";
      document.body.appendChild(debugEl);
    }

    const line =
      `[${new Date().toLocaleTimeString()}] ` +
      args
        .map((a) => {
          if (typeof a === "string") return a;
          try { return JSON.stringify(a); } catch { return String(a); }
        })
        .join(" ");
    debugEl.textContent = (debugEl.textContent + "\n" + line).trim().slice(-6000);
  }

  // -------------------------
  // Helpers
  // -------------------------

  function applyVars(str, vars) {
    const v = vars || {};
    return String(str || "").replace(
      /\[(name|user|amount|months|reward|input|recipient)\]/g,
      (_, key) => {
        if (key === "user") {
          return v.user != null
            ? String(v.user)
            : v.name != null
              ? String(v.name)
              : "";
        }
        return v[key] == null ? "" : String(v[key]);
      }
    );
  }

  function clamp(n, min, max) {
    n = Number(n);
    if (Number.isNaN(n)) return min;
    return Math.max(min, Math.min(max, n));
  }

  function safeLower(s) {
    return String(s || "").trim().toLowerCase();
  }

  function setElBox(el, box) {
    if (!el || !box) return;
    el.style.left = `${box.x || 0}px`;
    el.style.top = `${box.y || 0}px`;
    el.style.width = `${box.w || 0}px`;
    el.style.height = `${box.h || 0}px`;
  }

  /**
   * Shrink font so content fits inside el's fixed box.
   * `maxFontSize` is the authored size (ceiling). Floor is 10px.
   *
   * Uses an off-DOM probe — flex-centered boxes do not reliably grow
   * scrollWidth/scrollHeight when text overflows, so measuring `el` directly fails.
   */
  function fitTextToBox(el, maxFontSize, minFontSize = 10) {
    if (!el) return minFontSize;
    const max = clamp(Number(maxFontSize) || 54, 1, 500);
    const min = clamp(Number(minFontSize) || 10, 1, max);

    const targetW = el.clientWidth;
    const targetH = el.clientHeight;
    if (targetW <= 0 || targetH <= 0) {
      el.style.fontSize = `${max}px`;
      return max;
    }

    const cs = window.getComputedStyle(el);
    const probe = document.createElement("div");
    probe.setAttribute("aria-hidden", "true");
    probe.textContent = el.textContent || "";
    probe.style.cssText = [
      "position:absolute",
      "left:-99999px",
      "top:0",
      "visibility:hidden",
      "pointer-events:none",
      `width:${targetW}px`,
      "height:auto",
      "box-sizing:border-box",
      `padding:${cs.paddingTop} ${cs.paddingRight} ${cs.paddingBottom} ${cs.paddingLeft}`,
      `font-family:${cs.fontFamily}`,
      `font-weight:${cs.fontWeight}`,
      `font-style:${cs.fontStyle}`,
      `letter-spacing:${cs.letterSpacing}`,
      `line-height:${cs.lineHeight}`,
      `white-space:${cs.whiteSpace || "pre-wrap"}`,
      `text-align:${cs.textAlign || "center"}`,
      `word-break:${cs.wordBreak}`,
      `overflow-wrap:${cs.overflowWrap}`,
      "-webkit-text-stroke:" + (cs.webkitTextStroke || "0"),
    ].join(";");
    document.body.appendChild(probe);

    function overflows(size) {
      probe.style.fontSize = `${size}px`;
      void probe.offsetWidth;
      return (
        probe.offsetHeight > targetH + 1 ||
        probe.scrollWidth > targetW + 1
      );
    }

    let best = max;
    try {
      if (overflows(max)) {
        let lo = min;
        let hi = max;
        best = min;
        while (lo <= hi) {
          const mid = (lo + hi) >> 1;
          if (!overflows(mid)) {
            best = mid;
            lo = mid + 1;
          } else {
            hi = mid - 1;
          }
        }
      }
    } finally {
      probe.remove();
    }

    el.style.fontSize = `${best}px`;
    return best;
  }

  function isVideoSrc(src) {
    if (!src) return false;
    if (String(src).startsWith("data:video/")) return true;
    return /\.(mp4|webm|ogg|mov|m4v)(\?|#|$)/i.test(String(src));
  }

  /** Resolve relative /events-media/ paths for OBS browser sources. */
  function resolveMediaSrc(src) {
    if (!src) return "";
    const s = String(src).trim();
    if (
      s.startsWith("http://") ||
      s.startsWith("https://") ||
      s.startsWith("data:")
    ) {
      return s;
    }
    if (s.startsWith("/")) {
      return `${location.origin}${s}`;
    }
    return s;
  }

  function clearVisualMedia() {
    if (imgTag) {
      imgTag.removeAttribute("src");
      imgTag.classList.remove("show");
    }
    if (vidTag) {
      try {
        vidTag.pause();
      } catch {}
      vidTag.removeAttribute("src");
      vidTag.classList.remove("show");
      try {
        vidTag.load();
      } catch {}
    }
  }

  function setVisualMedia(src) {
    clearVisualMedia();
    if (!src) return;

    const resolved = resolveMediaSrc(src);
    const cacheBust =
      resolved.startsWith("http://") || resolved.startsWith("https://")
        ? resolved + (resolved.includes("?") ? "&" : "?") + "t=" + Date.now()
        : resolved;

    if (isVideoSrc(resolved)) {
      if (!vidTag) return;
      vidTag.src = cacheBust;
      vidTag.muted = true;
      vidTag.loop = false;
      vidTag.playsInline = true;
      vidTag.classList.add("show");
      vidTag.play().catch(() => {});
    } else if (imgTag) {
      imgTag.src = cacheBust;
      imgTag.classList.add("show");
    }
  }

  function extractVars(msg) {
    if (msg?.data?.variables && typeof msg.data.variables === "object") {
      return msg.data.variables;
    }
    if (msg?.variables && typeof msg.variables === "object") {
      return msg.variables;
    }
    if (msg?.data && typeof msg.data === "object") {
      return msg.data;
    }
    return {};
  }

  /** Test-alert override (0–1), when Events Studio pushes a live preview. */
  function extractSoundVolume(msg) {
    const raw = msg?.soundVolume ?? msg?.data?.soundVolume;
    if (raw == null || raw === "") return null;
    const n = Number(raw);
    return Number.isFinite(n) ? clamp(n, 0, 100) / 100 : null;
  }

  // -------------------------
  // Config + Stage scaling
  // -------------------------

  async function loadConfig() {
    const res = await fetch(
      `/api/events/overlay-config?profile=${encodeURIComponent(profileId)}`,
      { cache: "no-store" }
    );
    const json = await res.json();
    cfg = json?.config || json?.profile || null;

    const w = Number(cfg?.stage?.w || 1280);
    const h = Number(cfg?.stage?.h || 720);
    if (stage) {
      stage.style.width = `${w}px`;
      stage.style.height = `${h}px`;
    }
    rescaleStage();
    preloadConfigFonts();
  }

  function preloadConfigFonts() {
    if (!cfg?.events) return;
    for (const eventType of Object.keys(cfg.events)) {
      const ev = cfg.events[eventType];
      const vars = Array.isArray(ev?.variations) ? ev.variations : [];
      for (const v of vars) {
        const t = effectiveText(eventType, v);
        ensureTextFontLoaded(t);
      }
    }
  }

  function rescaleStage() {
    if (!stage || !cfg) return;

    const w = Number(cfg?.stage?.w || 1280);
    const h = Number(cfg?.stage?.h || 720);

    const vw = window.innerWidth || w;
    const vh = window.innerHeight || h;

    const scale = Math.min(vw / w, vh / h);
    stage.style.transform = `scale(${scale})`;
  }

  window.addEventListener("resize", () => rescaleStage());

  const picker = () => window.StreamSyncVariationPicker;

  function eventConfigFor(eventType) {
    return cfg?.events?.[eventType] || null;
  }

  function baseVariationFor(eventType) {
    return picker().baseVariationForEvent(eventConfigFor(eventType), eventType);
  }

  function pickVariation(eventType, variationId, variables) {
    return picker().pickVariation({
      eventConfig: eventConfigFor(eventType),
      eventType,
      variationId,
      variables,
    });
  }

  // -------------------------
  // Effective field resolvers
  // -------------------------

  // placement must ALWAYS come from base/root
  function effectivePlacement(eventType) {
    const base = baseVariationFor(eventType);
    return base?.placement || null;
  }

  function effectiveImageSrc(eventType, v) {
    const base = baseVariationFor(eventType);
    const img = v?.image || {};
    const bimg = base?.image || {};

    // Explicit value wins
    if (img.type === "url" && img.value) return img.value;
    if (img.type === "data" && img.value) return img.value;

    // inherit uses base
    if (img.type === "inherit") {
      if (bimg.type === "url" && bimg.value) return bimg.value;
      if (bimg.type === "data" && bimg.value) return bimg.value;
      return "";
    }

    // blank override falls back to base
    if (bimg.type === "url" && bimg.value) return bimg.value;
    if (bimg.type === "data" && bimg.value) return bimg.value;
    return "";
  }

  function effectiveSoundSrc(eventType, v) {
    const base = baseVariationFor(eventType);
    const snd = v?.sound || {};
    const bs = base?.sound || {};

    if (snd.type === "url" && snd.value) return snd.value;
    if (snd.type === "data" && snd.value) return snd.value;

    if (snd.type === "inherit") {
      if (bs.type === "url" && bs.value) return bs.value;
      if (bs.type === "data" && bs.value) return bs.value;
      return "";
    }

    if (bs.type === "url" && bs.value) return bs.value;
    if (bs.type === "data" && bs.value) return bs.value;
    return "";
  }

  /** Sound volume 0–1 from config (variation override, else base, default 1). */
  function effectiveSoundVolume(eventType, v) {
    const base = baseVariationFor(eventType);
    const snd = v?.sound || {};
    const bs = base?.sound || {};
    const own = Number(snd.volume);
    if (Number.isFinite(own)) return clamp(own, 0, 100) / 100;
    if (base && v && v.id !== base.id) {
      const baseVol = Number(bs.volume);
      if (Number.isFinite(baseVol)) return clamp(baseVol, 0, 100) / 100;
    }
    return 1;
  }

  function effectiveMessage(eventType, v) {
    const base = baseVariationFor(eventType);
    const msg = v?.message;
    if (msg != null && String(msg).length) return msg;
    return base?.message || "";
  }

  function effectiveDurationSec(eventType, v) {
    const base = baseVariationFor(eventType);
    const d = v?.durationSec;
    if (d != null && d !== "") return Number(d);
    return Number(base?.durationSec || 6);
  }

  // ---- TEXT INHERIT ----
  // Rules:
  // - Base defines everything.
  // - Variation may override any text fields, OR inherit base.
  // - We support a few possible flags to detect inherit, since configs can differ:
  //   t.mode === "inherit", t.inherit === true, t.type === "inherit"
  function variationTextIsInherit(v) {
    const t = v?.text;
    if (!t) return true; // absent => inherit base
    const mode = safeLower(t.mode);
    const type = safeLower(t.type);
    if (t.inherit === true) return true;
    if (mode === "inherit") return true;
    if (type === "inherit") return true;
    return false;
  }

  function effectiveText(eventType, v) {
    const base = baseVariationFor(eventType);
    const bt = base?.text || {};
    const vt = v?.text || null;

    if (!vt || variationTextIsInherit(v)) {
      return bt;
    }

    // Merge: base as default, variation overrides
    const merged = { ...bt, ...vt };

    // ✅ FONT INHERIT (variation wants base font settings)
    // Detect inherit more broadly (source/mode/fontSource)
    const vMeta = vt?.fontMeta || {};
    const vSrc = safeLower(vMeta.source || vMeta.mode || vt?.fontSource || "");

    // Some editors store a "default" family even when inherit is selected
    function isDefaultFamily(f) {
      const s = safeLower(f);
      return !s || s === "system-ui" || s === "default" || s === "inherit";
    }

    if (vSrc === "inherit") {
      // Force base meta + base family (do not let variation’s placeholder override)
      const baseMeta = bt?.fontMeta || {};
      merged.fontMeta = baseMeta;

      // Always inherit base family unless base is empty (googleFamily covers that)
      merged.fontFamily = bt?.fontFamily || "";

      // If variation had a meaningful family but still chose inherit, ignore it.
      // (We intentionally do nothing else here.)

      // Optional: if your base is google-based and only uses googleFamily,
      // leaving merged.fontFamily blank is OK because applyTextStyleFromText prefers meta.googleFamily.
    }

    return merged;
  }

  // -------------------------
  // Font loading (Google + Local) + wait
  // -------------------------

  function ensureGoogleFontLoaded(family) {
    const fam = String(family || "").trim();
    if (!fam) return;
    const id = "gf-" + fam.toLowerCase().replace(/[^a-z0-9]+/g, "-");
    if (document.getElementById(id)) return;

    const link = document.createElement("link");
    link.id = id;
    link.rel = "stylesheet";
    link.href =
      "https://fonts.googleapis.com/css2?family=" +
      encodeURIComponent(fam).replace(/%20/g, "+") +
      ":wght@100;200;300;400;500;600;700;800;900&display=swap";
    document.head.appendChild(link);
  }

  function ensureLocalFontLoaded(fontFamily, fontUrl) {
    const fam = String(fontFamily || "").trim();
    const url = resolveMediaSrc(fontUrl);
    if (!fam || !url) return;

    const id = "lf-" + fam.toLowerCase().replace(/[^a-z0-9]+/g, "-");

    // Pick a better format hint when possible (helps some Chromium/OBS cases)
    const u = url.toLowerCase();
    const fmt =
      u.includes("data:font/woff2") || u.endsWith(".woff2") ? "woff2" :
      u.includes("data:font/woff")  || u.endsWith(".woff")  ? "woff"  :
      u.includes("data:font/otf")   || u.endsWith(".otf")   ? "opentype" :
      "truetype";

    const css =
      `@font-face{font-family:${JSON.stringify(fam)};` +
      `src:url(${JSON.stringify(url)}) format('${fmt}');` +
      `font-weight:100 900;font-style:normal;font-display:swap;}`;

    const existing = document.getElementById(id);

    // ✅ If the style exists but points at a different URL, replace it.
    if (existing) {
      if (existing.textContent !== css) {
        existing.textContent = css;
        // Optional: bump a data attr so you can debug changes in DevTools
        existing.dataset.updatedAt = String(Date.now());
      }
      return;
    }

    const style = document.createElement("style");
    style.id = id;
    style.textContent = css;
    document.head.appendChild(style);
  }

  function ensureTextFontLoaded(t) {
    const meta = t?.fontMeta || {};
    const source = safeLower(meta.source);

    if (source === "local") {
      const localUrl =
        meta.localFontUrl ||
        meta.localUrl ||
        meta.localFontDataUrl ||
        meta.url ||
        "";

      // Prefer explicit local family if you ever store one, else use fontFamily
      const fam = meta.localFamily || t.fontFamily;

      ensureLocalFontLoaded(fam, localUrl);
      return;
    }

    // Default to Google
    const gf = meta.googleFamily || t.fontFamily;
    if (gf) ensureGoogleFontLoaded(gf);
  }

  async function waitForFont(family, weight = 400) {
    const fam = String(family || "").trim();
    if (!fam) return;

    // If Font Loading API unsupported, just proceed
    if (!document.fonts || !document.fonts.load) return;

    try {
      // Weight may be non-numeric; fall back
      const w = Number(weight);
      const useW = Number.isFinite(w) ? w : 400;

      // Load + wait until ready
      await document.fonts.load(`${useW} 32px "${fam}"`);
      await document.fonts.ready;
    } catch {
      // ignore
    }
  }

  // -------------------------
  // Apply styles
  // -------------------------

  function applyTextStyleFromText(t) {
    if (!textEl) return;

    const meta = t?.fontMeta || {};
    const source = safeLower(meta.source);

    // ✅ Resolve the *actual* family used for rendering
    // - local: use t.fontFamily (the local font-face name)
    // - google/default: prefer meta.googleFamily, fall back to t.fontFamily
    const resolvedFamily =
      source === "local"
        ? (meta.localFamily || t.fontFamily || "")
        : (meta.googleFamily || t.fontFamily || "");

    // Force refresh (OBS/Chromium can be stubborn about reusing fallback)
    textEl.style.fontFamily = "";
    textEl.style.fontFamily = resolvedFamily || "system-ui";

    textEl.style.fontSize = `${t.fontSize || 54}px`;
    textEl.style.fontWeight = String(t.fontWeight || 800);
    textEl.style.color = t.color || "#fff";

    const sw = Number(t.strokeWidth || 0);
    if (sw > 0) {
      textEl.style.webkitTextStroke = `${sw}px ${
        t.strokeColor || "rgba(0,0,0,0.75)"
      }`;
    } else {
      textEl.style.webkitTextStroke = "";
    }
  }

  // -------------------------
  // Hide/reset
  // -------------------------

  const ANIM_STYLES = ["none", "fade", "slide", "zoom"];
  const SLIDE_DIRS = ["up", "down", "left", "right"];

  function normalizeAnimStyle(s) {
    const x = String(s ?? "").trim().toLowerCase();
    if (x === "pop" || x === "flash") return "none";
    if (!ANIM_STYLES.includes(x)) return "fade";
    return x;
  }

  function normalizeSlideDir(s) {
    const x = String(s ?? "").trim().toLowerCase();
    if (!SLIDE_DIRS.includes(x)) return "down";
    return x;
  }

  /** 0 = slowest, 100 = fastest */
  function normalizeAnimSpeed(n) {
    const v = Number(n);
    if (!Number.isFinite(v)) return 50;
    return clamp(Math.round(v), 0, 100);
  }

  function animField(av, bv, key, normalizer, fallback) {
    let val = av?.[key];
    if (bv && (val == null || val === "")) val = bv?.[key];
    return normalizer(val ?? fallback);
  }

  function resolveAnimSpeed(av) {
    if (Number.isFinite(av?.animSpeed)) return normalizeAnimSpeed(av.animSpeed);
    if (Number.isFinite(av?.speedPct)) {
      return clamp(100 - Math.round((Number(av.speedPct) - 25) / 2.75), 0, 100);
    }
    return 50;
  }

  function effectiveAnimationInOut(eventType, v) {
    const base = baseVariationFor(eventType);
    const av = v?.animation || {};
    const bv = base && v && base.id !== v.id ? base.animation || {} : null;

    let animSpeed = resolveAnimSpeed(av);
    if (bv && av.animSpeed == null && av.speedPct == null) {
      animSpeed = resolveAnimSpeed(bv);
    }

    return {
      in: animField(av, bv, "in", normalizeAnimStyle, "fade"),
      out: animField(av, bv, "out", normalizeAnimStyle, "fade"),
      animSpeed,
      slideInDir: animField(av, bv, "slideInDir", normalizeSlideDir, "down"),
      slideOutDir: animField(av, bv, "slideOutDir", normalizeSlideDir, "down"),
    };
  }

  function getStageSize() {
    return {
      w: Number(cfg?.stage?.w) || 1280,
      h: Number(cfg?.stage?.h) || 720,
    };
  }

  function animSpeedToDurationMs(style, animSpeed) {
    if (style === "none") return 0;
    const t = normalizeAnimSpeed(animSpeed) / 100;
    const mult = 3 - t * 2.67;
    return Math.max(40, Math.round(180 * mult));
  }

  function elementBox(el) {
    return {
      x: Number.parseFloat(el?.style?.left) || 0,
      y: Number.parseFloat(el?.style?.top) || 0,
      w: el?.offsetWidth || Number.parseFloat(el?.style?.width) || 0,
      h: el?.offsetHeight || Number.parseFloat(el?.style?.height) || 0,
    };
  }

  function slideViewportOffset(box, dir, stage) {
    const d = normalizeSlideDir(dir);
    if (d === "up") return { x: 0, y: -(box.y + box.h) };
    if (d === "down") return { x: 0, y: stage.h - box.y };
    if (d === "left") return { x: -(box.x + box.w), y: 0 };
    return { x: stage.w - box.x, y: 0 };
  }

  // For slide-in, our keyframes animate from an offset -> settled position.
  // The config UI treats the selected direction as the *movement* direction,
  // so we invert the offset we calculate.
  function invertSlideDir(dir) {
    const d = normalizeSlideDir(dir);
    if (d === "up") return "down";
    if (d === "down") return "up";
    if (d === "left") return "right";
    return "left";
  }

  function applyAnimToEl(el, phase, anim, stage) {
    if (!el) return;
    const style = phase === "in" ? anim.in : anim.out;
    const ms = animSpeedToDurationMs(style, anim.animSpeed);

    if (style === "none") return;

    if (style === "slide") {
      const box = elementBox(el);
      if (phase === "in") {
        const off = slideViewportOffset(box, invertSlideDir(anim.slideInDir), stage);
        el.style.setProperty("--slide-x", `${off.x}px`);
        el.style.setProperty("--slide-y", `${off.y}px`);
        el.classList.add("anim-in-slide-viewport");
      } else {
        const off = slideViewportOffset(box, anim.slideOutDir, stage);
        el.style.setProperty("--slide-x", `${off.x}px`);
        el.style.setProperty("--slide-y", `${off.y}px`);
        el.classList.add("anim-out-slide-viewport");
      }
      el.style.animationDuration = `${ms}ms`;
      return;
    }

    const prefix = phase === "in" ? "anim-in" : "anim-out";
    el.classList.add(`${prefix}-${style}`);
    el.style.animationDuration = `${ms}ms`;
  }

  function clearAnimClasses(el) {
    if (!el || !el.classList) return;
    for (const c of Array.from(el.classList)) {
      if (
        c === "fade-in" ||
        c === "fade-out" ||
        /^anim-(in|out)-/.test(c)
      ) {
        el.classList.remove(c);
      }
    }
    el.style.animationDuration = "";
    el.style.removeProperty("--slide-x");
    el.style.removeProperty("--slide-y");
  }

  function hideAll() {
    try {
      if (audio) {
        audio.pause();
        audio.currentTime = 0;
        audio = null;
      }
    } catch {}

    clearVisualMedia();
    if (imgEl) imgEl.style.display = "none";
    if (textEl) textEl.style.display = "none";

    clearAnimClasses(imgEl);
    clearAnimClasses(textEl);
  }

  // -------------------------
  // Render alert
  // -------------------------

  async function showAlert(eventType, variables, variationId, soundVolumeOverride) {
    if (!cfg) return;

    const v = pickVariation(eventType, variationId, variables);
    if (!v) return;

    const basePlacement = effectivePlacement(eventType);
    const tEff = effectiveText(eventType, v);

    // Debug what "base" is and what font we resolved to
    const base = baseVariationFor(eventType);
    dlog("EVENT", eventType);
    dlog("PICKED VAR", { id: v?.id, name: v?.name });
    dlog("BASE VAR", { id: base?.id, name: base?.name });
    dlog("BASE TEXT", base?.text || null);
    dlog("VAR  TEXT", v?.text || null);
    dlog("TEFF TEXT", tEff || null);

    // New accepted play — bump gen so any in-flight stale play stops mutating.
    const gen = alertGen.begin();
    hideAll();

    const anim = effectiveAnimationInOut(eventType, v);
    const stageSize = getStageSize();

    // --- SOUND (effective) ---
    try {
      const sndSrc = effectiveSoundSrc(eventType, v);
      if (sndSrc) {
        audio = new Audio(sndSrc);
        let vol = effectiveSoundVolume(eventType, v);
        if (soundVolumeOverride != null) vol = soundVolumeOverride;
        audio.volume = vol;
        await audio.play();
        if (!alertGen.isCurrent(gen)) return;
      }
    } catch {}

    if (!alertGen.isCurrent(gen)) return;

    // --- VISUAL (image or video; placement from base) ---
    const imgSrc = effectiveImageSrc(eventType, v);
    if (imgSrc) {
      setVisualMedia(imgSrc);
      setElBox(imgEl, basePlacement?.image);
      imgEl.style.display = "flex";
      applyAnimToEl(imgEl, "in", anim, stageSize);
    }

    // --- TEXT (effective, inherits base; PLACEMENT from base) ---
    const msg = applyVars(effectiveMessage(eventType, v), variables);
    textEl.textContent = msg;

    // Ensure font is injected and WAIT for it (fixes OBS fallback)
    ensureTextFontLoaded(tEff);

    // If googleFamily used but fontFamily is empty, wait on googleFamily
    const meta = tEff?.fontMeta || {};
    const src = safeLower(meta.source);

    const waitFam =
      src === "local"
        ? (meta.localFamily || tEff.fontFamily || "")
        : (meta.googleFamily || tEff.fontFamily || "");

    await waitForFont(waitFam, tEff.fontWeight || 400);
    if (!alertGen.isCurrent(gen)) return;

    // Apply style after font is ready
    applyTextStyleFromText(tEff);

    setElBox(textEl, basePlacement?.text);
    // Must be laid out (not display:none) for clientWidth/Height.
    textEl.style.display = "flex";
    // Configured fontSize is the max; shrink to fit the fixed placement box.
    // Double-rAF so OBS/Chromium finishes layout before measuring.
    const maxFs = tEff.fontSize || 54;
    const rafStillCurrent = await new Promise((resolve) => {
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          resolve(alertGen.isCurrent(gen));
        });
      });
    });
    if (!rafStillCurrent) return;
    fitTextToBox(textEl, maxFs);
    applyAnimToEl(textEl, "in", anim, stageSize);

    // duration (effective) — await display, exit animation, then cleanup before resolve
    const durMs = clamp(effectiveDurationSec(eventType, v) * 1000, 800, 30000);
    const outAnimMs = animSpeedToDurationMs(anim.out, anim.animSpeed);

    if (!(await delayIfCurrent(durMs, gen))) return;

    if (imgEl) {
      clearAnimClasses(imgEl);
      applyAnimToEl(imgEl, "out", anim, stageSize);
    }
    if (textEl) {
      clearAnimClasses(textEl);
      applyAnimToEl(textEl, "out", anim, stageSize);
    }

    if (!(await delayIfCurrent(outAnimMs + 30, gen))) return;
    hideAll();
  }

  // -------------------------
  // Serialized alert delivery (FIFO, one worker)
  // -------------------------

  const alertDelivery = window.StreamSyncAlertDelivery.createDelivery({
    playOne(alert) {
      return showAlert(
        alert.eventType,
        alert.variables,
        alert.variationId,
        alert.soundVolumeOverride
      );
    },
  });

  // -------------------------
  // WebSocket feed
  // -------------------------

  function connect() {
    const ws = new WebSocket(wsUrl);

    ws.addEventListener("message", async (ev) => {
      let msg;
      try {
        msg = JSON.parse(ev.data);
      } catch {
        return;
      }

      if (msg.type === "events-overlay-config-updated") {
        try {
          await loadConfig();
        } catch {}
        return;
      }

      if (msg.type !== "event-alert") return;
      if (eventPlatform(msg) !== DOCK_PLATFORM) return;

      if (!cfg) {
        try {
          await loadConfig();
        } catch {
          return;
        }
      }

      const eventType = msg.eventType || "follow";
      const vars = extractVars(msg);
      const variationId = msg.variationId || null;
      const soundVolumeOverride = extractSoundVolume(msg);

      alertDelivery.enqueue({
        eventType,
        variables: vars,
        variationId,
        soundVolumeOverride,
      });
    });

    ws.addEventListener("close", () => setTimeout(connect, 1000));
  }

  loadConfig().finally(connect);
})();
