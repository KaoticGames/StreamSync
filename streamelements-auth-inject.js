// Injected into SE import webview. Scrapes Account → Channels secrets, then redirects
// to Stream Sync localhost callback (no Tauri invoke required on streamelements.com).
(function () {
  if (!/streamelements\.com/i.test(location.hostname || "")) return;

  const PORT = Number(window.__STREAMSYNC_OVERLAY_PORT__) || 4040;
  const FLOW = String(window.__STREAMSYNC_SE_FLOW__ || "");
  const CHANNELS_URL = "https://streamelements.com/dashboard/account/channels";
  const POLL_MS = 1200;
  const MAX_ATTEMPTS = 300;
  let attempts = 0;
  let done = false;
  let secretsClicked = false;

  function looksLikeJwt(s) {
    if (!s || typeof s !== "string") return false;
    const t = s.trim();
    if (t.length < 80) return false;
    const parts = t.split(".");
    return parts.length >= 2 && parts.every((p) => p.length > 0);
  }

  function looksLikeAccountId(s) {
    return /^[a-f0-9]{20,32}$/i.test((s || "").trim());
  }

  function showBanner(text) {
    let el = document.getElementById("streamsync-se-auth-banner");
    if (!el) {
      el = document.createElement("div");
      el.id = "streamsync-se-auth-banner";
      el.style.cssText =
        "position:fixed;bottom:16px;right:16px;z-index:2147483647;max-width:360px;" +
        "padding:12px 14px;background:#1a1a1a;color:#eee;border:1px solid #444;" +
        "border-radius:8px;font:14px/1.4 system-ui,sans-serif;box-shadow:0 8px 24px rgba(0,0,0,.4)";
      document.body.appendChild(el);
    }
    el.textContent = text;
  }

  function labelForInput(inp) {
    const id = inp.id;
    if (id) {
      const lbl = document.querySelector('label[for="' + CSS.escape(id) + '"]');
      if (lbl) return (lbl.textContent || "").trim();
    }
    const wrap = inp.closest("div, section, label");
    return (wrap && wrap.textContent ? wrap.textContent.slice(0, 120) : "") || "";
  }

  function clickShowSecrets() {
    const nodes = document.querySelectorAll(
      'button, [role="switch"], [role="button"], label, span, div, a, input[type="checkbox"]'
    );
    for (const el of nodes) {
      const t = (
        el.textContent ||
        el.getAttribute("aria-label") ||
        el.getAttribute("title") ||
        ""
      ).trim();
      if (/show\s*secrets/i.test(t)) {
        el.click();
        return true;
      }
    }
    return false;
  }

  function scrapeChannelsPage() {
    let accountId = null;
    let jwt = null;

    for (const inp of document.querySelectorAll("input, textarea")) {
      const v = (inp.value || "").trim();
      if (!v) continue;
      const ctx = (
        labelForInput(inp) +
        " " +
        (inp.name || "") +
        " " +
        (inp.id || "") +
        " " +
        (inp.placeholder || "")
      ).toLowerCase();

      if (/account\s*id|channel\s*id/.test(ctx) && looksLikeAccountId(v)) {
        accountId = v;
      }
      if (/jwt|token|secret/.test(ctx) && looksLikeJwt(v)) {
        jwt = v;
      }
      if (!accountId && looksLikeAccountId(v)) accountId = v;
      if (!jwt && looksLikeJwt(v)) jwt = v;
    }

    // Copy-to-clipboard fields / readonly text blocks
    if (!accountId || !jwt) {
      const blocks = document.querySelectorAll(
        "[data-clipboard-text], code, pre, .token, [readonly]"
      );
      for (const el of blocks) {
        const v = (
          el.getAttribute("data-clipboard-text") ||
          el.textContent ||
          ""
        ).trim();
        if (!accountId && looksLikeAccountId(v)) accountId = v;
        if (!jwt && looksLikeJwt(v)) jwt = v;
      }
    }

    return { accountId, jwt };
  }

  function finishRedirect(jwt, accountId) {
    if (done) return;
    done = true;
    const hash =
      "jwt=" +
      encodeURIComponent(jwt) +
      "&accountId=" +
      encodeURIComponent(accountId);
    showBanner("Stream Sync: connected — finishing…");
    if (!FLOW.startsWith("ssl_")) {
      showBanner("Stream Sync: login flow expired. Start Connect again.");
      done = false;
      return;
    }
    location.href =
      "http://127.0.0.1:" +
      PORT +
      "/auth/streamelements/callback?flow=" +
      encodeURIComponent(FLOW) +
      "#" +
      hash;
  }

  function isLoggedInDashboard() {
    const path = location.pathname || "";
    if (!/dashboard/i.test(path)) return false;
    if (/\/login|\/signin|\/oauth/i.test(path)) return false;
    const body = (document.body && document.body.innerText) || "";
    if (/log\s*in|sign\s*in/i.test(body.slice(0, 500)) && !/log\s*out/i.test(body)) {
      return false;
    }
    return true;
  }

  function isChannelsPage() {
    return /\/account\/channels/i.test(location.pathname || "");
  }

  async function tick() {
    if (done) return;
    attempts += 1;
    if (attempts > MAX_ATTEMPTS) {
      showBanner(
        "Stream Sync: open Account → Channels, enable Show secrets, or paste JWT in the Stream Sync window."
      );
      return;
    }

    if (!isLoggedInDashboard()) {
      showBanner("Stream Sync: log in with Twitch (or your provider), then wait…");
      setTimeout(tick, POLL_MS);
      return;
    }

    if (!isChannelsPage()) {
      showBanner("Stream Sync: opening Account → Channels to read API credentials…");
      if (attempts > 2) {
        location.href = CHANNELS_URL;
        return;
      }
      setTimeout(tick, POLL_MS);
      return;
    }

    if (!secretsClicked) {
      secretsClicked = clickShowSecrets();
      if (secretsClicked) {
        showBanner("Stream Sync: enabling Show secrets…");
        setTimeout(tick, 800);
        return;
      }
    }

    const { accountId, jwt } = scrapeChannelsPage();
    if (accountId && jwt) {
      console.log("[se-auth] scraped channels page");
      finishRedirect(jwt, accountId);
      return;
    }

    if (!secretsClicked) clickShowSecrets();
    showBanner(
      "Stream Sync: on Account → Channels — turn on Show secrets if prompted, then wait…"
    );
    setTimeout(tick, POLL_MS);
  }

  console.log("[se-auth] Stream Sync helper (channels scrape) loaded");
  showBanner("Stream Sync: log in, then we will read credentials from Account → Channels.");
  setTimeout(tick, 1000);
})();
