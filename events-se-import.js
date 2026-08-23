// StreamElements overlay import (Events tab) — requires Connections → StreamElements.
(function () {
  const MODAL_ID = "se-import-modal-root";
  let delegationBound = false;

  // Drop modal instances created before direct Import wiring (had stopPropagation on panel).
  const staleModal = document.getElementById(MODAL_ID);
  if (staleModal) staleModal.remove();

  async function api(path, options) {
    const opts = {
      cache: "no-store",
      ...options,
      headers: { Accept: "application/json", ...((options && options.headers) || {}) },
    };
    const res = await window.streamSyncControlApi.privilegedFetch(path, opts);
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const err = new Error(data.error || `HTTP ${res.status}`);
      err.status = res.status;
      throw err;
    }
    if (data && data.ok === false && data.error) {
      const err = new Error(String(data.error));
      err.status = res.status;
      throw err;
    }
    return data;
  }

  function ensureModal() {
    let root = document.getElementById(MODAL_ID);
    if (root) {
      wireModalActions(root);
      return root;
    }

    root = document.createElement("div");
    root.id = MODAL_ID;
    root.className = "se-import-modal-backdrop";
    root.setAttribute("aria-hidden", "true");
    root.innerHTML = `
      <div class="se-import-modal" role="dialog" aria-labelledby="se-import-modal-title">
        <div class="se-import-modal__header">
          <h3 class="se-import-modal__title" id="se-import-modal-title">Import StreamElements Overlays</h3>
          <button type="button" class="btn btn-secondary btn-sm" data-se-import-close aria-label="Close">✕</button>
        </div>
        <div class="se-import-modal__body">
          <p class="se-import-status" data-se-import-status>Loading…</p>
          <ul class="se-import-list" data-se-import-list></ul>
          <div class="se-import-results" data-se-import-results hidden></div>
        </div>
        <div class="se-import-modal__footer">
          <div class="se-import-progress" data-se-import-progress hidden>
            <div class="se-import-progress__label" data-se-import-progress-label>Downloading assets…</div>
            <div class="se-import-progress__track" aria-hidden="true">
              <div class="se-import-progress__bar" data-se-import-progress-bar style="width:0%"></div>
            </div>
          </div>
          <div class="se-import-modal__footer-actions">
            <button type="button" class="btn btn-secondary" data-se-import-cancel>Cancel</button>
            <button type="button" class="btn btn-primary" data-se-import-run>Import</button>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(root);
    wireModalActions(root);
    return root;
  }

  function wireModalActions(root) {
    if (!root) return;

    if (root.dataset.seBackdropWired !== "1") {
      root.dataset.seBackdropWired = "1";
      root.addEventListener("click", (e) => {
        if (e.target === root) closeModal();
      });
    }

    const closeBtn = root.querySelector("[data-se-import-close]");
    const cancelBtn = root.querySelector("[data-se-import-cancel]");
    const runBtn = root.querySelector("[data-se-import-run]");

    if (closeBtn && closeBtn.dataset.seWired !== "1") {
      closeBtn.dataset.seWired = "1";
      closeBtn.addEventListener("click", closeModal);
    }
    if (cancelBtn && cancelBtn.dataset.seWired !== "1") {
      cancelBtn.dataset.seWired = "1";
      cancelBtn.addEventListener("click", closeModal);
    }
    if (runBtn && runBtn.dataset.seWired !== "1") {
      runBtn.dataset.seWired = "1";
      runBtn.addEventListener("click", (e) => {
        e.preventDefault();
        onImport();
      });
    }

    if (root.dataset.seResultsWired !== "1") {
      root.dataset.seResultsWired = "1";
      root.addEventListener("click", (e) => {
        const openBtn = e.target.closest("[data-se-open-profile]");
        if (!openBtn) return;
        e.preventDefault();
        openImportedProfile(openBtn.getAttribute("data-se-open-profile"));
      });
    }
  }

  function closeModal() {
    const root = document.getElementById(MODAL_ID);
    if (!root) return;
    root.classList.remove("is-open");
    root.setAttribute("aria-hidden", "true");
  }

  function openModal() {
    const root = ensureModal();
    wireModalActions(root);
    root.classList.add("is-open");
    root.setAttribute("aria-hidden", "false");
  }

  function setStatus(text) {
    const el = document.querySelector(`#${MODAL_ID} [data-se-import-status]`);
    if (el) el.textContent = text;
  }

  let importProgressTimer = null;

  function setImportProgressVisible(visible) {
    const wrap = document.querySelector(`#${MODAL_ID} [data-se-import-progress]`);
    if (!wrap) return;
    wrap.hidden = !visible;
    wrap.classList.toggle("is-active", !!visible);
  }

  function setImportProgressLabel(text) {
    const el = document.querySelector(`#${MODAL_ID} [data-se-import-progress-label]`);
    if (el) el.textContent = text;
  }

  function setImportProgressPct(pct) {
    const bar = document.querySelector(`#${MODAL_ID} [data-se-import-progress-bar]`);
    if (!bar) return;
    const clamped = Math.max(0, Math.min(100, pct));
    bar.style.width = `${clamped}%`;
  }

  function stopImportProgressTimer() {
    if (importProgressTimer != null) {
      clearInterval(importProgressTimer);
      importProgressTimer = null;
    }
  }

  /** Ease toward ~92% while the import request runs; jump to 100% when done. */
  function startImportProgress() {
    stopImportProgressTimer();
    setImportProgressVisible(true);
    setImportProgressLabel("Downloading assets…");
    setImportProgressPct(4);
    let pct = 4;
    importProgressTimer = setInterval(() => {
      const remaining = 92 - pct;
      if (remaining <= 0.35) return;
      pct += Math.max(0.35, remaining * 0.07);
      setImportProgressPct(pct);
    }, 180);
  }

  function finishImportProgress(doneLabel) {
    stopImportProgressTimer();
    setImportProgressPct(100);
    setImportProgressLabel(doneLabel || "Done");
    setTimeout(() => {
      setImportProgressVisible(false);
      setImportProgressPct(0);
    }, 900);
  }

  function setImportReady(ready) {
    const btn = document.querySelector(`#${MODAL_ID} [data-se-import-run]`);
    if (!btn) return;
    btn.classList.toggle("is-ready", !!ready);
    btn.setAttribute("aria-disabled", ready ? "false" : "true");
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function renderOverlays(overlays) {
    const list = document.querySelector(`#${MODAL_ID} [data-se-import-list]`);
    const results = document.querySelector(`#${MODAL_ID} [data-se-import-results]`);
    if (!list) return;
    if (results) {
      results.hidden = true;
      results.innerHTML = "";
    }
    list.innerHTML = "";
    if (!overlays || !overlays.length) {
      setStatus("No overlays found on your StreamElements account.");
      setImportReady(false);
      return;
    }
    setStatus("Select overlays to import as Stream Sync Events profiles.");
    setImportReady(true);
    overlays.forEach((o, index) => {
      const li = document.createElement("li");
      const id = o.id || o._id;
      const name = o.name || "Untitled";
      const updated = o.updatedAt || o.updated_at || "";
      const safeId = `se-ov-${index}-${String(id).replace(/[^a-zA-Z0-9_-]/g, "_")}`;
      li.innerHTML = `
        <input type="checkbox" data-se-overlay-id="${escapeHtml(id)}" id="${safeId}" />
        <label for="${safeId}">
          <strong>${escapeHtml(name)}</strong>
          ${updated ? `<div style="font-size:12px;color:var(--text-soft)">Updated ${escapeHtml(updated)}</div>` : ""}
        </label>
      `;
      list.appendChild(li);
    });
  }

  function openImportedProfile(profileId) {
    if (!profileId) return;
    if (window.streamSyncOpenEventsProfile) {
      window.streamSyncOpenEventsProfile(profileId);
      return;
    }
    const overlayBtn = document.querySelector(
      '.subnav-events .subnav-btn[data-subview="events-overlay"]'
    );
    if (overlayBtn) overlayBtn.click();
  }

  function renderImportResults(data) {
    const results = document.querySelector(`#${MODAL_ID} [data-se-import-results]`);
    if (!results) return;
    const rows = data.results || [];
    if (!rows.length) {
      results.hidden = true;
      return;
    }
    results.hidden = false;
    results.innerHTML = rows
      .map((r) => {
        if (r.ok === false) {
          return `<div class="se-import-result se-import-result--error" style="margin-bottom:10px">
            <strong>Failed</strong> (${escapeHtml(r.overlayId || "overlay")})
            <div class="warn">${escapeHtml(r.error || "Unknown error")}</div>
          </div>`;
        }
        const warns = (r.warnings || [])
          .map((w) => `<div class="warn">⚠ ${escapeHtml(w)}</div>`)
          .join("");
        return `<div class="se-import-result" style="margin-bottom:10px">
          <strong>${escapeHtml(r.profileName || r.profileId)}</strong>
          <div class="se-import-result__actions">
            <button type="button" class="btn btn-secondary btn-sm" data-se-open-profile="${escapeHtml(r.profileId)}">
              Open in Events tab
            </button>
            <span class="se-import-result__id">${escapeHtml(r.profileId)}</span>
          </div>
          ${warns}
        </div>`;
      })
      .join("");
    results.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }

  async function refreshImportButton() {
    const btn = document.getElementById("btn-import-se-overlays");
    if (!btn) return;
    try {
      const sess = await api("/api/streamelements/session");
      btn.classList.toggle("is-connected", !!sess.connected);
    } catch (_) {
      btn.classList.remove("is-connected");
    }
  }

  async function loadOverlayList() {
    openModal();
    setStatus("Loading overlays…");
    setImportReady(false);
    const list = document.querySelector(`#${MODAL_ID} [data-se-import-list]`);
    if (list) list.innerHTML = "";
    try {
      const data = await api("/api/streamelements/overlays");
      renderOverlays(data.overlays || []);
      await refreshImportButton();
    } catch (e) {
      if (e.status === 401 || String(e.message).includes("not_connected")) {
        setStatus(
          "StreamElements is not connected. Open Connections, save Account ID and JWT, then try again."
        );
      } else {
        setStatus(`Failed to load overlays: ${e.message}`);
      }
      setImportReady(false);
    }
  }

  async function onImport() {
    const runBtnEl = document.querySelector(`#${MODAL_ID} [data-se-import-run]`);
    if (runBtnEl?.classList.contains("is-busy")) return;

    const ids = Array.from(
      document.querySelectorAll(`#${MODAL_ID} [data-se-overlay-id]:checked`)
    )
      .map((el) => el.getAttribute("data-se-overlay-id"))
      .filter(Boolean);
    if (!ids.length) {
      setStatus("Select at least one overlay.");
      return;
    }
    setStatus(`Importing ${ids.length} overlay${ids.length === 1 ? "" : "s"}…`);
    startImportProgress();
    const runBtn = document.querySelector(`#${MODAL_ID} [data-se-import-run]`);
    if (runBtn) {
      runBtn.classList.add("is-busy");
      runBtn.setAttribute("aria-disabled", "true");
    }
    const cancelBtn = document.querySelector(`#${MODAL_ID} [data-se-import-cancel]`);
    if (cancelBtn) cancelBtn.setAttribute("aria-disabled", "true");
    try {
      const data = await api("/api/streamelements/import", {
        method: "POST",
        body: JSON.stringify({ overlayIds: ids }),
      });
      finishImportProgress("Assets downloaded");
      renderImportResults(data);
      const rows = data.results || [];
      const okCount = rows.filter((r) => r.ok !== false && r.profileId).length;
      const failCount = rows.filter((r) => r.ok === false).length;
      if (okCount && failCount) {
        setStatus(`Imported ${okCount}; ${failCount} failed (see below).`);
      } else if (okCount) {
        setStatus(`Import complete — ${okCount} profile${okCount === 1 ? "" : "s"} added.`);
      } else if (failCount) {
        setStatus(`Import failed for ${failCount} overlay${failCount === 1 ? "" : "s"} (see below).`);
      } else {
        setStatus("Import finished (no profiles created).");
      }
      const importedIds = rows
        .filter((r) => r.ok !== false && r.profileId)
        .map((r) => r.profileId);

      try {
        if (importedIds.length && window.streamSyncRegisterProfiles) {
          window.streamSyncRegisterProfiles(importedIds);
        }
        if (window.streamSyncRefreshEventsProfiles) {
          await window.streamSyncRefreshEventsProfiles();
        } else if (window.refreshEventsIntegrationsProfiles) {
          await window.refreshEventsIntegrationsProfiles();
        } else if (window.initEventsIntegrationsConfig) {
          window.initEventsIntegrationsConfig();
        }
      } catch (_) {}

      const firstImported = rows.find((r) => r.ok !== false && r.profileId);
      if (firstImported?.profileId) {
        try {
          window.streamSyncSelectEventsProfile?.(firstImported.profileId);
        } catch (_) {}
        setTimeout(() => openImportedProfile(firstImported.profileId), 300);
      }
      setImportReady(true);
    } catch (e) {
      stopImportProgressTimer();
      setImportProgressVisible(false);
      setImportProgressPct(0);
      setStatus(`Import failed: ${e.message}`);
      setImportReady(true);
    } finally {
      const busyBtn = document.querySelector(`#${MODAL_ID} [data-se-import-run]`);
      if (busyBtn) {
        busyBtn.classList.remove("is-busy");
        busyBtn.setAttribute("aria-disabled", "false");
      }
      const cancelBtnDone = document.querySelector(`#${MODAL_ID} [data-se-import-cancel]`);
      if (cancelBtnDone) cancelBtnDone.setAttribute("aria-disabled", "false");
    }
  }

  function goToConnectionsTab() {
    const nav = document.querySelector('.nav-btn[data-view="connections"]');
    if (nav) nav.click();
  }

  async function beginStreamElementsLogin() {
    const flow = await api("/api/streamelements/begin-login");
    const nonce = String(flow.flowNonce || "");
    if (!nonce.startsWith("ssl_")) throw new Error("Invalid StreamElements login flow");
    if (window.electronAPI?.openSeAccountPage) {
      await window.electronAPI.openSeAccountPage(nonce);
      return;
    }
    throw new Error("StreamElements login requires the Stream Sync desktop app");
  }

  async function startImportFlow() {
    openModal();
    setStatus("Checking StreamElements connection…");
    try {
      const sess = await api("/api/streamelements/session");
      const connected = !!(sess && (sess.connected || sess.accountId));
      if (!connected) {
        setStatus("Opening StreamElements login…");
        await beginStreamElementsLogin();
        setStatus("Finish signing in to StreamElements. This window will refresh after connection.");
        return;
      }
      await loadOverlayList();
    } catch (e) {
      setStatus(`Error: ${e.message}`);
    }
  }

  function bindImportDelegation() {
    if (delegationBound) return;
    delegationBound = true;
    document.addEventListener("click", (e) => {
      const btn = e.target.closest("#btn-import-se-overlays");
      if (!btn) return;
      e.preventDefault();
      e.stopPropagation();
      startImportFlow();
    });
  }

  window.initEventsSeImport = function initEventsSeImport() {
    bindImportDelegation();
    refreshImportButton();
    const existing = document.getElementById(MODAL_ID);
    if (existing) wireModalActions(existing);
  };

  window.refreshSeImportButton = refreshImportButton;
  bindImportDelegation();
})();
