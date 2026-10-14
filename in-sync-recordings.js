// In-Sync Recordings functional UI (local finalization queue)

function inSyncModeLabel(mode) {
  return mode === "automatic_after_seal" ? "Automatic" : "Manual";
}

function setInSyncModeToggle(mode) {
  var isAutomatic = mode === "automatic_after_seal";
  var btn = document.getElementById("in-sync-processing-mode-toggle");
  if (btn) {
    btn.classList.toggle("on", isAutomatic);
    btn.setAttribute("aria-pressed", isAutomatic ? "true" : "false");
    var labelEl = btn.querySelector(".toggle-label");
    if (labelEl) labelEl.textContent = inSyncModeLabel(mode);
  }
  var manualSide = document.getElementById("in-sync-mode-manual-label");
  var autoSide = document.getElementById("in-sync-mode-automatic-label");
  if (manualSide) manualSide.classList.toggle("is-active", !isAutomatic);
  if (autoSide) autoSide.classList.toggle("is-active", isAutomatic);
}

async function inSyncPrivileged(path, options) {
  if (!window.streamSyncControlApi || !window.streamSyncControlApi.privilegedFetch) {
    throw new Error("control API unavailable");
  }
  var res = await window.streamSyncControlApi.privilegedFetch(path, options || {});
  var data = await res.json().catch(function () {
    return {};
  });
  if (!res.ok) throw new Error(data.error || "HTTP " + res.status);
  return data;
}

function formatPhase(phase) {
  if (!phase) return "—";
  return String(phase)
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/_/g, " ");
}

function applyInSyncEmptyUi() {
  var hudStatus = document.getElementById("in-sync-hud-status");
  var hudPhase = document.getElementById("in-sync-hud-phase");
  var hudSession = document.getElementById("in-sync-hud-session");
  var cardQueue = document.getElementById("in-sync-card-queue");
  var cardProcessing = document.getElementById("in-sync-card-processing");
  var cardMode = document.getElementById("in-sync-card-mode");
  var cardPublished = document.getElementById("in-sync-card-published");
  var empty = document.getElementById("in-sync-empty");
  var fields = document.getElementById("in-sync-session-fields");
  var log = document.getElementById("in-sync-progress-log");
  var processBtn = document.getElementById("in-sync-process-now");
  var revealBtn = document.getElementById("in-sync-reveal");
  var reportBtn = document.getElementById("in-sync-open-report");

  if (hudStatus) hudStatus.textContent = "Nothing processing";
  if (hudPhase) hudPhase.textContent = "—";
  if (hudSession) hudSession.textContent = "—";
  if (cardQueue) cardQueue.textContent = "No recordings ready";
  if (cardProcessing) cardProcessing.textContent = "Nothing processing";
  if (cardMode) cardMode.textContent = "Manual";
  if (cardPublished) cardPublished.textContent = "None yet";
  setInSyncModeToggle("manual");
  if (empty) {
    empty.hidden = false;
    empty.textContent = "No active session";
  }
  if (fields) {
    fields.hidden = true;
    fields.innerHTML = "";
  }
  if (log) log.textContent = "No events yet";
  if (processBtn) processBtn.disabled = true;
  if (revealBtn) revealBtn.disabled = true;
  if (reportBtn) reportBtn.disabled = true;
}

function applyInSyncStatus(status) {
  var mode = status.localProcessingMode || "manual";
  var modeLabel = inSyncModeLabel(mode);
  var queue = status.manualQueue || [];
  var activeId = status.activeSessionId || "";
  var phase = status.phase || "";
  var published = status.publishedSessions || [];
  var events = status.progressEvents || [];

  setInSyncModeToggle(mode === "automatic_after_seal" ? "automatic_after_seal" : "manual");

  var hudStatus = document.getElementById("in-sync-hud-status");
  var hudPhase = document.getElementById("in-sync-hud-phase");
  var hudSession = document.getElementById("in-sync-hud-session");
  var cardQueue = document.getElementById("in-sync-card-queue");
  var cardProcessing = document.getElementById("in-sync-card-processing");
  var cardMode = document.getElementById("in-sync-card-mode");
  var cardPublished = document.getElementById("in-sync-card-published");
  var empty = document.getElementById("in-sync-empty");
  var fields = document.getElementById("in-sync-session-fields");
  var log = document.getElementById("in-sync-progress-log");
  var processBtn = document.getElementById("in-sync-process-now");
  var revealBtn = document.getElementById("in-sync-reveal");
  var reportBtn = document.getElementById("in-sync-open-report");

  if (cardMode) cardMode.textContent = modeLabel;

  if (cardQueue) {
    cardQueue.textContent = queue.length
      ? queue.length + " in queue: " + queue.join(", ")
      : "No recordings ready";
  }

  var processingLabel = "Nothing processing";
  if (activeId) {
    processingLabel = formatPhase(phase) || "Active";
  }
  if (cardProcessing) cardProcessing.textContent = processingLabel;
  if (hudStatus) hudStatus.textContent = activeId ? "Processing" : "Idle";
  if (hudPhase) hudPhase.textContent = activeId ? formatPhase(phase) : "—";
  if (hudSession) {
    hudSession.textContent = activeId
      ? activeId.length > 24
        ? activeId.slice(0, 10) + "…" + activeId.slice(-8)
        : activeId
      : "—";
  }

  if (cardPublished) {
    cardPublished.textContent = published.length
      ? published.length + " session(s)"
      : "None yet";
  }

  if (log) {
    log.textContent = events.length
      ? JSON.stringify(events, null, 2)
      : "No events yet";
  }

  if (!activeId) {
    if (empty) {
      empty.hidden = false;
      empty.textContent = "No active session";
    }
    if (fields) {
      fields.hidden = true;
      fields.innerHTML = "";
    }
  } else {
    if (empty) empty.hidden = true;
    if (fields) {
      fields.hidden = false;
      fields.innerHTML =
        "<dt>Session ID</dt><dd>" +
        activeId +
        "</dd><dt>Phase</dt><dd>" +
        formatPhase(phase) +
        "</dd><dt>Manual queue</dt><dd>" +
        (queue.length ? queue.join(", ") : "—") +
        "</dd><dt>Output</dt><dd>" +
        (status.publishedOutputDir || "—") +
        "</dd>";
    }
  }

  var canOpen = Boolean(activeId && status.publishedOutputDir);
  if (processBtn) processBtn.disabled = !activeId;
  if (revealBtn) revealBtn.disabled = !canOpen;
  if (reportBtn) {
    reportBtn.disabled = !canOpen || !status.recoveryReportPath;
  }

  return { activeId: activeId, status: status, canOpen: canOpen };
}

async function refreshInSyncRecordings() {
  try {
    var status = await inSyncPrivileged("/api/in-sync/status");
    var ctx = applyInSyncStatus(status);

    var processBtn = document.getElementById("in-sync-process-now");
    if (processBtn) {
      processBtn.onclick = async function () {
        await inSyncPrivileged("/api/in-sync/process-now", {
          method: "POST",
          body: JSON.stringify({ sessionId: ctx.activeId }),
        });
        await refreshInSyncRecordings();
      };
    }

    var revealBtn = document.getElementById("in-sync-reveal");
    var reportBtn = document.getElementById("in-sync-open-report");
    var sid = ctx.activeId;
    if (revealBtn) {
      revealBtn.onclick = async function () {
        if (window.__TAURI__ && window.__TAURI__.core) {
          await window.__TAURI__.core.invoke("reveal_in_sync_output", {
            sessionId: sid,
          });
        }
      };
    }
    if (reportBtn) {
      reportBtn.onclick = async function () {
        if (window.__TAURI__ && window.__TAURI__.core) {
          await window.__TAURI__.core.invoke("open_in_sync_recovery_report", {
            sessionId: sid,
          });
        }
      };
    }
  } catch (err) {
    console.error("In-Sync status refresh failed:", err);
    applyInSyncEmptyUi();
  }
}

function wireInSyncRecordingsUi() {
  var modeToggle = document.getElementById("in-sync-processing-mode-toggle");
  if (modeToggle && !modeToggle.dataset.bound) {
    modeToggle.dataset.bound = "1";
    modeToggle.addEventListener("click", async function () {
      var previousMode =
        modeToggle.getAttribute("aria-pressed") === "true"
          ? "automatic_after_seal"
          : "manual";
      var nextMode =
        previousMode === "automatic_after_seal" ? "manual" : "automatic_after_seal";

      modeToggle.disabled = true;
      try {
        await inSyncPrivileged("/api/in-sync/processing-mode", {
          method: "POST",
          body: JSON.stringify({ mode: nextMode }),
        });
        setInSyncModeToggle(nextMode);
        var cardMode = document.getElementById("in-sync-card-mode");
        if (cardMode) cardMode.textContent = inSyncModeLabel(nextMode);
      } catch (e) {
        console.error(e);
        setInSyncModeToggle(previousMode);
      } finally {
        modeToggle.disabled = false;
      }
    });
  }

  var refreshBtn = document.getElementById("in-sync-refresh");
  if (refreshBtn) {
    refreshBtn.addEventListener("click", function () {
      refreshInSyncRecordings().catch(console.error);
    });
  }

  document.getElementById("in-sync-pause")?.addEventListener("click", async function () {
    await inSyncPrivileged("/api/in-sync/pause", {
      method: "POST",
      body: JSON.stringify({ paused: true }),
    });
    await refreshInSyncRecordings();
  });
  document.getElementById("in-sync-resume")?.addEventListener("click", async function () {
    await inSyncPrivileged("/api/in-sync/pause", {
      method: "POST",
      body: JSON.stringify({ paused: false }),
    });
    await refreshInSyncRecordings();
  });
  document.getElementById("in-sync-retry")?.addEventListener("click", function () {
    refreshInSyncRecordings().catch(console.error);
  });

  applyInSyncEmptyUi();
  refreshInSyncRecordings().catch(console.error);
}
