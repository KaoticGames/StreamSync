// In-Sync Recordings functional UI (local finalization queue)

var inSyncInitState = "idle";
var inSyncLastRefreshAt = null;
var IN_SYNC_POLL_INTERVAL_MS = 2000;
var inSyncLiveRefreshTimerId = null;
var inSyncPollFetchInFlight = false;
var inSyncPollFetchWait = null;
var inSyncActiveRefresh = null;
var inSyncStatusFetchInFlight = false;
var inSyncUiWired = false;

function stopInSyncLiveRefresh() {
  if (inSyncLiveRefreshTimerId != null) {
    clearInterval(inSyncLiveRefreshTimerId);
    inSyncLiveRefreshTimerId = null;
  }
}

function ensureInSyncLiveRefresh() {
  if (inSyncLiveRefreshTimerId != null) return;
  inSyncLiveRefreshTimerId = setInterval(function () {
    return pollInSyncStatus().catch(console.error);
  }, IN_SYNC_POLL_INTERVAL_MS);
}

async function pollInSyncStatus() {
  if (inSyncPollFetchInFlight) return inSyncPollFetchWait;
  inSyncPollFetchInFlight = true;
  inSyncPollFetchWait = refreshInSyncRecordings({ skipIfBusy: true }).finally(
    function () {
      inSyncPollFetchInFlight = false;
      inSyncPollFetchWait = null;
    }
  );
  return inSyncPollFetchWait;
}

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

function showInSyncPanelError(message) {
  var el = document.getElementById("in-sync-init-error");
  if (!el) return;
  el.hidden = false;
  el.textContent = message || "In-Sync initialization failed";
}

function clearInSyncPanelError() {
  var el = document.getElementById("in-sync-init-error");
  if (!el) return;
  el.hidden = true;
  el.textContent = "";
}

function setInSyncBusy(busy, label) {
  var refreshBtn = document.getElementById("in-sync-refresh");
  if (refreshBtn) {
    refreshBtn.disabled = !!busy;
    if (busy && label) refreshBtn.dataset.busyLabel = label;
  }
}

async function fetchInSyncStatusPayload() {
  if (!window.streamSyncControlApi || !window.streamSyncControlApi.privilegedFetch) {
    throw new Error("control API unavailable");
  }
  inSyncStatusFetchInFlight = true;
  var res;
  try {
    res = await window.streamSyncControlApi.privilegedFetch(
      "/api/in-sync/status",
      {}
    );
  } finally {
    inSyncStatusFetchInFlight = false;
  }
  var data = await res.json().catch(function () {
    return {};
  });
  if (!res.ok) throw new Error(data.error || "HTTP " + res.status);
  return data;
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

function formatSupervisorStatus(status) {
  if (!status || typeof status !== "string") return "starting";
  return status;
}

function formatTimestampMs(ms) {
  if (ms == null || !isFinite(ms)) return "—";
  try {
    return new Date(ms).toLocaleString();
  } catch (e) {
    return String(ms);
  }
}

function shortSessionId(id) {
  if (!id) return "—";
  return id.length > 24 ? id.slice(0, 10) + "…" + id.slice(-8) : id;
}

function formatDuration(secs) {
  if (secs == null || !isFinite(secs)) return "—";
  var m = Math.floor(secs / 60);
  var s = Math.floor(secs % 60);
  return m > 0 ? m + "m " + s + "s" : s + "s";
}

function formatGiB(bytes) {
  if (bytes == null || !isFinite(bytes)) return "—";
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + " GiB";
}

function formatByteProgress(complete, total) {
  var c = complete != null && isFinite(complete) ? complete : 0;
  if (total == null || !isFinite(total)) return c + " bytes";
  return c + " / " + total + " bytes";
}

function formatInSyncProgressEventLine(event) {
  if (!event || !event.type) return "";
  switch (event.type) {
    case "journal_ready":
      return "Journal ready";
    case "download_progress":
      return (
        "Download segment " +
        (event.segmentSeq != null ? event.segmentSeq : "—") +
        ": " +
        formatByteProgress(event.bytesComplete, event.bytesTotal)
      );
    case "segment_verified":
      return (
        "Segment " +
        (event.segmentSeq != null ? event.segmentSeq : "—") +
        " verified"
      );
    case "processing_started":
      return "Processing started";
    case "timeline_progress":
      return (
        "Timeline " +
        (event.processedSegments != null ? event.processedSegments : "—") +
        " / " +
        (event.totalSegments != null ? event.totalSegments : "—") +
        " segments"
      );
    case "publication_complete":
      return (
        "Published: " +
        (event.finalSessionDir || "—") +
        (event.recoveryReportPath
          ? " (report: " + event.recoveryReportPath + ")"
          : "")
      );
    case "receipt_acknowledged":
      return "Receipt acknowledged";
    default:
      return formatPhase(event.type);
  }
}

function buildInSyncProgressLogText(status) {
  var events = (status && status.progressEvents) || [];
  var phase = status && status.phase ? String(status.phase) : "";
  var lines = [];

  if (phase === "paused") {
    lines.push("Phase: Paused");
  } else if (phase === "failed_retryable") {
    lines.push("Phase: Failed (retryable — use Retry)");
  } else if (phase === "failed_blocked") {
    lines.push("Phase: Blocked");
  } else if (phase) {
    lines.push("Phase: " + formatPhase(phase));
  }

  for (var i = 0; i < events.length; i++) {
    var line = formatInSyncProgressEventLine(events[i]);
    if (line) lines.push(line);
  }

  if (!lines.length) {
    return { primary: "No events yet", diagnostics: "" };
  }

  var diagnostics = events.length
    ? "--- Diagnostics ---\n" + JSON.stringify(events, null, 2)
    : "";
  return { primary: lines.join("\n"), diagnostics: diagnostics };
}

function renderInSyncProgressLog(status) {
  var log = document.getElementById("in-sync-progress-log");
  if (!log) return;
  var rendered = buildInSyncProgressLogText(status);
  log.textContent = rendered.diagnostics
    ? rendered.primary + "\n\n" + rendered.diagnostics
    : rendered.primary;
}

function renderInSyncStorageBlocked(status) {
  var el = document.getElementById("in-sync-storage-blocked");
  if (!el) return;
  var failure = status.storageBlocked;
  var sid = status.storageBlockedSessionId || "";
  if (!failure || !sid) {
    el.hidden = true;
    el.textContent = "";
    return;
  }
  el.hidden = false;
  var parts = [
    "Storage blocked for " + shortSessionId(sid) + ":",
  ];
  if (failure.kind === "insufficient_space") {
    parts.push(
      "need " +
        formatGiB(failure.required_bytes) +
        ", available " +
        formatGiB(failure.available_bytes)
    );
  } else if (failure.kind === "destination_unsupported") {
    parts.push(failure.message || "destination unsupported");
  } else if (failure.kind === "destination_inaccessible") {
    parts.push(failure.message || "destination inaccessible");
  } else if (failure.kind === "free_space_query_failed") {
    parts.push(failure.message || "could not query free space");
  } else if (failure.kind === "required_bytes_overflow") {
    parts.push("session size estimate overflow");
  } else {
    parts.push(JSON.stringify(failure));
  }
  parts.push("Use Process now after correcting the recording folder or freeing space.");
  el.textContent = parts.join(" ");
}

function renderInSyncPendingList(pending, primarySessionId) {
  var listEl = document.getElementById("in-sync-pending-list");
  if (!listEl) return;
  if (!pending.length) {
    listEl.hidden = true;
    listEl.innerHTML = "";
    return;
  }
  listEl.hidden = false;
  listEl.innerHTML = pending
    .map(function (item) {
      var sid = item.sessionId || "";
      var isPrimary = sid && sid === primarySessionId;
      return (
        '<div class="in-sync-pending-card' +
        (isPrimary ? " is-primary" : "") +
        '">' +
        '<div class="in-sync-pending-card-title">Ready: ' +
        shortSessionId(sid) +
        (isPrimary ? " (next)" : "") +
        "</div>" +
        '<div class="in-sync-pending-card-meta">' +
        "Sealed " +
        formatTimestampMs(item.sealedAtMs) +
        "<br>Digest " +
        (item.manifestDigest
          ? item.manifestDigest.slice(0, 12) + "…"
          : "—") +
        (item.segmentCount != null
          ? "<br>Segments " + item.segmentCount + " · ~" + formatDuration(item.targetDurationSecs)
          : "") +
        "</div></div>"
      );
    })
    .join("");
}

function renderInSyncDiscovery(discovery, supervisor) {
  var el = document.getElementById("in-sync-discovery-status");
  if (!el) return;
  var status = discovery && discovery.status ? discovery.status : "";
  var msg = discovery && discovery.message ? discovery.message : "";
  var sup = formatSupervisorStatus(supervisor && supervisor.status);
  if (sup === "blocked" || sup === "error") {
    el.hidden = false;
    var supErr = supervisor && supervisor.lastError ? supervisor.lastError : "";
    el.textContent = sup === "blocked"
      ? "Supervisor blocked" + (supErr ? ": " + supErr : "")
      : "Supervisor error" + (supErr ? ": " + supErr : "");
    return;
  }
  if (status === "ok" || (!status && !msg)) {
    el.hidden = true;
    el.textContent = "";
    return;
  }
  el.hidden = false;
  var label = status === "blocked" ? "Discovery blocked" : "Discovery error";
  el.textContent = msg ? label + ": " + msg : label;
}

function renderInSyncSupervisorMeta(status) {
  var el = document.getElementById("in-sync-supervisor-meta");
  if (!el) return;
  var sup = formatSupervisorStatus(status.supervisorStatus);
  var parts = ["Supervisor: " + sup];
  parts.push("Pending: " + (status.supervisorPendingCount != null ? status.supervisorPendingCount : "—"));
  parts.push("Last poll: " + formatTimestampMs(status.supervisorLastAttemptAtMs));
  if (status.supervisorLastError) {
    parts.push("Error: " + status.supervisorLastError);
  }
  el.textContent = parts.join(" · ");
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

  if (hudStatus) hudStatus.textContent = "Idle";
  if (hudPhase) hudPhase.textContent = "—";
  if (hudSession) hudSession.textContent = "—";
  if (cardQueue) cardQueue.textContent = "No recordings ready";
  renderInSyncPendingList([], "");
  renderInSyncDiscovery(null, null);
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
  var pending = status.pendingSessions || [];
  var primaryPendingId =
    pending.length && pending[0].sessionId ? pending[0].sessionId : "";
  var activeId = status.activeSessionId || "";
  var phase = status.phase || "";
  var published = status.publishedSessions || [];

  setInSyncModeToggle(mode === "automatic_after_seal" ? "automatic_after_seal" : "manual");
  renderInSyncSupervisorMeta(status);

  var hudStatus = document.getElementById("in-sync-hud-status");
  var hudPhase = document.getElementById("in-sync-hud-phase");
  var hudSession = document.getElementById("in-sync-hud-session");
  var cardQueue = document.getElementById("in-sync-card-queue");
  var cardProcessing = document.getElementById("in-sync-card-processing");
  var cardMode = document.getElementById("in-sync-card-mode");
  var cardPublished = document.getElementById("in-sync-card-published");
  var empty = document.getElementById("in-sync-empty");
  var fields = document.getElementById("in-sync-session-fields");
  var processBtn = document.getElementById("in-sync-process-now");
  var revealBtn = document.getElementById("in-sync-reveal");
  var reportBtn = document.getElementById("in-sync-open-report");

  if (cardMode) cardMode.textContent = modeLabel;

  renderInSyncPendingList(pending, primaryPendingId);
  renderInSyncDiscovery(status.discovery, {
    status: status.supervisorStatus,
    lastError: status.supervisorLastError,
  });
  renderInSyncStorageBlocked(status);

  if (cardQueue) {
    if (pending.length) {
      cardQueue.textContent =
        pending.length +
        " ready in cloud" +
        (queue.length ? " · " + queue.length + " queued locally" : "");
    } else if (queue.length) {
      cardQueue.textContent = queue.length + " in local queue: " + queue.join(", ");
    } else {
      cardQueue.textContent = "No recordings ready";
    }
  }

  var sup = formatSupervisorStatus(status.supervisorStatus);
  var processingLabel = "Nothing processing";
  if (activeId) {
    processingLabel = formatPhase(phase) || "Active";
    if (phase === "failed_blocked" && status.storageBlocked) {
      processingLabel = "Blocked (storage)";
    }
  }
  if (cardProcessing) cardProcessing.textContent = processingLabel;

  var hudLabel = "Idle";
  if (activeId) hudLabel = "Processing";
  else if (sup === "polling") hudLabel = "Polling";
  else if (sup === "blocked") hudLabel = "Blocked";
  else if (sup === "error") hudLabel = "Error";
  else if (sup === "starting") hudLabel = "Starting";
  if (hudStatus) {
    hudStatus.textContent =
      phase === "failed_blocked" ? "Blocked" : hudLabel;
  }
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

  renderInSyncProgressLog(status);

  if (!activeId) {
    if (empty) {
      empty.hidden = pending.length > 0;
      empty.textContent = pending.length
        ? "Select Process now to finalize the next ready session"
        : "No active session";
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
  if (processBtn) {
    processBtn.disabled = !primaryPendingId || Boolean(activeId);
  }
  if (revealBtn) revealBtn.disabled = !canOpen;
  if (reportBtn) {
    reportBtn.disabled = !canOpen || !status.recoveryReportPath;
  }

  return {
    activeId: activeId,
    primaryPendingId: primaryPendingId,
    status: status,
    canOpen: canOpen,
  };
}

async function refreshInSyncRecordings(options) {
  var skipIfBusy = options && options.skipIfBusy;
  if (inSyncActiveRefresh) {
    if (skipIfBusy) {
      return inSyncActiveRefresh.then(function () {
        return refreshInSyncRecordings({ skipIfBusy: true });
      });
    }
    await inSyncActiveRefresh;
  }
  var run = (async function () {
  setInSyncBusy(true, "Refreshing…");
  try {
    var status = await fetchInSyncStatusPayload();
    inSyncInitState = "ready";
    inSyncLastRefreshAt = Date.now();
    clearInSyncPanelError();
    var ctx = applyInSyncStatus(status);

    var processBtn = document.getElementById("in-sync-process-now");
    if (processBtn && !processBtn.dataset.boundClick) {
      processBtn.dataset.boundClick = "1";
      processBtn.onclick = async function () {
        if (!ctx.primaryPendingId) return;
        processBtn.disabled = true;
        processBtn.textContent = "Queuing…";
        try {
          await inSyncPrivileged("/api/in-sync/process-now", {
            method: "POST",
            body: JSON.stringify({ sessionId: ctx.primaryPendingId }),
          });
          processBtn.textContent = "Queued";
          await refreshInSyncRecordings();
        } catch (e) {
          showInSyncPanelError(e && e.message ? e.message : "Process now failed");
        } finally {
          processBtn.textContent = "Process now";
        }
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
    inSyncInitState = "error";
    showInSyncPanelError(
      err && err.message ? err.message : "Could not load In-Sync status"
    );
  } finally {
    setInSyncBusy(false);
  }
  })();
  inSyncActiveRefresh = run;
  try {
    return await run;
  } finally {
    if (inSyncActiveRefresh === run) inSyncActiveRefresh = null;
  }
}

function wireInSyncRecordingsUi() {
  if (!window.streamSyncControlApi || !window.streamSyncControlApi.privilegedFetch) {
    inSyncInitState = "error";
    showInSyncPanelError("Control API unavailable — restart Stream Sync");
    return;
  }

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
        clearInSyncPanelError();
      } catch (e) {
        console.error(e);
        setInSyncModeToggle(previousMode);
        showInSyncPanelError(e && e.message ? e.message : "Mode change failed");
      } finally {
        modeToggle.disabled = false;
      }
    });
  }

  var refreshBtn = document.getElementById("in-sync-refresh");
  if (refreshBtn && !refreshBtn.dataset.bound) {
    refreshBtn.dataset.bound = "1";
    refreshBtn.addEventListener("click", function () {
      refreshInSyncRecordings().catch(console.error);
    });
  }

  document.getElementById("in-sync-pause")?.addEventListener("click", async function () {
    try {
      await inSyncPrivileged("/api/in-sync/pause", {
        method: "POST",
        body: JSON.stringify({ paused: true }),
      });
      await refreshInSyncRecordings();
    } catch (e) {
      showInSyncPanelError(e && e.message ? e.message : "Pause failed");
    }
  });
  document.getElementById("in-sync-resume")?.addEventListener("click", async function () {
    try {
      await inSyncPrivileged("/api/in-sync/pause", {
        method: "POST",
        body: JSON.stringify({ paused: false }),
      });
      await refreshInSyncRecordings();
    } catch (e) {
      showInSyncPanelError(e && e.message ? e.message : "Resume failed");
    }
  });
  document.getElementById("in-sync-retry")?.addEventListener("click", function () {
    refreshInSyncRecordings().catch(console.error);
  });

  ensureInSyncLiveRefresh();
  if (!inSyncUiWired) {
    inSyncUiWired = true;
    inSyncInitState = "starting";
    applyInSyncEmptyUi();
    refreshInSyncRecordings().catch(console.error);
  }
}
