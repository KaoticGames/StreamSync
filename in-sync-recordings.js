// In-Sync Recordings functional UI (local finalization queue)

async function inSyncPrivileged(path, options) {
  if (!window.streamSyncControlApi || !window.streamSyncControlApi.privilegedFetch) {
    throw new Error("control API unavailable");
  }
  const res = await window.streamSyncControlApi.privilegedFetch(path, options || {});
  const data = await res.json().catch(function () {
    return {};
  });
  if (!res.ok) throw new Error(data.error || "HTTP " + res.status);
  return data;
}

async function refreshInSyncRecordings() {
  const status = await inSyncPrivileged("/api/in-sync/status");
  const modeSel = document.getElementById("in-sync-processing-mode");
  if (modeSel && status.localProcessingMode) {
    modeSel.value =
      status.localProcessingMode === "automatic_after_seal"
        ? "automatic_after_seal"
        : "manual";
  }
  const fields = document.getElementById("in-sync-session-fields");
  const empty = document.getElementById("in-sync-empty");
  const log = document.getElementById("in-sync-progress-log");
  if (!fields || !empty || !log) return;

  if (!status.activeSessionId) {
    empty.hidden = false;
    fields.hidden = true;
    log.textContent = JSON.stringify(status.progressEvents || [], null, 2);
    return;
  }
  empty.hidden = true;
  fields.hidden = false;
  fields.innerHTML =
    "<dt>Session</dt><dd>" +
    status.activeSessionId +
    "</dd><dt>Phase</dt><dd>" +
    (status.phase || "—") +
    "</dd><dt>Manual queue</dt><dd>" +
    ((status.manualQueue || []).join(", ") || "—") +
    "</dd>";
  log.textContent = JSON.stringify(status.progressEvents || [], null, 2);

  const processBtn = document.getElementById("in-sync-process-now");
  if (processBtn) {
    processBtn.disabled = !status.activeSessionId;
    processBtn.onclick = async function () {
      await inSyncPrivileged("/api/in-sync/process-now", {
        method: "POST",
        body: JSON.stringify({ sessionId: status.activeSessionId }),
      });
      await refreshInSyncRecordings();
    };
  }

  const revealBtn = document.getElementById("in-sync-reveal");
  const reportBtn = document.getElementById("in-sync-open-report");
  const sid = status.activeSessionId;
  const canOpen = Boolean(sid && status.publishedOutputDir);
  if (revealBtn) {
    revealBtn.disabled = !canOpen;
    revealBtn.onclick = async function () {
      if (window.__TAURI__ && window.__TAURI__.core) {
        await window.__TAURI__.core.invoke("reveal_in_sync_output", {
          sessionId: sid,
        });
      }
    };
  }
  if (reportBtn) {
    reportBtn.disabled = !canOpen || !status.recoveryReportPath;
    reportBtn.onclick = async function () {
      if (window.__TAURI__ && window.__TAURI__.core) {
        await window.__TAURI__.core.invoke("open_in_sync_recovery_report", {
          sessionId: sid,
        });
      }
    };
  }
}

function wireInSyncRecordingsUi() {
  const refreshBtn = document.getElementById("in-sync-refresh");
  if (refreshBtn) {
    refreshBtn.addEventListener("click", function () {
      refreshInSyncRecordings().catch(console.error);
    });
  }
  const modeSel = document.getElementById("in-sync-processing-mode");
  if (modeSel) {
    modeSel.addEventListener("change", async function (e) {
      await inSyncPrivileged("/api/in-sync/processing-mode", {
        method: "POST",
        body: JSON.stringify({ mode: e.target.value }),
      });
    });
  }
  document.getElementById("in-sync-pause")?.addEventListener("click", async function () {
    await inSyncPrivileged("/api/in-sync/pause", {
      method: "POST",
      body: JSON.stringify({ paused: true }),
    });
  });
  document.getElementById("in-sync-resume")?.addEventListener("click", async function () {
    await inSyncPrivileged("/api/in-sync/pause", {
      method: "POST",
      body: JSON.stringify({ paused: false }),
    });
  });
  document.getElementById("in-sync-retry")?.addEventListener("click", function () {
    refreshInSyncRecordings().catch(console.error);
  });
  refreshInSyncRecordings().catch(console.error);
}
