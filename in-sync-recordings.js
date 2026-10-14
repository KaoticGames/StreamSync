// In-Sync Recordings functional UI (local finalization queue)

var IN_SYNC_MODE_ITEMS = [
  { id: "manual", name: "Manual" },
  { id: "automatic_after_seal", name: "Automatic" },
];

function inSyncModeLabel(mode) {
  return mode === "automatic_after_seal" ? "Automatic" : "Manual";
}

function mountInSyncSyndicateSelect(rootEl, items, selectedId, onChange) {
  var btn = rootEl.querySelector(".ss-select__btn");
  var menu = rootEl.querySelector(".ss-select__menu");
  if (!btn || !menu) return null;

  var activeIndex = -1;

  function render() {
    var selected = items.find(function (i) {
      return i.id === selectedId;
    });
    if (!selected && items.length) selected = items[0];
    btn.textContent = selected ? selected.name : "Select…";
    rootEl.dataset.selected = selectedId || "";
    btn.setAttribute("aria-expanded", menu.classList.contains("open") ? "true" : "false");

    menu.innerHTML = "";
    items.forEach(function (it, idx) {
      var div = document.createElement("div");
      div.className =
        "ss-select__item" + (it.id === selectedId ? " selected" : "");
      div.textContent = it.name;
      div.dataset.id = it.id;
      div.setAttribute("role", "option");
      div.setAttribute("aria-selected", it.id === selectedId ? "true" : "false");
      div.tabIndex = -1;
      div.addEventListener("click", function () {
        selectItem(it.id);
      });
      menu.appendChild(div);
    });
  }

  function selectItem(id) {
    selectedId = id;
    menu.classList.remove("open");
    render();
    if (typeof onChange === "function") onChange(id);
  }

  function openMenu() {
    menu.classList.add("open");
    btn.setAttribute("aria-expanded", "true");
    var itemsEl = menu.querySelectorAll(".ss-select__item");
    activeIndex = Math.max(
      0,
      items.findIndex(function (i) {
        return i.id === selectedId;
      })
    );
    if (itemsEl[activeIndex]) itemsEl[activeIndex].focus();
  }

  function closeMenu() {
    menu.classList.remove("open");
    btn.setAttribute("aria-expanded", "false");
    btn.focus();
  }

  btn.addEventListener("click", function (e) {
    e.preventDefault();
    if (menu.classList.contains("open")) closeMenu();
    else openMenu();
  });

  btn.addEventListener("keydown", function (e) {
    if (e.key === "ArrowDown" || e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      if (!menu.classList.contains("open")) openMenu();
    }
    if (e.key === "Escape") closeMenu();
  });

  menu.addEventListener("keydown", function (e) {
    var itemsEl = menu.querySelectorAll(".ss-select__item");
    if (!itemsEl.length) return;
    if (e.key === "Escape") {
      e.preventDefault();
      closeMenu();
      return;
    }
    if (e.key === "ArrowDown") {
      e.preventDefault();
      activeIndex = Math.min(activeIndex + 1, itemsEl.length - 1);
      itemsEl[activeIndex].focus();
    }
    if (e.key === "ArrowUp") {
      e.preventDefault();
      activeIndex = Math.max(activeIndex - 1, 0);
      itemsEl[activeIndex].focus();
    }
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      var id = itemsEl[activeIndex] && itemsEl[activeIndex].dataset.id;
      if (id) selectItem(id);
    }
  });

  document.addEventListener("click", function (e) {
    if (!rootEl.contains(e.target)) menu.classList.remove("open");
  });

  render();

  return {
    setSelected: function (id) {
      selectedId = id;
      render();
    },
    getSelected: function () {
      return selectedId;
    },
  };
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

  var selectRoot = document.getElementById("in-sync-processing-mode");
  if (selectRoot && selectRoot.__ssWidget) {
    selectRoot.__ssWidget.setSelected(
      mode === "automatic_after_seal" ? "automatic_after_seal" : "manual"
    );
  }

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
  var selectRoot = document.getElementById("in-sync-processing-mode");
  if (selectRoot && selectRoot.dataset.bound !== "1") {
    selectRoot.dataset.bound = "1";
    var initial = selectRoot.dataset.selected || "manual";
    selectRoot.__ssWidget = mountInSyncSyndicateSelect(
      selectRoot,
      IN_SYNC_MODE_ITEMS,
      initial,
      async function (id) {
        try {
          await inSyncPrivileged("/api/in-sync/processing-mode", {
            method: "POST",
            body: JSON.stringify({ mode: id }),
          });
          var cardMode = document.getElementById("in-sync-card-mode");
          if (cardMode) cardMode.textContent = inSyncModeLabel(id);
        } catch (e) {
          console.error(e);
        }
      }
    );
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
