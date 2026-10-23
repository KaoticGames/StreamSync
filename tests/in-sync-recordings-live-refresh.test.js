"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const IN_SYNC_POLL_INTERVAL_MS = 2000;

function createElement(tag) {
  return {
    tagName: tag.toUpperCase(),
    id: "",
    hidden: false,
    textContent: "",
    innerHTML: "",
    className: "",
    disabled: false,
    dataset: {},
    classList: {
      _values: new Set(),
      add(name) {
        this._values.add(name);
      },
      remove(name) {
        this._values.delete(name);
      },
      toggle(name, on) {
        if (on) this._values.add(name);
        else this._values.delete(name);
      },
      contains(name) {
        return this._values.has(name);
      },
    },
    setAttribute() {},
    getAttribute(name) {
      if (name === "aria-pressed") return "false";
      return null;
    },
    querySelector() {
      return null;
    },
    addEventListener() {},
    click() {},
    onclick: null,
  };
}

function createMockDocument() {
  const nodes = new Map();

  function register(el) {
    if (el.id) nodes.set(el.id, el);
    return el;
  }

  const requiredIds = [
    "in-sync-processing-mode-toggle",
    "in-sync-mode-manual-label",
    "in-sync-mode-automatic-label",
    "in-sync-init-error",
    "in-sync-refresh",
    "in-sync-hud-status",
    "in-sync-hud-phase",
    "in-sync-hud-session",
    "in-sync-card-queue",
    "in-sync-card-processing",
    "in-sync-card-mode",
    "in-sync-card-published",
    "in-sync-empty",
    "in-sync-session-fields",
    "in-sync-progress-log",
    "in-sync-process-now",
    "in-sync-reveal",
    "in-sync-open-report",
    "in-sync-pending-list",
    "in-sync-discovery-status",
    "in-sync-storage-blocked",
    "in-sync-supervisor-meta",
    "in-sync-pause",
    "in-sync-resume",
    "in-sync-retry",
  ];

  for (const id of requiredIds) {
    register(Object.assign(createElement("div"), { id }));
  }

  return {
    getElementById(id) {
      return nodes.get(id) || null;
    },
    createElement(tag) {
      return createElement(tag);
    },
  };
}

function loadInSyncRecordings(host) {
  vm.runInContext(
    fs.readFileSync(path.join(__dirname, "..", "in-sync-recordings.js"), "utf8"),
    vm.createContext(host)
  );
}

function baseStatus() {
  return {
    localProcessingMode: "manual",
    phase: "queued",
    activeSessionId: null,
    manualQueue: [],
    pendingSessions: [],
    discovery: { status: "ok" },
    progressEvents: [],
    publishedSessions: [],
    publishedOutputDir: null,
    recoveryReportPath: null,
    storageBlocked: null,
    storageBlockedSessionId: null,
    supervisorStatus: "ready",
    supervisorPendingCount: 0,
  };
}

function loadApplyInSyncStatus() {
  const document = createMockDocument();
  const host = {
    document,
    console,
    setInterval() {
      return 1;
    },
    clearInterval() {},
    Date,
    streamSyncControlApi: {
      privilegedFetch: async () => ({
        ok: true,
        json: async () => baseStatus(),
      }),
    },
  };
  host.window = host;
  host.globalThis = host;
  loadInSyncRecordings(host);
  assert.equal(typeof host.applyInSyncStatus, "function");
  return { host, document };
}

describe("In-Sync progress log UX", () => {
  it("applyInSyncStatus renders human-readable progress, not raw JSON as primary text", () => {
    const { host, document } = loadApplyInSyncStatus();
    const publishedDir = "/home/recorder/voice-out/session-abc";
    const status = Object.assign(baseStatus(), {
      activeSessionId: "550e8400-e29b-41d4-a716-446655440099",
      phase: "downloading",
      progressEvents: [
        { type: "journal_ready", sessionId: "550e8400-e29b-41d4-a716-446655440099" },
        {
          type: "download_progress",
          sessionId: "550e8400-e29b-41d4-a716-446655440099",
          segmentSeq: 0,
          bytesComplete: 1024,
          bytesTotal: 4096,
        },
        {
          type: "segment_verified",
          sessionId: "550e8400-e29b-41d4-a716-446655440099",
          segmentSeq: 0,
        },
        {
          type: "processing_started",
          sessionId: "550e8400-e29b-41d4-a716-446655440099",
        },
        {
          type: "timeline_progress",
          sessionId: "550e8400-e29b-41d4-a716-446655440099",
          processedSegments: 2,
          totalSegments: 5,
        },
        {
          type: "publication_complete",
          sessionId: "550e8400-e29b-41d4-a716-446655440099",
          finalSessionDir: publishedDir,
          recoveryReportPath: publishedDir + "/recovery-report.json",
        },
        {
          type: "receipt_acknowledged",
          sessionId: "550e8400-e29b-41d4-a716-446655440099",
        },
      ],
    });

    host.applyInSyncStatus(status);
    const log = document.getElementById("in-sync-progress-log");
    assert.ok(log, "progress log element must exist");
    const text = log.textContent || "";

    assert.ok(
      !text.trim().startsWith("["),
      "progress log primary text must not be raw JSON array"
    );
    assert.ok(
      text.indexOf('"type":') === -1 || text.indexOf("--- Diagnostics ---") !== -1,
      "raw JSON must not appear before diagnostics section"
    );
    assert.match(text, /Phase: downloading/i);
    assert.match(text, /Journal ready/);
    assert.match(text, /Download segment 0: 1024 \/ 4096 bytes/);
    assert.match(text, /Segment 0 verified/);
    assert.match(text, /Processing started/);
    assert.match(text, /Timeline 2 \/ 5 segments/);
    assert.match(text, new RegExp("Published: " + publishedDir.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
    assert.match(text, /Receipt acknowledged/);
    assert.match(text, /--- Diagnostics ---/);
  });

  it("shows paused and retryable phase text in the progress log", () => {
    const { host, document } = loadApplyInSyncStatus();

    host.applyInSyncStatus(
      Object.assign(baseStatus(), {
        activeSessionId: "sess-paused",
        phase: "paused",
        progressEvents: [
          { type: "journal_ready", sessionId: "sess-paused" },
        ],
      })
    );
    let log = document.getElementById("in-sync-progress-log");
    assert.match(log.textContent, /Phase: Paused/);

    host.applyInSyncStatus(
      Object.assign(baseStatus(), {
        activeSessionId: "sess-retry",
        phase: "failed_retryable",
        progressEvents: [],
      })
    );
    log = document.getElementById("in-sync-progress-log");
    assert.match(log.textContent, /Failed \(retryable/);
  });
});

describe("In-Sync live refresh contract", () => {
  it("installs a single bounded polling loop while wired; refresh does not overlap", async () => {
    const intervals = [];
    const clears = [];
    let inFlight = 0;
    let maxInFlight = 0;
    let fetchCount = 0;

    const document = createMockDocument();
    const host = {
      document,
      console,
      setInterval(fn, ms) {
        intervals.push({ fn, ms });
        return intervals.length;
      },
      clearInterval(id) {
        clears.push(id);
      },
      Date,
      streamSyncControlApi: {
        privilegedFetch: async () => {
          fetchCount += 1;
          inFlight += 1;
          maxInFlight = Math.max(maxInFlight, inFlight);
          await Promise.resolve();
          inFlight -= 1;
          return {
            ok: true,
            json: async () => baseStatus(),
          };
        },
      },
    };
    host.window = host;
    host.globalThis = host;

    loadInSyncRecordings(host);
    assert.equal(typeof host.wireInSyncRecordingsUi, "function");

    host.wireInSyncRecordingsUi();
    await Promise.resolve();
    const afterFirstWire = fetchCount;

    host.wireInSyncRecordingsUi();
    await Promise.resolve();

    assert.equal(
      intervals.length,
      1,
      "exactly one polling interval must be installed while In-Sync is wired"
    );
    assert.equal(
      intervals[0].ms,
      IN_SYNC_POLL_INTERVAL_MS,
      "polling interval must use the bounded live-refresh period"
    );

    const firstPoll = intervals[0].fn();
    const overlappingPoll = intervals[0].fn();
    await Promise.all([firstPoll, overlappingPoll]);

    assert.ok(
      fetchCount > afterFirstWire,
      "automatic polling must refresh status without pressing Refresh"
    );
    assert.equal(
      maxInFlight,
      1,
      "polling must not overlap in-flight status requests"
    );

    if (typeof host.stopInSyncLiveRefresh === "function") {
      host.stopInSyncLiveRefresh();
      assert.ok(clears.length >= 1, "stop must clear the polling interval");
    } else {
      assert.fail(
        "stopInSyncLiveRefresh must exist to stop polling without duplicate timers"
      );
    }
  });
});
