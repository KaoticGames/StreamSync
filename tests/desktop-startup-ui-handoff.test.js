"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { describe, it } = require("node:test");

const root = path.join(__dirname, "..");

function read(relativePath) {
  return fs.readFileSync(path.join(root, relativePath), "utf8");
}

function assertNonInSyncSurfaceExcluded() {
  const shell = read("shell.html");
  const renderer = read("renderer.js");
  const desktopLib = read("crates/stream-sync-desktop/src/lib.rs");

  assert.doesNotMatch(shell, /data-view=["']in-sync["']/);
  assert.doesNotMatch(renderer, /views\/in-sync\.html/);
  assert.doesNotMatch(
    desktopLib,
    /pick_recording_folder|reveal_in_sync_output|open_in_sync_recovery_report/
  );
  assert.equal(fs.existsSync(path.join(root, "views", "in-sync.html")), false);
}

describe("desktop startup loading UI (ui-placeholder)", () => {
  it("provides an accessible progress bar, status text, and startup-progress handler", () => {
    const html = read("crates/stream-sync-desktop/ui-placeholder/index.html");
    const hasProgressBar =
      /<progress[\s>]/i.test(html) ||
      /role\s*=\s*["']progressbar["']/i.test(html);
    assert.ok(
      hasProgressBar,
      "startup placeholder must expose a progress bar element or role=progressbar"
    );
    assert.match(
      html,
      /startup-status|id=["'][^"']*status[^"']*["']|role=["']status["']/i,
      "startup placeholder must include dedicated status text"
    );
    assert.match(
      html,
      /startup-progress[\s\S]*?(addEventListener|listen|on\()/,
      "startup placeholder must handle startup-progress events"
    );
  });
});

describe("desktop startup window handoff (lib.rs)", () => {
  it("keeps the revenue candidate free of In-Sync startup surfaces", () => {
    assertNonInSyncSurfaceExcluded();
  });

  it("shows a startup window, emits progress, opens main, then closes startup", () => {
    const lib = read("crates/stream-sync-desktop/src/lib.rs");

    assert.match(
      lib,
      /WebviewWindowBuilder::new\([\s\S]*?"startup"/,
      "must create a webview window labeled startup"
    );
    assert.match(
      lib,
      /WebviewUrl::App/,
      "startup window must load bundled app assets via WebviewUrl::App"
    );
    assert.match(
      lib,
      /startup-progress/,
      "native startup must emit startup-progress updates"
    );

    const builders = [...lib.matchAll(
      /WebviewWindowBuilder::new\(\s*[^,]+,\s*"([^"]+)"/g
    )];
    const mainBuilder = builders.find((match) => match[1] === "main");
    assert.ok(mainBuilder, "must create the main window");
    const startupBuilder = builders.find((match) => match[1] === "startup");
    assert.ok(startupBuilder, "must create the startup window");
    assert.ok(
      startupBuilder.index < mainBuilder.index,
      "startup window must be created before the main window handoff"
    );

    const afterMain = lib.slice(mainBuilder.index);
    assert.match(
      afterMain,
      /"startup"[\s\S]*?\.close\s*\(/,
      "startup window must close only after main window creation"
    );
    assert.match(afterMain, /\.show\s*\(/, "main window must be shown after load handoff");
  });
});
