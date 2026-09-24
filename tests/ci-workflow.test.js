"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const repoRoot = path.join(__dirname, "..");
const ciWorkflow = fs.readFileSync(
  path.join(repoRoot, ".github", "workflows", "ci.yml"),
  "utf8"
);
const releaseWorkflow = fs.readFileSync(
  path.join(repoRoot, ".github", "workflows", "release.yml"),
  "utf8"
);
const ciTauriConfig = JSON.parse(
  fs.readFileSync(
    path.join(repoRoot, "crates", "stream-sync-desktop", "tauri.ci.conf.json"),
    "utf8"
  )
);
const prodTauriConfig = JSON.parse(
  fs.readFileSync(
    path.join(repoRoot, "crates", "stream-sync-desktop", "tauri.conf.json"),
    "utf8"
  )
);

describe("CI workflow", () => {
  it("windows-installer smoke uses checked-in CI Tauri config override", () => {
    const jobStart = ciWorkflow.indexOf("  windows-installer:");
    assert.ok(jobStart >= 0, "windows-installer job exists");
    const nextJob = ciWorkflow.indexOf("\n  signed-release:", jobStart);
    const job = ciWorkflow.slice(jobStart, nextJob);
    assert.match(
      job,
      /npx tauri build --bundles nsis --ci --no-sign --config tauri\.ci\.conf\.json/
    );
    assert.doesNotMatch(
      job,
      /--config '\{\\"bundle\\"/,
      "inline JSON --config is stripped by PowerShell and must not be used"
    );
    assert.equal(ciTauriConfig.bundle.createUpdaterArtifacts, false);
  });

  it("production tauri.conf.json still enables signed updater artifacts for release", () => {
    assert.equal(prodTauriConfig.bundle.createUpdaterArtifacts, true);
    assert.match(
      releaseWorkflow,
      /npx tauri build --bundles nsis --ci\s*\n\s+working-directory: crates\/stream-sync-desktop/
    );
    assert.doesNotMatch(
      releaseWorkflow,
      /tauri\.ci\.conf\.json/,
      "release must not use CI-only updater override"
    );
    assert.match(releaseWorkflow, /if \(-not \$sig\)[\s\S]*?throw/);
  });
});
