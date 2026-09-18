"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const workflowsDir = path.join(__dirname, "..", ".github", "workflows");
const workflow = fs.readFileSync(path.join(workflowsDir, "release.yml"), "utf8");

describe("release workflow", () => {
  it("routes version-like tags through the strict tag validator", () => {
    assert.match(workflow, /^\s{6}- "v\*"$/m);
  });

  it("pins every external action to a full commit SHA", () => {
    for (const name of fs.readdirSync(workflowsDir)) {
      if (!name.endsWith(".yml") && !name.endsWith(".yaml")) continue;
      const source = fs.readFileSync(path.join(workflowsDir, name), "utf8");
      for (const match of source.matchAll(/uses:\s*([^\s@]+)@([^\s#]+)/g)) {
        assert.match(
          match[2],
          /^[0-9a-f]{40}$/,
          `${name}: ${match[1]} must use a reviewed full commit SHA`
        );
      }
    }
  });

  it("gives draft verification both GitHub auth and the expected tag", () => {
    const step = workflow.match(
      /- name: Verify draft assets before publish([\s\S]*?)(?=\n\s{6}- name:)/
    );
    assert.ok(step, "draft verification step exists");
    assert.equal((step[1].match(/^\s{8}env:/gm) || []).length, 1);
    assert.match(step[1], /GH_TOKEN:\s*\$\{\{ github\.token \}\}/);
    assert.match(step[1], /TAG:\s*\$\{\{ needs\.validate-tag\.outputs\.tag \}\}/);
  });

  it("refuses to publish incomplete signed-updater assets", () => {
    assert.match(workflow, /if \(-not \$sig\)[\s\S]*?throw/);
    for (const asset of [
      "StreamSync-windows-x86_64-setup.exe",
      "StreamSync-windows-x86_64-setup.exe.sig",
      "SHA256SUMS.txt",
      "latest.json",
    ]) {
      const occurrences = workflow.split(asset).length - 1;
      assert.ok(occurrences >= 3, `${asset} must be staged, uploaded, and verified`);
    }
  });

  it("uploads one flat release-assets directory", () => {
    assert.doesNotMatch(
      workflow,
      /path:\s*\|\s*\n\s+release-assets\/\s*\n\s+release-notes\.md/
    );
  });

  it("scopes signing secrets only to the Tauri build step", () => {
    assert.equal((workflow.match(/TAURI_SIGNING_PRIVATE_KEY:/g) || []).length, 1);
    assert.equal(
      (workflow.match(/TAURI_SIGNING_PRIVATE_KEY_PASSWORD:/g) || []).length,
      1
    );
    const buildStep = workflow.match(
      /- name: NSIS installer with updater artifacts([\s\S]*?)(?=\n\s{6}- name:)/
    );
    assert.ok(buildStep);
    assert.match(buildStep[1], /env:[\s\S]*TAURI_SIGNING_PRIVATE_KEY:/);
    assert.match(buildStep[1], /TAURI_SIGNING_PRIVATE_KEY_PASSWORD:/);
  });

  it("grants write permission only to release mutation jobs", () => {
    assert.match(workflow, /^permissions:\n  contents: read$/m);
    for (const job of ["draft-release", "publish-release"]) {
      assert.match(
        workflow,
        new RegExp(`^  ${job}:[\\s\\S]*?^    permissions:\\n      contents: write$`, "m")
      );
    }
  });
});
