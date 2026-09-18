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
});
