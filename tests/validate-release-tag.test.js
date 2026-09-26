"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  validateReleaseTag,
  readTauriVersion,
} = require("../scripts/validate-release-tag.js");

describe("validateReleaseTag", () => {
  it("accepts v2.1.0 when source version is 2.1.0", () => {
    const result = validateReleaseTag("v2.1.0", "2.1.0");
    assert.equal(result.ok, true);
    assert.equal(result.tagVersion, "2.1.0");
  });

  it("rejects version mismatch", () => {
    const result = validateReleaseTag("v2.1.1", "2.1.0");
    assert.equal(result.ok, false);
    assert.match(result.error, /does not match/);
  });

  it("rejects malformed and non-stable tags", () => {
    assert.equal(validateReleaseTag("2.1.0", "2.1.0").ok, false);
    assert.equal(validateReleaseTag("v2.1", "2.1.0").ok, false);
    assert.equal(validateReleaseTag("", "2.1.0").ok, false);
    assert.equal(validateReleaseTag("v2.1.0-beta.1", "2.1.0-beta.1").ok, false);
    assert.equal(validateReleaseTag("v2.1.0+build.7", "2.1.0+build.7").ok, false);
  });

  it("reads the current tauri.conf.json version", () => {
    assert.equal(readTauriVersion(), "2.1.0");
    assert.equal(validateReleaseTag("v2.1.0").ok, true);
  });
});
