"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  canonicalToDisplayLabel,
  uiVersionTag,
  expectedUiVersionTag,
} = require("../scripts/version-labels.js");

describe("version labels", () => {
  it("maps canonical 2.1.1 to padded display label 2.1.001", () => {
    assert.equal(canonicalToDisplayLabel("2.1.1"), "2.1.001");
  });

  it("distinguishes canonical semver from padded UI tags", () => {
    assert.equal(expectedUiVersionTag("2.1.1"), "v2.1.001");
    assert.equal(uiVersionTag("2.1.1"), "v2.1.001");
    assert.notEqual(uiVersionTag("2.1.1"), "v2.1.1");
  });
});
