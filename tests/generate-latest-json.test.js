"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  generateLatestJson,
} = require("../scripts/generate-latest-json.js");

describe("generateLatestJson", () => {
  it("builds windows-x86_64 only manifest", () => {
    const json = generateLatestJson({
      version: "2.1.1",
      assetUrl:
        "https://github.com/KaoticGames/StreamSync/releases/download/v2.1.1/StreamSync-windows-x86_64-setup.exe",
      signature: "dGVzdC1zaWc=",
      notes: "Bug fixes",
      pubDate: "2026-09-17T18:00:00Z",
    });

    assert.equal(json.version, "2.1.1");
    assert.equal(json.notes, "Bug fixes");
    assert.equal(json.pub_date, "2026-09-17T18:00:00Z");
    assert.deepEqual(Object.keys(json.platforms), ["windows-x86_64"]);
    assert.equal(
      json.platforms["windows-x86_64"].url,
      "https://github.com/KaoticGames/StreamSync/releases/download/v2.1.1/StreamSync-windows-x86_64-setup.exe"
    );
    assert.equal(json.platforms["windows-x86_64"].signature, "dGVzdC1zaWc=");
  });

  it("rejects non-GitHub asset hosts", () => {
    assert.throws(
      () =>
        generateLatestJson({
          version: "2.1.1",
          assetUrl: "https://evil.example/installer.exe",
          signature: "sig",
        }),
      /unexpected asset host/
    );
  });

  it("requires the immutable canonical asset URL for the same version", () => {
    for (const assetUrl of [
      "https://github.com/Other/Repo/releases/download/v2.1.1/StreamSync-windows-x86_64-setup.exe",
      "https://github.com/KaoticGames/StreamSync/releases/download/v2.1.0/StreamSync-windows-x86_64-setup.exe",
      "https://github.com/KaoticGames/StreamSync/releases/download/v2.1.1/renamed.exe",
    ]) {
      assert.throws(
        () =>
          generateLatestJson({
            version: "2.1.1",
            assetUrl,
            signature: "sig",
          }),
        /unexpected asset path/
      );
    }
  });
});
