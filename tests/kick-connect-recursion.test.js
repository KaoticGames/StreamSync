"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const src = fs.readFileSync(
  path.join(__dirname, "..", "connections-api.js"),
  "utf8"
);

function functionBody(name) {
  const re = new RegExp(`async function ${name}\\(\\) \\{([\\s\\S]*?)\\n  \\}`);
  const m = src.match(re);
  assert.ok(m, `${name} must exist in connections-api.js`);
  return m[1];
}

describe("Kick connect must not recurse through electronAPI", () => {
  it("kickConnect fetches an auth URL instead of calling electronAPI.kickConnect", () => {
    const body = functionBody("kickConnect");
    assert.equal(
      /electronAPI\?\.kickConnect/.test(body),
      false,
      "kickConnect calling electronAPI.kickConnect recurses: tauri-bridge.kickConnect calls streamSyncConnections.kickConnect"
    );
    assert.match(body, /kickFetchAuthUrl/);
    assert.match(body, /openAuthUrl/);
  });

  it("Twitch connect does not call electronAPI.connect (same trap)", () => {
    const body = functionBody("connect");
    assert.equal(/electronAPI\?\.connect/.test(body), false);
    assert.match(body, /fetchAuthUrl/);
  });
});
