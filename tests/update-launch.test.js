"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.join(__dirname, "..");
const shell = fs.readFileSync(path.join(root, "shell.html"), "utf8");
const bridge = fs.readFileSync(path.join(root, "tauri-bridge.js"), "utf8");
const lib = fs.readFileSync(
  path.join(root, "crates", "stream-sync-desktop", "src", "lib.rs"),
  "utf8"
);

describe("launch update check", () => {
  it("loads modal support before the Tauri bridge", () => {
    assert.ok(
      shell.indexOf('src="update-modal.js"') < shell.indexOf('src="tauri-bridge.js"')
    );
  });

  it("starts the background check only after listeners are ready", () => {
    assert.match(
      bridge,
      /wireUpdateModal[\s\S]*?\.ready[\s\S]*?invoke\("check_for_updates_background"\)/
    );
  });

  it("does not race the page by spawning from native window creation", () => {
    assert.doesNotMatch(lib, /spawn_launch_check/);
  });
});
