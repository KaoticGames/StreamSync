"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { describe, it } = require("node:test");

const root = path.join(__dirname, "..");
const coreSrc = path.join(root, "crates", "stream-sync-core", "src");

function read(relativePath) {
  return fs.readFileSync(path.join(root, relativePath), "utf8");
}

function topLevelCoreRsFiles() {
  return fs
    .readdirSync(coreSrc)
    .filter((name) => name.endsWith(".rs"))
    .map((name) => ({
      name,
      stem: name.replace(/\.rs$/, ""),
      content: fs.readFileSync(path.join(coreSrc, name), "utf8"),
    }));
}

function isWindowsCommandHelperSource(content) {
  return (
    /use std::os::windows::process::CommandExt/.test(content) &&
    /CREATE_NO_WINDOW/.test(content) &&
    /0x0800_0000|0x08000000/.test(content) &&
    /#\[cfg\(windows\)\]/.test(content) &&
    /creation_flags\s*\(\s*CREATE_NO_WINDOW\s*\)/.test(content) &&
    /#\[cfg\(not\(windows\)\)\]/.test(content)
  );
}

function discoverWindowsCommandHelperModule() {
  return topLevelCoreRsFiles().filter((file) =>
    isWindowsCommandHelperSource(file.content)
  );
}

function extractFunctionSource(source, fnName) {
  const marker = `fn ${fnName}`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `expected fn ${fnName}`);
  const openBrace = source.indexOf("{", start);
  assert.notEqual(openBrace, -1, `expected body for fn ${fnName}`);
  let depth = 0;
  for (let i = openBrace; i < source.length; i += 1) {
    const ch = source[i];
    if (ch === "{") {
      depth += 1;
    } else if (ch === "}") {
      depth -= 1;
      if (depth === 0) {
        return source.slice(start, i + 1);
      }
    }
  }
  assert.fail(`unbalanced braces in fn ${fnName}`);
}

describe("Windows hidden subprocess helper (icacls / tasklist seams)", () => {
  it("defines one shared top-level core helper with CREATE_NO_WINDOW and a non-Windows cfg", () => {
    const helpers = discoverWindowsCommandHelperModule();
    assert.equal(
      helpers.length,
      1,
      `expected exactly one top-level Windows command helper in stream-sync-core/src, found ${helpers.length}`
    );
  });

  it("declares the Windows command helper module from stream-sync-core lib.rs", () => {
    const helpers = discoverWindowsCommandHelperModule();
    assert.equal(helpers.length, 1);
    const modName = helpers[0].stem;
    const lib = read("crates/stream-sync-core/src/lib.rs");
    assert.match(
      lib,
      new RegExp(`\\bmod\\s+${modName}\\s*;`),
      `lib.rs must declare mod ${modName}`
    );
  });

  it("routes storage.rs icacls calls through the shared helper instead of direct Command", () => {
    const storageFn = extractFunctionSource(
      read("crates/stream-sync-core/src/storage.rs"),
      "apply_secret_file_permissions"
    );
    assert.doesNotMatch(
      storageFn,
      /std::process::Command::new\(\s*"icacls"\s*\)/,
      "icacls must not be launched via raw std::process::Command in storage.rs"
    );
    const helpers = discoverWindowsCommandHelperModule();
    assert.equal(helpers.length, 1);
    const modName = helpers[0].stem;
    assert.match(
      storageFn,
      new RegExp(`crate::${modName}::`),
      `storage.rs must delegate icacls to crate::${modName}`
    );
  });

  it("routes control_plane.rs tasklist calls through the shared helper instead of direct Command", () => {
    const lockFn = extractFunctionSource(
      read("crates/stream-sync-core/src/control_plane.rs"),
      "lock_owner_is_alive"
    );
    assert.doesNotMatch(
      lockFn,
      /std::process::Command::new\(\s*"tasklist"\s*\)/,
      "tasklist must not be launched via raw std::process::Command in control_plane.rs"
    );
    const helpers = discoverWindowsCommandHelperModule();
    assert.equal(helpers.length, 1);
    const modName = helpers[0].stem;
    assert.match(
      lockFn,
      new RegExp(`crate::${modName}::`),
      `control_plane.rs must delegate tasklist to crate::${modName}`
    );
  });
});
