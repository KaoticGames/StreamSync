#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const TAURI_CONF = path.join(
  __dirname,
  "..",
  "crates",
  "stream-sync-desktop",
  "tauri.conf.json"
);

const SEMVER_TAG = /^v(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?)$/;

function readTauriVersion() {
  const raw = fs.readFileSync(TAURI_CONF, "utf8");
  const conf = JSON.parse(raw);
  if (!conf.version || typeof conf.version !== "string") {
    throw new Error("tauri.conf.json is missing a string version");
  }
  return conf.version.trim();
}

/**
 * @param {string} tagName e.g. v2.1.0
 * @param {string} [sourceVersion] defaults to tauri.conf.json version
 * @returns {{ ok: true, tagVersion: string, sourceVersion: string } | { ok: false, error: string }}
 */
function validateReleaseTag(tagName, sourceVersion = readTauriVersion()) {
  const tag = String(tagName || "").trim();
  const match = SEMVER_TAG.exec(tag);
  if (!match) {
    return {
      ok: false,
      error: `tag "${tag}" is not a valid semver tag (expected vX.Y.Z)`,
    };
  }

  const tagVersion = match[1];
  const expected = String(sourceVersion || "").trim();
  if (!expected) {
    return { ok: false, error: "source version is empty" };
  }
  if (tagVersion !== expected) {
    return {
      ok: false,
      error: `tag version ${tagVersion} does not match source version ${expected}`,
    };
  }

  return { ok: true, tagVersion, sourceVersion: expected };
}

module.exports = { validateReleaseTag, readTauriVersion, SEMVER_TAG };

if (require.main === module) {
  const tag = process.argv[2];
  if (!tag) {
    console.error("usage: node scripts/validate-release-tag.js <tag>");
    process.exit(2);
  }
  const result = validateReleaseTag(tag);
  if (!result.ok) {
    console.error(result.error);
    process.exit(1);
  }
  console.log(
    `tag ${tag} matches source version ${result.sourceVersion}`
  );
}
