#!/usr/bin/env node
/** Fail unless Cargo workspace, npm, and Tauri versions match. */
const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");

function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

function cargoWorkspaceVersion() {
  const text = read("Cargo.toml");
  const block = text.match(/\[workspace\.package\]([\s\S]*?)(?:\n\[|\s*$)/);
  if (!block) {
    throw new Error("Cargo.toml is missing [workspace.package]");
  }
  const version = block[1].match(/^\s*version\s*=\s*"([^"]+)"/m);
  if (!version) {
    throw new Error("Cargo.toml [workspace.package] is missing version");
  }
  return version[1];
}

function crateUsesWorkspaceVersion(rel) {
  const text = read(rel);
  return /^\s*version\.workspace\s*=\s*true\s*$/m.test(text);
}

const cargoVersion = cargoWorkspaceVersion();
const npmVersion = JSON.parse(read("package.json")).version;
const tauriVersion = JSON.parse(
  read("crates/stream-sync-desktop/tauri.conf.json")
).version;

const mismatches = [];
if (npmVersion !== cargoVersion) {
  mismatches.push(`package.json ${npmVersion} != Cargo ${cargoVersion}`);
}
if (tauriVersion !== cargoVersion) {
  mismatches.push(`tauri.conf.json ${tauriVersion} != Cargo ${cargoVersion}`);
}

const crates = [
  "crates/stream-sync-core/Cargo.toml",
  "crates/stream-sync-desktop/Cargo.toml",
  "crates/stream-sync-server/Cargo.toml",
];
for (const rel of crates) {
  if (!crateUsesWorkspaceVersion(rel)) {
    mismatches.push(`${rel} must set version.workspace = true`);
  }
}

if (mismatches.length) {
  console.error("Version consistency failed:");
  for (const line of mismatches) console.error(`  - ${line}`);
  process.exit(1);
}

console.log(`version ${cargoVersion} consistent (Cargo, npm, Tauri)`);
