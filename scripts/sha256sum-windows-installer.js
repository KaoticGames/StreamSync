#!/usr/bin/env node
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

const CANONICAL_NAME = "StreamSync-windows-x86_64-setup.exe";

/**
 * @param {string} installerPath
 * @param {string} [assetName]
 * @returns {string}
 */
function sha256sumWindowsInstaller(installerPath, assetName = CANONICAL_NAME) {
  const resolved = path.resolve(installerPath);
  const bytes = fs.readFileSync(resolved);
  const digest = crypto.createHash("sha256").update(bytes).digest("hex");
  return `${digest}  ${assetName}\n`;
}

module.exports = { sha256sumWindowsInstaller, CANONICAL_NAME };

if (require.main === module) {
  const installerPath = process.argv[2];
  if (!installerPath) {
    console.error(
      "usage: node scripts/sha256sum-windows-installer.js <installer.exe>"
    );
    process.exit(2);
  }
  process.stdout.write(sha256sumWindowsInstaller(installerPath));
}
