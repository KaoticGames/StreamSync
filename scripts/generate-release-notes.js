#!/usr/bin/env node
"use strict";

const { execSync } = require("node:child_process");

const REPO = "KaoticGames/StreamSync";

/**
 * @param {string} tag e.g. v2.1.0
 * @returns {string}
 */
function generateReleaseNotes(tag) {
  const version = String(tag || "").replace(/^v/, "").trim();
  if (!version) {
    throw new Error("tag is required");
  }

  let commits = "";
  try {
    commits = execSync(
      `git log --pretty=format:"- %s (%h)" --no-merges $(git describe --tags --abbrev=0 2>/dev/null || echo HEAD~20)..HEAD`,
      { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }
    ).trim();
  } catch {
    commits = "";
  }

  const lines = [
    `# Stream Sync ${version}`,
    "",
    `Windows x86_64 NSIS installer for Stream Sync ${version}.`,
    "",
    "## Downloads",
    "",
    `- **Installer:** https://github.com/${REPO}/releases/download/${tag}/StreamSync-windows-x86_64-setup.exe`,
    `- **Checksums:** https://github.com/${REPO}/releases/download/${tag}/SHA256SUMS.txt`,
    "",
    "## Smoke checklist",
    "",
    "Before publishing, complete [docs/INSTALLER_SMOKE.md](../docs/INSTALLER_SMOKE.md) on the draft release asset.",
    "",
  ];

  if (commits) {
    lines.push("## Changes since previous tag", "", commits, "");
  }

  return lines.join("\n");
}

module.exports = { generateReleaseNotes };

if (require.main === module) {
  const tag = process.argv[2];
  if (!tag) {
    console.error("usage: node scripts/generate-release-notes.js <tag>");
    process.exit(2);
  }
  process.stdout.write(generateReleaseNotes(tag));
}
