#!/usr/bin/env node
"use strict";

const fs = require("node:fs");

const ALLOWED_HOSTS = new Set(["github.com", "objects.githubusercontent.com"]);

/**
 * @param {object} input
 * @param {string} input.version
 * @param {string} input.assetUrl
 * @param {string} input.signature
 * @param {string} [input.notes]
 * @param {string} [input.pubDate]
 * @returns {object}
 */
function generateLatestJson({
  version,
  assetUrl,
  signature,
  notes = "",
  pubDate = new Date().toISOString(),
}) {
  const v = String(version || "").trim();
  if (!/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/.test(v)) {
    throw new Error(`invalid semver version: ${version}`);
  }

  const url = new URL(String(assetUrl || ""));
  if (url.protocol !== "https:") {
    throw new Error("asset URL must use https");
  }
  if (!ALLOWED_HOSTS.has(url.hostname)) {
    throw new Error(`unexpected asset host: ${url.hostname}`);
  }

  const sig = String(signature || "").trim();
  if (!sig) {
    throw new Error("signature is required");
  }

  return {
    version: v,
    notes: String(notes || ""),
    pub_date: pubDate,
    platforms: {
      "windows-x86_64": {
        url: url.toString(),
        signature: sig,
      },
    },
  };
}

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--version") out.version = argv[++i];
    else if (arg === "--tag") out.tag = argv[++i];
    else if (arg === "--asset-url") out.assetUrl = argv[++i];
    else if (arg === "--sig-file") out.sigFile = argv[++i];
    else if (arg === "--notes-file") out.notesFile = argv[++i];
    else if (arg === "--pub-date") out.pubDate = argv[++i];
  }
  return out;
}

module.exports = { generateLatestJson, ALLOWED_HOSTS };

if (require.main === module) {
  const args = parseArgs(process.argv.slice(2));
  if (!args.version || !args.assetUrl || !args.sigFile) {
    console.error(
      "usage: node scripts/generate-latest-json.js --version X.Y.Z --asset-url URL --sig-file path [--notes-file path]"
    );
    process.exit(2);
  }
  const signature = fs.readFileSync(args.sigFile, "utf8").trim();
  const notes = args.notesFile
    ? fs.readFileSync(args.notesFile, "utf8").trim()
    : "";
  const json = generateLatestJson({
    version: args.version,
    assetUrl: args.assetUrl,
    signature,
    notes,
    pubDate: args.pubDate,
  });
  process.stdout.write(`${JSON.stringify(json, null, 2)}\n`);
}
