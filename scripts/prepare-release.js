#!/usr/bin/env node
/**
 * Copies rust/.env → config/bundled.env for installer builds.
 * Run automatically via `npm run build`; safe to run manually before release.
 */
const fs = require("fs");
const path = require("path");

const rustRoot = path.resolve(__dirname, "..");
const src = path.join(rustRoot, ".env");
const dest = path.join(rustRoot, "config", "bundled.env");

if (!fs.existsSync(src)) {
  console.error(
    "[prepare-release] Missing rust/.env — copy config/env.example and fill in Twitch + update values."
  );
  process.exit(1);
}

fs.mkdirSync(path.dirname(dest), { recursive: true });
fs.copyFileSync(src, dest);
console.log("[prepare-release] Wrote config/bundled.env from .env");
