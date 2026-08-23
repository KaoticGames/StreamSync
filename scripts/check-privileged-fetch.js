#!/usr/bin/env node
/**
 * Fail if privileged Stream Sync API paths are called with plain fetch()
 * instead of streamSyncControlApi.privilegedFetch / privilegedFetch helper.
 */
const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const FILES = [
  "renderer.js",
  "tauri-bridge.js",
  "connections-api.js",
  "chat-dock-config.js",
  "chat-overlay-config.js",
  "events-dock-config.js",
  "events-se-import.js",
  "overlay-server/events-studio.html",
];

const PRIVILEGED_HINT =
  /fetch\s*\(\s*[`'"][^`'"]*\/api\/(status|twitch\/|kick\/|streamelements\/|chat\/dock-config|chat\/overlay-config|chat\/upload-font|events\/dock-config|events\/overlay-config|events\/upload-media|events\/test-alert|dock\/)/;

const ALLOW_PUBLIC_GET =
  /fetch\s*\(\s*[`'"][^`'"]*\/api\/(chat\/overlay-config|chat\/overlay-profiles|events\/overlay-config|events\/overlay-profiles|events\/dock-config|twitch\/badges|twitch\/emotes)/;

let failed = false;
for (const rel of FILES) {
  const full = path.join(ROOT, rel);
  if (!fs.existsSync(full)) {
    console.error("missing", rel);
    failed = true;
    continue;
  }
  const text = fs.readFileSync(full, "utf8");
  const lines = text.split(/\r?\n/);
  lines.forEach((line, i) => {
    if (!PRIVILEGED_HINT.test(line)) return;
    if (ALLOW_PUBLIC_GET.test(line) && !/\b(POST|DELETE|method:\s*['"]POST|method:\s*['"]DELETE)/i.test(line)) {
      return;
    }
    // Allowed if same line already uses privilegedFetch wrapper variable assignment context
    if (/privilegedFetch\s*\(/.test(line)) return;
    console.error(`${rel}:${i + 1}: privileged plain fetch: ${line.trim()}`);
    failed = true;
  });
}

if (failed) {
  process.exit(1);
}
console.log("privileged-fetch static check ok");
