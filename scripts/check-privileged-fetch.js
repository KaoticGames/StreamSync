#!/usr/bin/env node
/**
 * Fail if privileged Stream Sync API paths are called with plain fetch()
 * instead of streamSyncControlApi.privilegedFetch / privilegedFetch helper.
 */
const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const SKIP_DIRS = new Set([".git", "legacy", "node_modules", "target"]);
function sourceFiles(dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      if (!SKIP_DIRS.has(entry.name)) out.push(...sourceFiles(path.join(dir, entry.name)));
    } else if (/\.(?:js|html)$/i.test(entry.name)) {
      out.push(path.join(dir, entry.name));
    }
  }
  return out;
}

const PLAIN_API_FETCH =
  /\bfetch\s*\(\s*([`'"])([^`'"]*\/api\/(?:status|twitch\/|kick\/|streamelements\/|chat\/dock-config|chat\/overlay-config|chat\/upload-font|events\/dock-config|events\/overlay-config|events\/upload-media|events\/test-alert|dock\/)[^`'"]*)\1/gs;
const PUBLIC_GET =
  /\/api\/(?:chat\/overlay-(?:config|profiles)|events\/(?:overlay-(?:config|profiles)|dock-config)|twitch\/(?:badges|emotes))/;

function fetchCall(text, start) {
  let depth = 0;
  let quote = "";
  let escaped = false;
  for (let i = start; i < text.length; i += 1) {
    const ch = text[i];
    if (quote) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === quote) quote = "";
      continue;
    }
    if (ch === "'" || ch === '"' || ch === "`") {
      quote = ch;
    } else if (ch === "(") {
      depth += 1;
    } else if (ch === ")" && --depth === 0) {
      return text.slice(start, i + 1);
    }
  }
  return text.slice(start);
}

let failed = false;
for (const full of sourceFiles(ROOT)) {
  const rel = path.relative(ROOT, full);
  const text = fs.readFileSync(full, "utf8");
  for (const match of text.matchAll(PLAIN_API_FETCH)) {
    const start = match.index || 0;
    const call = fetchCall(text, start);
    const line = text.slice(0, start).split(/\r?\n/).length;
    const isPublicGet =
      PUBLIC_GET.test(match[2]) && !/method\s*:\s*["'](?:POST|DELETE|PUT|PATCH)["']/i.test(call);
    const isNonceCompletion = /x-streamsync-login-nonce/.test(call);
    if (isPublicGet || isNonceCompletion) continue;
    console.error(`${rel}:${line}: privileged plain fetch: ${match[2]}`);
    failed = true;
  }
}

if (failed) {
  process.exit(1);
}
console.log("privileged-fetch static check ok");
