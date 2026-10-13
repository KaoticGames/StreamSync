#!/usr/bin/env node
/**
 * Cross-repo gate: verify the shared recovery-report reviewer fixture against ChatBot TS
 * `canonicalizeFinalizedManifestForDigest` + `computeFinalizedManifestDigest`.
 *
 * Usage:
 *   CHATBOT_VOICE_PROTOCOL_DIR=/path/to/chatbot/.../voice-delivery-v2-protocol \
 *     node scripts/verify-voice-delivery-v2-reviewer-fixture.mjs
 *
 * Default protocol dir (if unset): ../chatbot-tolerant-audio/packages/voice-delivery-v2-protocol
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fixtureRel =
  "crates/stream-sync-core/tests/fixtures/voice_delivery_v2_finalized_manifest_with_recovery_report_reviewer.json";
const fixturePath = path.join(repoRoot, fixtureRel);

const expectedDigest =
  "6c6d8f7c4b5fe3d9db6490c6b966038c72f05961ee6c3dc2daa931670efd135a";

const protocolDir =
  process.env.CHATBOT_VOICE_PROTOCOL_DIR ??
  path.resolve(repoRoot, "../chatbot-tolerant-audio/packages/voice-delivery-v2-protocol");

const fixtureBytes = fs.readFileSync(fixturePath);
const raw = JSON.parse(fixtureBytes.toString("utf8"));

const modPath = path.join(protocolDir, "dist/finalizedManifest.js");
if (!fs.existsSync(modPath)) {
  console.error(
    `ChatBot protocol not built at ${modPath}. Run: npm run build in ${protocolDir}`
  );
  process.exit(1);
}

const {
  parseVoiceDeliveryFinalizedManifest,
  canonicalizeFinalizedManifestForDigest,
  computeFinalizedManifestDigest,
} = await import(pathToFileUrl(modPath));

const manifest = parseVoiceDeliveryFinalizedManifest(raw);
const canonical = canonicalizeFinalizedManifestForDigest(manifest);
const digest = computeFinalizedManifestDigest(manifest);

const fixtureSha256 = crypto.createHash("sha256").update(fixtureBytes).digest("hex");

console.log("fixture_path:", fixturePath);
console.log("fixture_bytes_sha256:", fixtureSha256);
console.log("chatbot_module:", modPath);
console.log("ts_functions:", [
  "parseVoiceDeliveryFinalizedManifest",
  "canonicalizeFinalizedManifestForDigest",
  "computeFinalizedManifestDigest",
]);
console.log("canonical:", canonical);
console.log("digest:", digest);

if (digest !== expectedDigest) {
  console.error(`digest mismatch: expected ${expectedDigest}`);
  process.exit(1);
}

if (!canonical.includes('"version":1,"recoveryReport":')) {
  console.error("canonical must append recoveryReport immediately after version");
  process.exit(1);
}

console.log("ok: reviewer fixture matches ChatBot TS canonical digest");

function pathToFileUrl(p) {
  return new URL(`file://${p.replace(/\\/g, "/")}`).href;
}
