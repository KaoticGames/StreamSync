#!/usr/bin/env node
/**
 * Self-contained reviewer fixture gate: reproduces documented Syndicate finalized-manifest
 * canonical JSON key order (matches StreamSync Rust + ChatBot TS contract) without importing
 * an external ChatBot checkout.
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

const expectedCanonical =
  '{"channelId":"234567890123456789","guildId":"123456789012345678","journalFormatVersion":2,"journalSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sealedStopMonotonicNs":"200","sealedStopWallMs":2,"sessionId":"550e8400-e29b-41d4-a716-446655440000","sessionStartMonotonicNs":"100","sessionStartWallMs":1,"stems":[{"discordUserId":"123456789012345678","pathNick":"user1","sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","wavBytes":44,"wavRelativePath":"user1.wav"}],"targetSampleCount48k":0,"version":1,"recoveryReport":{"byteLength":64,"contentType":"text/plain","relativePath":"Syndicate Audio Recovery Report.txt","sha256":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}}';

function compareAscii(a, b) {
  const ab = Buffer.from(a, "utf8");
  const bb = Buffer.from(b, "utf8");
  const len = Math.min(ab.length, bb.length);
  for (let i = 0; i < len; i++) {
    if (ab[i] !== bb[i]) return ab[i] - bb[i];
  }
  return ab.length - bb.length;
}

/** Documented TS/Rust canonical order for finalized manifest digest. */
function canonicalizeFinalizedManifestForDigest(raw) {
  const stems = raw.stems.map((s) => ({
    discordUserId: s.discordUserId,
    pathNick: s.pathNick,
    sha256: s.sha256,
    wavBytes: s.wavBytes,
    wavRelativePath: s.wavRelativePath,
  }));
  stems.sort((a, b) => compareAscii(a.pathNick, b.pathNick));
  const base = {
    channelId: raw.channelId,
    guildId: raw.guildId,
    journalFormatVersion: raw.journalFormatVersion,
    journalSha256: raw.journalSha256,
    sealedStopMonotonicNs: raw.sealedStopMonotonicNs,
    sealedStopWallMs: raw.sealedStopWallMs,
    sessionId: raw.sessionId,
    sessionStartMonotonicNs: raw.sessionStartMonotonicNs,
    sessionStartWallMs: raw.sessionStartWallMs,
    stems,
    targetSampleCount48k: raw.targetSampleCount48k,
    version: raw.version,
  };
  let canonical = JSON.stringify(base);
  if (raw.recoveryReport) {
    const report = {
      byteLength: raw.recoveryReport.byteLength,
      contentType: raw.recoveryReport.contentType,
      relativePath: raw.recoveryReport.relativePath,
      sha256: raw.recoveryReport.sha256,
    };
    const reportSerialized = JSON.stringify(report);
    canonical =
      canonical.slice(0, -1) + ',"recoveryReport":' + reportSerialized + "}";
  }
  return canonical;
}

function computeFinalizedManifestDigest(canonical) {
  return crypto.createHash("sha256").update(canonical, "utf8").digest("hex");
}

const fixtureBytes = fs.readFileSync(fixturePath);
const raw = JSON.parse(fixtureBytes.toString("utf8"));
const canonical = canonicalizeFinalizedManifestForDigest(raw);
const digest = computeFinalizedManifestDigest(canonical);
const fixtureSha256 = crypto.createHash("sha256").update(fixtureBytes).digest("hex");

console.log("fixture_path:", fixturePath);
console.log("fixture_bytes_sha256:", fixtureSha256);
console.log("verifier:", "self-contained documented canonical order (not ChatBot runtime)");
console.log("canonical:", canonical);
console.log("digest:", digest);

if (canonical !== expectedCanonical) {
  console.error("canonical mismatch vs committed golden string");
  process.exit(1);
}

if (digest !== expectedDigest) {
  console.error(`digest mismatch: expected ${expectedDigest}`);
  process.exit(1);
}

if (!canonical.includes('"version":1,"recoveryReport":')) {
  console.error("canonical must append recoveryReport immediately after version");
  process.exit(1);
}

console.log("ok: reviewer fixture canonical/digest gate");
