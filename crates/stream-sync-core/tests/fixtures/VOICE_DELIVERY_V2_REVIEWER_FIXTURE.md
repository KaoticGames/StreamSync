# Voice delivery v2 reviewer fixture (recovery report)

Import this JSON into ChatBot `voice-delivery-v2-protocol` tests unchanged.

- **Path (StreamSync):** `crates/stream-sync-core/tests/fixtures/voice_delivery_v2_finalized_manifest_with_recovery_report_reviewer.json`
- **ChatBot TS:** `parseVoiceDeliveryFinalizedManifest`, then `canonicalizeFinalizedManifestForDigest` / `computeFinalizedManifestDigest` from `packages/voice-delivery-v2-protocol/src/finalizedManifest.ts`
- **Expected digest:** `6c6d8f7c4b5fe3d9db6490c6b966038c72f05961ee6c3dc2daa931670efd135a`
- **Canonical `recoveryReport` order:** appended after `"version":1` in the outer object; inner keys `byteLength`, `contentType`, `relativePath`, `sha256`.

Verify locally:

```bash
node scripts/verify-voice-delivery-v2-reviewer-fixture.mjs
```

Rust unit test: `cross_language_golden_manifest_with_recovery_report_reviewer` in `finalized_manifest.rs`.
