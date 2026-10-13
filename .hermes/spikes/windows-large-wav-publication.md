# Phase 0C — Large WAV durable publication feasibility gate

| Field | Value |
|-------|-------|
| **Plan** | `2026-09-23_162634-syndicate-opus-finalization-streamsync-delivery` § Phase 0 / Slice 9 |
| **Branch** | `feat/finalized-voice-delivery-v2` |
| **Baseline** | `1fe796e0ec20136cb0b7104a3213e8b4e8a1d059` |
| **Spike module** | **SUPERSEDED / deleted** — reviewed pure logic ported to `voice_delivery/{bounds,wav,manifest,hash}`; durable state in `records/`, `ingest/partial`, `marker` (slices 6–9; second review pass from `a67a880`, staging under final-parent via `DeliverySessionGuard::begin`) |
| **Linux gate** | **PENDING** (slice 6–9 unit tests GREEN on host; not full GO — no slice 10/11, no rename) |
| **Windows gate** | **PENDING** (isolated API cross-check only; runtime via `phase0c-windows` after push) |

## Independent review correction pass

| Finding | Fix |
|---------|-----|
| Unstable `MetadataExt::volume_serial_number` / `GetLastError` outside `unsafe` | Stable `GetVolumeInformationW` volume serial; `GetLastError` only inside `unsafe` blocks |
| `exists()` + `rename` TOCTOU on directory publish | Linux `renameat2(RENAME_NOREPLACE)`; macOS `renamex_np(RENAME_EXCL)`; Windows `MoveFileExW` without replace + `ERROR_ALREADY_EXISTS` mapping |
| Missing pre-rename staging fsync + dual parent sync | `fsync_staging_for_publication` + sync source/destination parents after rename (injectable fault points) |
| Ledger ancestry not durable | `durable_create_dir_all` fsyncs each new directory and parent |
| No publication lock / ledger overwrite guard | `fs2` exclusive `publication.lock`; `assert_ledger_transition_allowed` before replace |
| `recover(Published)` / `assert_destination_available` too weak | Full final/stem/path/state/content validation; staging+final ambiguity fails closed |
| Partial resume inferred from file length | Sidecar `.checkpoint` binds expected len, durable contiguous len, resume binding, prefix SHA-256; advance only on `fsync` |
| Partial WAV verify | `validate_canonical_pcm_wav_header` (44-byte stereo PCM @ 48 kHz + RIFF/file length) |
| RIFF max not frame-aligned | `RIFF_MAX_CHUNK_BYTES = 4_294_967_256`; reject unaligned data |
| Session membership / traversal | `validate_stem_manifest` + exact directory membership; symlink rejection |
| `chunk_size == 0` | Immediate `InvalidChunkSize` on hash/stream/synthetic helpers |

## RED / GREEN evidence (Linux, 2026-09-24)

| Step | Command | Result |
|------|---------|--------|
| **RED (pre-fix)** | Independent review on `b44a08d` | **BLOCKED** — TOCTOU publish, length-based partial resume, weak ledger/recover validation, unstable Windows metadata API |
| **GREEN (post-fix)** | `cargo test -p stream-sync-core discord_voice_delivery -- --nocapture` | **30 passed**, **1 ignored** (`manual_stress_sparse_three_gib_synthetic_hash`) |
| **fmt** | `cargo fmt --all -- --check` | **pass** |
| **compile (Linux)** | `cargo check -p stream-sync-core` | **pass** |
| **whitespace** | `git diff --check` | **pass** |
| **Windows API (isolated)** | `cargo check -p stream-sync-windows-fs --target x86_64-pc-windows-msvc` | **pass** (no `ring` / no full crate link) |
| **Windows full crate** | `cargo check -p stream-sync-core --target x86_64-pc-windows-msvc` | **blocked on this host** — `ring` build requires MSVC `lib.exe` (not installed) |

### Focused test inventory (added/changed)

| Area | Tests |
|------|-------|
| RIFF / bounds | `riff_max_data_bytes_exact_limit_and_reject_one_over`, `plan_duration_bounds_match_unsigned_expectations` |
| Partial checkpoint | `partial_writer_valid_resume_requires_checkpoint_not_length`, `partial_writer_rejects_corrupt_checkpoint_prefix`, `partial_writer_rejects_foreign_binding_and_sparse_tail`, `partial_writer_rejects_bytes_without_checkpoint` |
| Chunk size | `chunk_size_zero_rejected_by_hash_and_synthetic_helpers` |
| WAV canonical | `canonical_wav_header_mutations_fail_verification` |
| Stems / membership | `stem_manifest_rejects_empty_duplicate_and_traversal`, `verify_all_stems_rejects_unexpected_directory_entry`, `verify_all_stems_rejects_symlink_escape` |
| Publish / concurrency | `concurrent_publish_exactly_one_wins`, `publish_never_replaces_existing_destination_bytes`, `publication_lock_denies_conflicting_owner` |
| Ledger / recover | `recover_published_requires_final_stems`, `assert_destination_requires_published_state_and_matching_bytes`, `publication_durability_fault_points_are_ordered` |
| Existing gates | crash/recovery, ledger atomic replace, streaming hash, multi-stem publish, sparse header read, >3 GiB logical offset |

## Windows implementation notes

- **Directory publish:** `MoveFileExW` + `MOVEFILE_WRITE_THROUGH` only; destination must not exist.
- **Ledger file replace:** `MOVEFILE_WRITE_THROUGH | MOVEFILE_REPLACE_EXISTING`.
- **Volume identity:** `GetVolumeInformationW` on nearest existing path prefix.
- **Directory durability (Windows):** `FlushFileBuffers` via `CreateFileW` + `FILE_FLAG_BACKUP_SEMANTICS` on parent paths — **not** power-loss proven on this host.

## Required acceptance still pending

1. **Windows hardware:** `cargo test -p stream-sync-core discord_voice_delivery` on Windows (including `windows_publish_directory_smoke`).
2. **Full crate Windows cross-compile** on a host with MSVC `lib.exe` (or CI Windows runner).
3. **Manual >3 GiB stress:** `cargo test -p stream-sync-core manual_stress_sparse_three_gib_synthetic_hash -- --ignored`.

```powershell
cd C:\path\to\streamsync-voice-delivery-v2
git checkout feat/finalized-voice-delivery-v2
cargo test -p stream-sync-core discord_voice_delivery -- --nocapture
cargo test -p stream-sync-core discord_voice_delivery::tests::windows_publish_directory_smoke -- --nocapture
cargo test -p stream-sync-core manual_stress_sparse_three_gib_synthetic_hash -- --ignored --nocapture
```

## Verdict

| Platform | Verdict |
|----------|---------|
| Linux filesystem primitives (corrected) | **PENDING** — tests green; await plan Phase A sign-off after Windows + manual stress |
| Windows runtime + full crate build | **PENDING** |
| Manual >3 GiB streaming hash | **PENDING** (ignored test) |
