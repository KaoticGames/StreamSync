const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const root = path.join(__dirname, "..");

function read(relativePath) {
  return fs.readFileSync(path.join(root, relativePath), "utf8");
}

test("revenue candidate excludes the In-Sync customer surface", () => {
  const shell = read("shell.html");
  const shellCss = read("shell.css");
  const renderer = read("renderer.js");
  const bridge = read("tauri-bridge.js");
  const tauriConfig = read("crates/stream-sync-desktop/tauri.conf.json");
  const desktopCommands = read("crates/stream-sync-desktop/src/commands.rs");
  const desktopLib = read("crates/stream-sync-desktop/src/lib.rs");
  const desktopPermissions = read(
    "crates/stream-sync-desktop/permissions/stream-sync-commands.toml"
  );

  assert.doesNotMatch(shell, /data-view=["']in-sync["']/);
  assert.doesNotMatch(shell, /in-sync-recordings\.js/);
  assert.doesNotMatch(shellCss, /In-Sync \(recordings processor\)/);
  assert.doesNotMatch(shellCss, /\.view-in-sync|\.in-sync-/);
  assert.doesNotMatch(renderer, /views\/in-sync\.html/);
  assert.doesNotMatch(renderer, /wireInSyncRecordingsUi/);
  assert.doesNotMatch(bridge, /pickRecordingFolder|pick_recording_folder/);
  assert.doesNotMatch(tauriConfig, /in-sync-recordings\.js/);
  assert.doesNotMatch(
    desktopCommands,
    /pick_recording_folder|reveal_in_sync_output|open_in_sync_recovery_report/
  );
  assert.doesNotMatch(
    desktopLib,
    /pick_recording_folder|reveal_in_sync_output|open_in_sync_recovery_report/
  );
  assert.doesNotMatch(
    desktopPermissions,
    /pick_recording_folder|reveal_in_sync_output|open_in_sync_recovery_report/
  );
  assert.equal(fs.existsSync(path.join(root, "views", "in-sync.html")), false);
  assert.equal(fs.existsSync(path.join(root, "in-sync-recordings.js")), false);
});

test("revenue candidate excludes Discord recording setup and privileged recording routes", () => {
  const connectionsView = read("views/connections/twitch-connection.html");
  const connectionsApi = read("connections-api.js");
  const renderer = read("renderer.js");
  const proxy = read("crates/stream-sync-desktop/src/overlay_proxy.rs");
  const routes = read("crates/stream-sync-core/src/routes.rs");
  const routeManifest = read("crates/stream-sync-core/src/route_manifest.rs");

  assert.doesNotMatch(connectionsView, /Discord Voice Recordings/);
  assert.doesNotMatch(connectionsView, /discord-recording-folder/);
  assert.doesNotMatch(connectionsView, /discord-connect-key/);
  assert.doesNotMatch(connectionsApi, /discordSaveRecordingFolder/);
  assert.doesNotMatch(renderer, /discord-recording-folder/);
  assert.doesNotMatch(renderer, /btn-discord-pick-folder/);
  assert.doesNotMatch(proxy, /\/api\/discord\/recording-folder/);
  assert.doesNotMatch(proxy, /\/api\/discord\/connect-key\/redeem/);
  assert.doesNotMatch(proxy, /\/api\/in-sync\//);
  assert.doesNotMatch(routes, /\/api\/discord\/recording-folder/);
  assert.doesNotMatch(routes, /\/api\/discord\/connect-key\/redeem/);
  assert.doesNotMatch(routes, /\/api\/in-sync\//);
  assert.doesNotMatch(routeManifest, /\/api\/discord\/recording-folder/);
  assert.doesNotMatch(routeManifest, /\/api\/discord\/connect-key\/redeem/);
  assert.doesNotMatch(routeManifest, /\/api\/in-sync\//);
  assert.doesNotMatch(routes, /"discordVoice"|"inSync"/);
});

test("revenue candidate does not start recording workers", () => {
  const core = read("crates/stream-sync-core/src/lib.rs");
  const twitch = read("crates/stream-sync-core/src/twitch.rs");

  assert.doesNotMatch(core, /discord_voice::maybe_autostart/);
  assert.doesNotMatch(core, /in_sync_supervisor::ensure_in_sync_supervisor/);
  assert.doesNotMatch(twitch, /discord_voice::ensure_ingest_worker/);
});
