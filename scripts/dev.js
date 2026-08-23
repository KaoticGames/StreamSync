// Launch Stream Sync Tauri desktop from the rust/ workspace (no parent repo paths).
//
// Default: `tauri dev --no-watch` so the app does not rebuild/restart when the
// filesystem reports spurious .rs mtime changes (common on this machine ~every
// 10 minutes). Use `npm run dev:watch` when you want Rust hot-reload.
const { runTauri } = require("./tauri-env");

const watch =
  process.argv.includes("--watch") ||
  process.env.STREAMSYNC_DEV_WATCH === "1" ||
  process.env.STREAMSYNC_DEV_WATCH === "true";

const args = watch ? ["dev"] : ["dev", "--no-watch"];
if (!watch) {
  console.log(
    "[dev] file watcher off (stable runs). Use `npm run dev:watch` to rebuild on Rust edits."
  );
}

runTauri(args, "dev");
