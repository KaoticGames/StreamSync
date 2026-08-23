// Release build for Stream Sync (Tauri NSIS). Prefers the same cargo PATH fix as `npm run dev`.
// Run from rust/: `npm run build` (which also runs prepare-release first).
const { runTauri } = require("./tauri-env");

runTauri(["build"], "build");
