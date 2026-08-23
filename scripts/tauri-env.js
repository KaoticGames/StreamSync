// Shared cargo PATH helpers for Tauri scripts (Windows often has cargo off PATH for npx).
const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawnSync } = require("child_process");

function resolveCargoBin() {
  const candidates = [];
  if (process.env.CARGO_HOME) {
    candidates.push(path.join(process.env.CARGO_HOME, "bin"));
  }
  candidates.push(path.join(os.homedir(), ".cargo", "bin"));
  if (process.env.USERPROFILE) {
    candidates.push(path.join(process.env.USERPROFILE, ".cargo", "bin"));
  }
  const exe = process.platform === "win32" ? "cargo.exe" : "cargo";
  for (const dir of candidates) {
    const full = path.join(dir, exe);
    if (fs.existsSync(full)) {
      return { binDir: dir, cargoExe: full };
    }
  }
  return null;
}

/**
 * Build a child env with cargo on PATH.
 * On Windows, process.env may expose Path while we also set PATH — duplicate
 * keys break CreateProcess lookup, so keep a single canonical key.
 */
function envWithCargo(cargo) {
  const env = { ...process.env };
  const sep = path.delimiter;
  const pathKey =
    process.platform === "win32"
      ? Object.keys(env).find((k) => k.toLowerCase() === "path") || "Path"
      : "PATH";
  const current = env[pathKey] || env.PATH || env.Path || "";
  for (const k of Object.keys(env)) {
    if (k.toLowerCase() === "path") delete env[k];
  }
  const parts = current
    .split(sep)
    .filter(Boolean)
    .filter((p) => !cargo || path.resolve(p) !== path.resolve(cargo.binDir));
  if (cargo) parts.unshift(cargo.binDir);
  env[pathKey] = parts.join(sep);
  return env;
}

function requireCargoEnv() {
  const cargo = resolveCargoBin();
  if (!cargo) {
    console.error(
      "cargo not found under %USERPROFILE%\\.cargo\\bin (or CARGO_HOME).\n" +
        "Install Rust from https://rustup.rs/ then reopen this terminal."
    );
    process.exit(1);
  }

  const env = envWithCargo(cargo);
  const cargoCheck = spawnSync(cargo.cargoExe, ["--version"], {
    env,
    encoding: "utf8",
    windowsHide: true,
  });
  if (cargoCheck.error || cargoCheck.status !== 0) {
    console.error("Failed to run cargo at:", cargo.cargoExe);
    if (cargoCheck.error) console.error(cargoCheck.error.message);
    if (cargoCheck.stderr) console.error(cargoCheck.stderr.trim());
    process.exit(1);
  }

  return {
    cargo,
    env,
    versionLine: cargoCheck.stdout.trim(),
    desktopDir: path.join(__dirname, "..", "crates", "stream-sync-desktop"),
    npx: process.platform === "win32" ? "npx.cmd" : "npx",
  };
}

function runTauri(args, label) {
  const { cargo, env, versionLine, desktopDir, npx } = requireCargoEnv();
  console.log(`[${label}] using ${versionLine} (${cargo.cargoExe})`);

  const result = spawnSync(npx, ["tauri", ...args], {
    cwd: desktopDir,
    stdio: "inherit",
    shell: true,
    env,
    windowsHide: true,
  });

  process.exit(result.status ?? 1);
}

module.exports = { requireCargoEnv, runTauri };
