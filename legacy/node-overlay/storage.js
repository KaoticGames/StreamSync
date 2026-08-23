// overlay-server/storage.js
// Single source of truth for ALL writable paths + robust read/write helpers.
// Goals:
// - Never write inside app.asar
// - Always self-heal missing/corrupt files by restoring defaults
// - Atomic writes to avoid corruption on crashes/power loss
// - Windows-safe replace logic (rename-over-existing is fragile on Windows)

const fs = require("fs");
const path = require("path");
const os = require("os");

function nowTs() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function ensureDir(dirPath) {
  try {
    fs.mkdirSync(dirPath, { recursive: true });
  } catch (_) {}
}

function fileExists(p) {
  try {
    fs.accessSync(p, fs.constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

function safeUnlink(p) {
  try {
    if (fileExists(p)) fs.unlinkSync(p);
    return true;
  } catch {
    return false;
  }
}

function safeRename(from, to) {
  try {
    fs.renameSync(from, to);
    return true;
  } catch {
    return false;
  }
}

function safeCopy(from, to) {
  try {
    fs.copyFileSync(from, to);
    return true;
  } catch {
    return false;
  }
}

function looksLikeAsarPath(p) {
  // Windows packaged: ...\resources\app.asar\...
  // macOS packaged: .../Contents/Resources/app.asar/...
  const s = String(p || "").toLowerCase();
  return s.includes("app.asar");
}

function assertWritableRoot(root) {
  const r = path.resolve(String(root || ""));
  if (!r) throw new Error("Invalid storage root.");

  if (looksLikeAsarPath(r)) {
    throw new Error(
      `Storage root points inside app.asar (read-only): ${r}\n` +
        `This would break persistence. STREAMSYNC_USERDATA must be a user-writable directory.`
    );
  }

  ensureDir(r);

  // Quick write test (best-effort). If it fails, we still want to surface it loudly.
  const probe = path.join(r, `.writetest-${process.pid}-${Date.now()}.tmp`);
  try {
    fs.writeFileSync(probe, "ok", "utf8");
    fs.unlinkSync(probe);
  } catch (err) {
    throw new Error(`Storage root is not writable: ${r} (${err?.message || err})`);
  }

  return r;
}

// Atomic write: write to temp, fsync, then replace target.
// Windows notes: rename-over-existing can fail, so we do a safe replace strategy.
function writeFileAtomic(targetPath, data, encoding = "utf8") {
  const dir = path.dirname(targetPath);
  ensureDir(dir);

  const tmpPath = `${targetPath}.tmp-${process.pid}-${Date.now()}`;
  const fd = fs.openSync(tmpPath, "w");

  try {
    fs.writeFileSync(fd, data, { encoding });
    fs.fsyncSync(fd);
  } finally {
    try {
      fs.closeSync(fd);
    } catch {}
  }

  // Best-effort backup rotation
  const bak = `${targetPath}.bak`;
  try {
    if (fileExists(bak)) fs.unlinkSync(bak);
  } catch {}

  // If target exists, try to move it out of the way (preferred),
  // otherwise we may have to delete it to allow replace.
  if (fileExists(targetPath)) {
    // move current to .bak (may fail if locked)
    safeRename(targetPath, bak);
  }

  // Attempt replace
  try {
    fs.renameSync(tmpPath, targetPath);
    return;
  } catch (err) {
    // If target still exists, try unlink then rename
    safeUnlink(targetPath);
    try {
      fs.renameSync(tmpPath, targetPath);
      return;
    } catch (err2) {
      // Last-resort: copy then delete tmp
      if (!safeCopy(tmpPath, targetPath)) {
        // If we can't even copy, throw the most relevant error
        throw err2 || err;
      }
      safeUnlink(tmpPath);
      return;
    }
  }
}

// Read JSON with self-heal:
// - If missing => write default and return it
// - If corrupt => rename to *.corrupt-<ts>, write default, return default
function readJsonOrDefault(filePath, defaultValue) {
  ensureDir(path.dirname(filePath));

  if (!fileExists(filePath)) {
    try {
      writeFileAtomic(filePath, JSON.stringify(defaultValue, null, 2), "utf8");
    } catch (_) {}
    return defaultValue;
  }

  try {
    const raw = fs.readFileSync(filePath, "utf8");
    const parsed = JSON.parse(raw);

    // Accept objects or arrays, reject null/primitive
    if (parsed === null || typeof parsed !== "object") {
      throw new Error("Invalid JSON root type");
    }

    return parsed;
  } catch (err) {
    // quarantine corrupt file (rename may fail if locked; copy as fallback)
    const corruptPath = `${filePath}.corrupt-${nowTs()}`;
    if (!safeRename(filePath, corruptPath)) {
      // If rename fails, try copy+unlink
      safeCopy(filePath, corruptPath);
      safeUnlink(filePath);
    }

    // recreate default
    try {
      writeFileAtomic(filePath, JSON.stringify(defaultValue, null, 2), "utf8");
    } catch (_) {}

    return defaultValue;
  }
}

function writeJson(filePath, value) {
  writeFileAtomic(filePath, JSON.stringify(value, null, 2), "utf8");
}

// Resolve Stream Sync root (userData) from env.
// BORING-STABLE RULE: We must NEVER fall back to an asar-relative directory.
// If STREAMSYNC_USERDATA is missing, we fall back to a user-writable folder in the home dir.
// (This helps dev/test tools and prevents accidental writes to the app directory.)
function getUserDataRoot() {
  const envRoot = process.env.STREAMSYNC_USERDATA;

  // Safe fallback (ONLY if envRoot missing). This should never happen in packaged app if main.js is correct.
  const fallbackRoot = path.join(os.homedir(), ".stream-sync");

  const root = envRoot && String(envRoot).trim() ? envRoot : fallbackRoot;
  return assertWritableRoot(root);
}

// All paths (future-proofed)
function getPaths() {
  const root = getUserDataRoot();

  const paths = {
    root,

    // Configs
    dockConfig:
      process.env.STREAMSYNC_DOCK_CONFIG || path.join(root, "dock-config.json"),
    overlayConfig:
      process.env.STREAMSYNC_OVERLAY_CONFIG ||
      path.join(root, "overlay-config.json"),
    eventsOverlayConfig:
      process.env.STREAMSYNC_EVENTS_OVERLAY_CONFIG ||
      path.join(root, "events-overlay-config.json"),

    // Profiles (if/when you use it here)
    profiles: path.join(root, "profiles.json"),

    // Tokens (namespaced for multi-platform future)
    tokensDir: path.join(root, "tokens"),
    twitchTokens:
      process.env.STREAMSYNC_TOKENS_FILE || path.join(root, "tokens", "twitch.json"),

    // Uploaded assets
    fontsDir: process.env.STREAMSYNC_FONTS_DIR || path.join(root, "fonts"),
  };

  ensureDir(paths.tokensDir);
  ensureDir(paths.fontsDir);

  return paths;
}

module.exports = {
  getPaths,
  ensureDir,
  readJsonOrDefault,
  writeJson,
  writeFileAtomic,
};
