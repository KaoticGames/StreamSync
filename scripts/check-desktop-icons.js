#!/usr/bin/env node
/** Fail when Tauri is configured with missing or unusably tiny desktop icons. */
const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const TAURI_DIR = path.join(ROOT, "crates", "stream-sync-desktop");
const TAURI_CONFIG = path.join(TAURI_DIR, "tauri.conf.json");

function pngDimensions(bytes) {
  const signature = "89504e470d0a1a0a";
  if (bytes.length < 24 || bytes.subarray(0, 8).toString("hex") !== signature) {
    throw new Error("invalid PNG header");
  }
  return [{ width: bytes.readUInt32BE(16), height: bytes.readUInt32BE(20) }];
}

function icoDimensions(bytes) {
  if (bytes.length < 6 || bytes.readUInt16LE(0) !== 0 || bytes.readUInt16LE(2) !== 1) {
    throw new Error("invalid ICO header");
  }
  const count = bytes.readUInt16LE(4);
  if (count < 1 || bytes.length < 6 + count * 16) {
    throw new Error("invalid ICO directory");
  }
  const dimensions = [];
  for (let i = 0; i < count; i += 1) {
    const offset = 6 + i * 16;
    dimensions.push({
      width: bytes[offset] || 256,
      height: bytes[offset + 1] || 256,
    });
  }
  return dimensions;
}

function iconDimensions(iconPath, bytes) {
  switch (path.extname(iconPath).toLowerCase()) {
    case ".png":
      return pngDimensions(bytes);
    case ".ico":
      return icoDimensions(bytes);
    default:
      throw new Error("unsupported icon format; expected .png or .ico");
  }
}

const config = JSON.parse(fs.readFileSync(TAURI_CONFIG, "utf8"));
const configuredIcons = config?.bundle?.icon;
const failures = [];

if (!Array.isArray(configuredIcons) || configuredIcons.length === 0) {
  failures.push("bundle.icon must contain at least one icon path");
} else {
  for (const configuredPath of configuredIcons) {
    const absolutePath = path.resolve(TAURI_DIR, configuredPath);
    if (!fs.existsSync(absolutePath)) {
      failures.push(`${configuredPath}: file does not exist`);
      continue;
    }
    try {
      const dimensions = iconDimensions(configuredPath, fs.readFileSync(absolutePath));
      const largest = dimensions.reduce(
        (best, current) =>
          current.width * current.height > best.width * best.height ? current : best,
        dimensions[0]
      );
      if (largest.width < 32 || largest.height < 32) {
        failures.push(
          `${configuredPath}: largest image is ${largest.width}x${largest.height}; expected at least 32x32`
        );
      }
    } catch (error) {
      failures.push(`${configuredPath}: ${error.message}`);
    }
  }
}

if (failures.length) {
  console.error("Desktop icon validation failed:");
  for (const failure of failures) console.error(`  - ${failure}`);
  process.exit(1);
}

console.log(`desktop icon validation ok (${configuredIcons.join(", ")})`);
