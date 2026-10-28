"use strict";

/**
 * Map canonical workspace semver (e.g. 2.1.1) to the padded human UI label (e.g. 2.1.001).
 * Patch 0 keeps the legacy display form (2.1.0); patch >= 1 uses a three-digit patch segment.
 * @param {string} canonical
 * @returns {string}
 */
function canonicalToDisplayLabel(canonical) {
  const trimmed = String(canonical || "").trim();
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(trimmed);
  if (!match) {
    throw new Error(`invalid canonical version "${canonical}"`);
  }
  const major = match[1];
  const minor = match[2];
  const patch = parseInt(match[3], 10);
  if (patch === 0) {
    return `${major}.${minor}.0`;
  }
  return `${major}.${minor}.${String(patch).padStart(3, "0")}`;
}

/** @param {string} canonical */
function uiVersionTag(canonical) {
  return `v${canonicalToDisplayLabel(canonical)}`;
}

/** Alias for version-consistency checks. */
const expectedUiVersionTag = uiVersionTag;

module.exports = {
  canonicalToDisplayLabel,
  uiVersionTag,
  expectedUiVersionTag,
};
