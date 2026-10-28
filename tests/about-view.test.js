"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { describe, it } = require("node:test");

const root = path.join(__dirname, "..");

function read(relativePath) {
  return fs.readFileSync(path.join(root, relativePath), "utf8");
}

function extractInitAboutViewBlock(rendererSource) {
  const marker = "function initAboutView()";
  const start = rendererSource.indexOf(marker);
  assert.notEqual(start, -1, "expected initAboutView in renderer.js");
  const openBrace = rendererSource.indexOf("{", start);
  assert.notEqual(openBrace, -1, "expected initAboutView body");
  let depth = 0;
  for (let i = openBrace; i < rendererSource.length; i += 1) {
    const ch = rendererSource[i];
    if (ch === "{") {
      depth += 1;
    } else if (ch === "}") {
      depth -= 1;
      if (depth === 0) {
        return rendererSource.slice(start, i + 1);
      }
    }
  }
  assert.fail("unbalanced braces in initAboutView");
}

function aboutVisibleText(html) {
  return html
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

describe("About view (kaoticgames.com/about summary)", () => {
  const aboutHtml = read("views/about.html");
  const aboutText = aboutVisibleText(aboutHtml);

  it("shows the Stream Sync v2.1.001 version label", () => {
    assert.match(aboutHtml, /Stream Sync v2\.1\.001/);
  });

  it("summarizes current Kaotic company themes in concise copy", () => {
    assert.match(aboutText, /Kaotic DNA/i);
    assert.match(aboutText, /different move/i);
    assert.match(aboutText, /independent studio/i);
    assert.match(aboutText, /R\s*&\s*D lab|R&D lab/i);
    assert.match(aboutText, /entertainment company/i);
    assert.match(aboutText, /people over profit/i);
    assert.match(aboutText, /leadership/i);
    assert.match(aboutText, /builders/i);
    assert.match(
      aboutText,
      /extend(?:ing)? human ability|extend(?:ing)? human/i
    );
    assert.match(aboutText, /Kaotic Labs/i);
    assert.match(aboutText, /fund(?:s|ing)?\s+independence|funds independence/i);
    assert.match(aboutText, /Syndicate AI/i);
    assert.match(aboutText, /Stream\s*Sync/i);
    assert.match(
      aboutText,
      /Early Access|ecosystem|flagship/i
    );
  });

  it("does not retain stale game titles, beta invite funnels, or absolute privacy claims", () => {
    assert.doesNotMatch(aboutText, /Guild of Greed/i);
    assert.doesNotMatch(aboutText, /Velion Mythera/i);
    assert.doesNotMatch(aboutText, /Chrono Junk/i);
    assert.doesNotMatch(aboutText, /closed beta/i);
    assert.doesNotMatch(aboutText, /request an invite|request invite/i);
    assert.doesNotMatch(aboutText, /harvesting their data/i);
    assert.doesNotMatch(aboutText, /no silent data capture/i);
  });

  it("keeps Syndicate, Kaotic Games, and Discord action buttons in HTML", () => {
    assert.match(aboutHtml, /id=["']btn-about-syndicate-site["']/);
    assert.match(aboutHtml, /id=["']btn-about-kaotic-site["']/);
    assert.match(aboutHtml, /id=["']btn-about-discord["']/);
    assert.match(aboutHtml, /Syndicate AI website/);
    assert.match(aboutHtml, /Kaotic Games website/);
    assert.match(aboutHtml, /Syndicate Discord/);
  });

  it("removes About-specific Twitch markup from views/about.html", () => {
    assert.doesNotMatch(aboutHtml, /id=["']btn-about-twitch["']/);
    assert.doesNotMatch(aboutHtml, /@fukiplays/i);
    assert.doesNotMatch(aboutHtml, /twitch\.tv\/fukiplays/i);
    assert.doesNotMatch(aboutHtml, /Twitch:\s*@fukiplays/i);
  });
});

describe("initAboutView renderer wiring", () => {
  const renderer = read("renderer.js");
  const initAboutView = extractInitAboutViewBlock(renderer);

  it("wires retained external links and Discord without About Twitch", () => {
    assert.match(initAboutView, /#btn-about-syndicate-site/);
    assert.match(initAboutView, /#btn-about-kaotic-site/);
    assert.match(initAboutView, /#btn-about-discord/);
    assert.match(initAboutView, /syndicateai\.net/);
    assert.match(initAboutView, /kaoticgames\.com/);
    assert.match(initAboutView, /openDiscord/);

    assert.doesNotMatch(initAboutView, /#btn-about-twitch/);
    assert.doesNotMatch(initAboutView, /\bbtnTwitch\b/);
    assert.doesNotMatch(initAboutView, /\bURLS\.twitch\b/);
    assert.doesNotMatch(initAboutView, /twitch\.tv\/fukiplays/i);
    assert.doesNotMatch(initAboutView, /fukiplays/i);
  });
});
