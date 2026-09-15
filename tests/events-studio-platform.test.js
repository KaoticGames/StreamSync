"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

require(path.join(__dirname, "..", "overlay-server", "events-studio-platform.js"));

const platform = globalThis.StreamSyncEventsStudioPlatform;
assert.ok(platform, "StreamSyncEventsStudioPlatform must load onto globalThis");

describe("StreamSyncEventsStudioPlatform selection", () => {
  it("defaults to Twitch when only Twitch is connected", () => {
    assert.deepEqual(platform.computeTestPlatformUi({ twitch: true, kick: false }), {
      showSelector: false,
      platform: "twitch",
      liveEnabled: true,
      connections: { twitch: true, kick: false },
    });
  });

  it("defaults to Kick when only Kick is connected", () => {
    assert.deepEqual(platform.computeTestPlatformUi({ twitch: false, kick: true }), {
      showSelector: false,
      platform: "kick",
      liveEnabled: true,
      connections: { twitch: false, kick: true },
    });
  });

  it("shows selector and defaults Twitch when both are connected", () => {
    assert.deepEqual(platform.computeTestPlatformUi({ twitch: true, kick: true }), {
      showSelector: true,
      platform: "twitch",
      liveEnabled: true,
      connections: { twitch: true, kick: true },
    });
  });

  it("preserves a still-valid prior selection when both are connected", () => {
    assert.deepEqual(
      platform.computeTestPlatformUi({
        twitch: true,
        kick: true,
        previousSelection: "kick",
      }),
      {
        showSelector: true,
        platform: "kick",
        liveEnabled: true,
        connections: { twitch: true, kick: true },
      }
    );
  });

  it("disables live testing when neither platform is connected", () => {
    assert.deepEqual(platform.computeTestPlatformUi({ twitch: false, kick: false }), {
      showSelector: false,
      platform: null,
      liveEnabled: false,
      connections: { twitch: false, kick: false },
    });
  });

  it("filters simulate modal events per platform", () => {
    const kickEvents = platform.simEventsForPlatform("kick");
    assert.deepEqual(
      kickEvents.map((e) => e.key),
      ["follow", "sub", "gift", "kicks"]
    );
    assert.equal(platform.variationEventKeyForPlatform("kicks", "kick"), "cheer");
  });

  it("offers tier controls only for Twitch subscriber events", () => {
    assert.equal(platform.usesTierForPlatform("twitch", "sub"), true);
    assert.equal(platform.usesTierForPlatform("twitch", "resub"), true);
    assert.equal(platform.usesTierForPlatform("twitch", "gift"), true);
    assert.equal(platform.usesTierForPlatform("kick", "sub"), false);
    assert.equal(platform.usesTierForPlatform("kick", "gift"), false);
  });
});
