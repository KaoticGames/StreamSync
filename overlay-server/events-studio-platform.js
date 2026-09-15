(function (root) {
  const TWITCH_SIM_EVENTS = [
    { key: "follow", label: "New follower" },
    { key: "sub", label: "New sub" },
    { key: "resub", label: "Resub" },
    { key: "gift", label: "Gift subs" },
    { key: "cheer", label: "Cheer / Bits" },
    { key: "raid", label: "Raid" },
  ];

  const KICK_SIM_EVENTS = [
    { key: "follow", label: "New follower" },
    { key: "sub", label: "New sub" },
    { key: "gift", label: "Gift subs" },
    { key: "kicks", label: "Kicks" },
  ];

  function computeTestPlatformUi({ twitch, kick, previousSelection }) {
    const connections = { twitch: !!twitch, kick: !!kick };

    if (connections.twitch && !connections.kick) {
      return {
        showSelector: false,
        platform: "twitch",
        liveEnabled: true,
        connections,
      };
    }

    if (!connections.twitch && connections.kick) {
      return {
        showSelector: false,
        platform: "kick",
        liveEnabled: true,
        connections,
      };
    }

    if (connections.twitch && connections.kick) {
      const prior =
        previousSelection === "twitch" || previousSelection === "kick"
          ? previousSelection
          : null;
      return {
        showSelector: true,
        platform: prior || "twitch",
        liveEnabled: true,
        connections,
      };
    }

    return {
      showSelector: false,
      platform: null,
      liveEnabled: false,
      connections,
    };
  }

  function simEventsForPlatform(platform) {
    if (platform === "kick") return KICK_SIM_EVENTS.slice();
    return TWITCH_SIM_EVENTS.slice();
  }

  function reconcileSimEventSelection(platform, currentSelection) {
    const events = simEventsForPlatform(platform);
    const selected = events.some((event) => event.key === currentSelection)
      ? currentSelection
      : events[0]?.key || "follow";
    return { events, selected };
  }

  function variationEventKeyForPlatform(eventKey, platform) {
    if (platform === "kick" && eventKey === "kicks") return "cheer";
    return eventKey;
  }

  function simHintForPlatform(platform) {
    if (platform === "kick") {
      return "Simulates a Kick event through the same variation rules (value triggers + chance) as live. Use <strong>Live</strong> mode in the toolbar to fire your OBS browser source.";
    }
    return "Simulates a Twitch event through the same variation rules (value triggers + chance) as live. Use <strong>Live</strong> mode in the toolbar to fire your OBS browser source.";
  }

  function usesTierForPlatform(platform, eventKey) {
    return (
      platform === "twitch" &&
      (eventKey === "sub" || eventKey === "resub" || eventKey === "gift")
    );
  }

  function safeTestMode(requestedMode, connections) {
    const wantsLive = requestedMode === "live";
    const connected = !!(connections?.twitch || connections?.kick);
    return wantsLive && connected ? "live" : "local";
  }

  function isSelectedPlatformConnected(platform, connections) {
    return (
      (platform === "twitch" && !!connections?.twitch) ||
      (platform === "kick" && !!connections?.kick)
    );
  }

  function createLatestStatusRunner({ load, applyStatus, applyUnavailable }) {
    let latestRequest = 0;
    return async function run(context) {
      const request = ++latestRequest;
      try {
        const status = await load();
        if (request !== latestRequest) return false;
        applyStatus(status, context);
        return true;
      } catch (error) {
        if (request !== latestRequest) return false;
        applyUnavailable(error, context);
        return false;
      }
    };
  }

  root.StreamSyncEventsStudioPlatform = {
    computeTestPlatformUi,
    simEventsForPlatform,
    reconcileSimEventSelection,
    variationEventKeyForPlatform,
    simHintForPlatform,
    usesTierForPlatform,
    safeTestMode,
    isSelectedPlatformConnected,
    createLatestStatusRunner,
  };
})(
  typeof globalThis !== "undefined"
    ? globalThis
    : typeof window !== "undefined"
      ? window
      : global
);
