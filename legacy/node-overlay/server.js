// overlay-server/server.js
// Local HTTP + WebSocket server for Stream Sync overlays + Twitch chat (read/write)
// IMPORTANT: Twitch auth must follow the REQUIRED workflow:
// Connect -> Browser opens -> Authorize -> Done
// This is implemented using the OAuth Implicit Grant flow (public client; no client secret).

const express = require("express");
const http = require("http");
const WebSocketLib = require("ws");
const { WebSocketServer } = WebSocketLib;
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const tmi = require("tmi.js");

// NEW: robust storage + paths (userData-safe, self-healing, atomic writes)
const storage = require("./storage");

// Lazy node-fetch wrapper for CommonJS
const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));

// Load .env (optional; supported for dev, and also optionally from userData in packaged builds)
try {
  const userDataRoot = process.env.STREAMSYNC_USERDATA || null;

  // Prefer a userData .env for packaged installs (user-writable)
  const envPathPreferred = userDataRoot ? path.join(userDataRoot, ".env") : null;

  // Fallback for dev: project root next to main.js
  const envPathDev = path.join(__dirname, "..", ".env");

  const envPathToUse =
    envPathPreferred && fs.existsSync(envPathPreferred)
      ? envPathPreferred
      : envPathDev;

  require("dotenv").config({ path: envPathToUse });
  console.log("[OverlayServer] dotenv loaded from", envPathToUse);
} catch (err) {
  console.warn(
    "[OverlayServer] dotenv not loaded (OK if env vars are set another way)",
    err
  );
}

// ───────────────────────────────────────────────
// Basic config
// ───────────────────────────────────────────────

const PORT = Number(process.env.OVERLAY_PORT || 4040);

// Resolve ALL writable paths now (requires main.js to set STREAMSYNC_USERDATA before requiring server.js)
const PATHS = storage.getPaths();

console.log("[OverlayServer] Writable paths:");
console.log("  root =", PATHS.root);
console.log("  dockConfig =", PATHS.dockConfig);
console.log("  overlayConfig =", PATHS.overlayConfig);
console.log("  eventsOverlayConfig =", PATHS.eventsOverlayConfig);
console.log("  twitchTokens =", PATHS.twitchTokens);
console.log("  fontsDir =", PATHS.fontsDir);

// Public Twitch app config (no secret)
const TWITCH_CLIENT_ID = process.env.TWITCH_CLIENT_ID || "";

if (!TWITCH_CLIENT_ID) {
  console.warn(
    "[OverlayServer] TWITCH_CLIENT_ID is not set. Twitch functionality will NOT work until you set it. " +
      "Create a .env in the project root (or userData/.env) with: TWITCH_CLIENT_ID=your_client_id"
  );
}

// Redirect MUST be registered in Twitch dev console.
// For implicit grant, Twitch returns access_token in the URL hash fragment.
const TWITCH_REDIRECT_URI =
  process.env.TWITCH_REDIRECT_URI ||
  `http://localhost:${PORT}/auth/twitch/callback`;

// ───────────────────────────────────────────────
// Dock config + overlay config defaults (in-memory)
// ───────────────────────────────────────────────

let dockConfig = {
  profiles: {
    // default profile; can be overridden by POST
    "chat-default": {
      fontSize: 13,
      showTimestamps: true,
      showBadges: true,
    },
  },
  eventsDock: null, // will be ensured below
};

// Events Dock config (GLOBAL; no profiles)
let eventsDockConfig = {
  fontSize: 13,
  showTimestamps: true,
  showBadges: true,
  events: {
    follow: true,
    sub: true,
    resub: true,
    gift: true,
    bits: true,
    raid: true,
    redeem: true,
    hypetrain: true,
    announce: true,
  },
};

let overlayConfig = {
  profiles: {
    "chat-default": {
      // basic visibility
      showTimestamps: true,
      showBadges: true,

      // font
      fontSize: 18,
      fontFamily: "system-ui",

      // transform
      textRotate: 0,
      textSkew: 0,

      // layout / style
      feedDirection: "up-down", // up-down | down-up | left-right | right-left
      messageStyle: "bubble", // bubble | plain
      bubbleRadius: 18,
      bubbleColorMode: "fixed", // fixed | user
      bubbleColor: "rgba(15, 23, 42, 0.85)",

      // stroke
      strokeEnabled: false,
      strokeColor: "#000000",
      strokeWidth: 0,

      // bubble transparency (0–1)
      bubbleAlpha: 1,

      // background
      bgMode: "transparent", // transparent | solid | gradient
      bgColor: "#000000",
      bgGradient: "",

      // display mode
      displayMode: "solid", // solid | popup
      popupDuration: 8,
      popupExitStyle: "fade", // fade | slide | etc. (future-proof)
    },
  },
};

// ───────────────────────────────────────────────
// Load/save configs (ROBUST, SELF-HEALING, ATOMIC)
// ───────────────────────────────────────────────

function loadDockConfig() {
  dockConfig = storage.readJsonOrDefault(PATHS.dockConfig, {
    profiles: {
      "chat-default": { fontSize: 13, showTimestamps: true, showBadges: true },
    },
    eventsDock: null,
  });

  dockConfig.profiles = dockConfig.profiles || {};
  dockConfig.profiles["chat-default"] = dockConfig.profiles["chat-default"] || {
    fontSize: 13,
    showTimestamps: true,
    showBadges: true,
  };

  // Ensure global events dock config exists + merge
  if (dockConfig.eventsDock && typeof dockConfig.eventsDock === "object") {
    const ed = dockConfig.eventsDock || {};
    eventsDockConfig = {
      ...eventsDockConfig,
      ...ed,
      events: {
        ...eventsDockConfig.events,
        ...(ed.events || {}),
      },
    };
  } else {
    dockConfig.eventsDock = eventsDockConfig;
    try {
      storage.writeJson(PATHS.dockConfig, dockConfig);
    } catch (_) {}
  }

  console.log("[OverlayServer] Dock config ready.");
}

function saveDockConfig() {
  try {
    dockConfig.eventsDock = eventsDockConfig;
    storage.writeJson(PATHS.dockConfig, dockConfig);
    console.log("[OverlayServer] Saved dock config to disk.");
  } catch (err) {
    console.error("[OverlayServer] Failed to save dock config:", err);
  }
}

function loadOverlayConfig() {
  overlayConfig = storage.readJsonOrDefault(PATHS.overlayConfig, {
    profiles: {
      "chat-default": {
        showTimestamps: true,
        showBadges: true,
        fontSize: 18,
        fontFamily: "system-ui",
        textRotate: 0,
        textSkew: 0,
        feedDirection: "up-down",
        messageStyle: "bubble",
        bubbleRadius: 18,
        bubbleColorMode: "fixed",
        bubbleColor: "rgba(15, 23, 42, 0.85)",
        strokeEnabled: false,
        strokeColor: "#000000",
        strokeWidth: 0,
        bubbleAlpha: 1,
        bgMode: "transparent",
        bgColor: "#000000",
        bgGradient: "",
        displayMode: "solid",
        popupDuration: 8,
        popupExitStyle: "fade",
      },
    },
  });

  overlayConfig.profiles = overlayConfig.profiles || {};
  overlayConfig.profiles["chat-default"] =
    overlayConfig.profiles["chat-default"] || {
      showTimestamps: true,
      showBadges: true,
      fontSize: 18,
      fontFamily: "system-ui",
      messageStyle: "bubble",
    };

  console.log("[OverlayServer] Overlay config ready.");
}

function saveOverlayConfig() {
  try {
    storage.writeJson(PATHS.overlayConfig, overlayConfig);
    console.log("[OverlayServer] Saved overlay config to disk.");
  } catch (err) {
    console.error("[OverlayServer] Failed to save overlay config:", err);
  }
}

// ───────────────────────────────────────────────
// Events overlay config (Events Studio)
// ───────────────────────────────────────────────

function _defaultEventsOverlayProfile() {
  const baseVariation = () => ({
    id: (
      Math.random().toString(16).slice(2) + Date.now().toString(16)
    ).slice(0, 18),
    name: "Base Alert",
    image: { type: "url", value: "" },
    sound: { type: "url", value: "", volume: 100 },
    layout: "textSide",
    message: "[name] triggered an alert!",
    durationSec: 6,
    text: {
      fontFamily:
        "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
      fontSize: 54,
      fontWeight: 800,
      color: "#ffffff",
      strokeColor: "rgba(0,0,0,0.75)",
      strokeWidth: 0,
    },
    placement: {
      image: { x: 120, y: 140, w: 320, h: 320 },
      text: { x: 500, y: 170, w: 520, h: 180 },
    },
  });

  const makeEvent = () => ({ variations: [baseVariation()] });

  return {
    version: 1,
    stage: { w: 1280, h: 720, grid: true, zoom: 1 },
    events: {
      follow: makeEvent(),
      sub: makeEvent(),
      resub: makeEvent(),
      gift: makeEvent(),
      cheer: makeEvent(),
      raid: makeEvent(),
      // redeem optional
    },
  };
}

let eventsOverlayConfig = { version: 1, profiles: {} };

function loadEventsOverlayConfig() {
  const defaults = {
    version: 1,
    profiles: { default: _defaultEventsOverlayProfile() },
  };

  eventsOverlayConfig = storage.readJsonOrDefault(
    PATHS.eventsOverlayConfig,
    defaults
  );

  eventsOverlayConfig.version = eventsOverlayConfig.version || 1;
  eventsOverlayConfig.profiles = eventsOverlayConfig.profiles || {};
  if (!eventsOverlayConfig.profiles.default) {
    eventsOverlayConfig.profiles.default = _defaultEventsOverlayProfile();
  }

  console.log("[OverlayServer] Events overlay config ready.");
}

function saveEventsOverlayConfig() {
  try {
    storage.writeJson(PATHS.eventsOverlayConfig, eventsOverlayConfig);
  } catch (err) {
    console.error("[OverlayServer] Failed to save events overlay config:", err);
  }
}

// ───────────────────────────────────────────────
// Token persistence
// ───────────────────────────────────────────────

const twitchAuth = {
  accessToken: null,
  // NOTE: Implicit grant does NOT provide refresh_token
  refreshToken: null,
  expiresIn: null,
  obtainmentTimestamp: null,
  login: null,
  userId: null,
  scopes: null,
};

const twitchStatus = {
  connected: false,
  channel: null, // login name used as channel
};

// Map profileId -> Set<WebSocket>
const feedClients = new Map();

// tmi.js client
let twitchClient = null;

// Badge cache (to avoid hammering Twitch)
let badgeCache = {
  sets: null,
  lastFetch: 0,
  userId: null,
};
const BADGE_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

// Emote cache (combined global + channel + user emotes)
let emoteCache = {
  list: null,
  lastFetch: 0,
  userId: null,
};
const EMOTE_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

function loadTwitchTokens() {
  const parsed = storage.readJsonOrDefault(PATHS.twitchTokens, {
    accessToken: null,
    refreshToken: null,
    expiresIn: null,
    obtainmentTimestamp: null,
    login: null,
    userId: null,
    scopes: null,
  });

  twitchAuth.accessToken = parsed.accessToken || null;
  twitchAuth.refreshToken = parsed.refreshToken || null;
  twitchAuth.expiresIn = parsed.expiresIn || null;
  twitchAuth.obtainmentTimestamp = parsed.obtainmentTimestamp || null;
  twitchAuth.login = parsed.login || null;
  twitchAuth.userId = parsed.userId || null;
  twitchAuth.scopes = parsed.scopes || null;

  console.log("[OverlayServer] Twitch tokens ready.");
}

function saveTwitchTokens() {
  try {
    storage.writeJson(PATHS.twitchTokens, {
      accessToken: twitchAuth.accessToken,
      refreshToken: twitchAuth.refreshToken,
      expiresIn: twitchAuth.expiresIn,
      obtainmentTimestamp: twitchAuth.obtainmentTimestamp,
      login: twitchAuth.login,
      userId: twitchAuth.userId,
      scopes: twitchAuth.scopes,
    });
    console.log("[OverlayServer] Saved Twitch tokens to disk.");
  } catch (err) {
    console.error("[OverlayServer] Failed to save Twitch tokens:", err);
  }
}

// ───────────────────────────────────────────────
// Helpers: broadcast
// ───────────────────────────────────────────────

function broadcastEvent(profileId, event) {
  const clients = feedClients.get(profileId);
  if (!clients) return;

  const payload = JSON.stringify(event);
  for (const socket of clients) {
    try {
      if (socket.readyState === socket.OPEN) {
        socket.send(payload);
      }
    } catch (err) {
      console.error("[OverlayServer] Error sending WS message:", err);
    }
  }
}

function broadcastEventToAll(event) {
  const payload = JSON.stringify(event);
  for (const [profileId, clients] of feedClients.entries()) {
    if (!clients) continue;
    for (const socket of clients) {
      try {
        if (socket.readyState === socket.OPEN) {
          socket.send(payload);
        }
      } catch (err) {
        console.error("[OverlayServer] Error broadcasting to", profileId, err);
      }
    }
  }
}

function safeUuid() {
  try {
    if (crypto.randomUUID) return crypto.randomUUID();
  } catch (_) {}
  return (
    Date.now().toString(16) + "-" + Math.random().toString(16).slice(2)
  );
}

function makeDockEvent({ eventType, ts, detail, label }) {
  return {
    type: "dock-event",
    id: safeUuid(),
    ts: ts || Date.now(),
    eventType,
    label: label || eventType,
    detail: detail || "",
  };
}

// ───────────────────────────────────────────────
// OAuth token lifecycle (Implicit Grant)
// ───────────────────────────────────────────────

function tokenExpiredOrMissing() {
  if (!twitchAuth.accessToken) return true;
  if (!twitchAuth.obtainmentTimestamp || !twitchAuth.expiresIn) return false; // if missing, treat as "unknown"
  const age = (Date.now() - twitchAuth.obtainmentTimestamp) / 1000;
  return age >= twitchAuth.expiresIn - 10;
}

async function validateToken(accessToken) {
  const validateRes = await fetch("https://id.twitch.tv/oauth2/validate", {
    headers: { Authorization: "OAuth " + accessToken },
  });

  if (!validateRes.ok) {
    const text = await validateRes.text();
    throw new Error(`Twitch validate failed: ${validateRes.status} ${text}`);
  }

  return validateRes.json(); // { login, user_id, expires_in, scopes, client_id }
}

async function ensureValidToken() {
  if (!twitchAuth.accessToken) {
    throw new Error("No Twitch accessToken. Please connect Twitch first.");
  }
  if (tokenExpiredOrMissing()) {
    // Implicit flow has no refresh token; user must reconnect
    throw new Error("Twitch token expired. Please reconnect Twitch.");
  }
}

// ───────────────────────────────────────────────
// Twitch Helix helper
// ───────────────────────────────────────────────

async function twitchFetch(pathname, options = {}) {
  await ensureValidToken();

  if (!TWITCH_CLIENT_ID) {
    throw new Error("TWITCH_CLIENT_ID not configured.");
  }

  const url = new URL("https://api.twitch.tv/helix" + pathname);

  const res = await fetch(url.toString(), {
    ...options,
    headers: {
      "Client-Id": TWITCH_CLIENT_ID,
      Authorization: "Bearer " + twitchAuth.accessToken,
      ...(options.headers || {}),
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(
      `Helix ${pathname} failed: ${res.status} ${res.statusText} – ${text}`
    );
  }

  return res.json();
}

/**
 * Update chat settings for the broadcaster's own channel.
 * Used for commands like /slow, /slowoff.
 */
async function updateChatSettings(partialSettings) {
  if (!twitchAuth.userId) {
    throw new Error(
      "Cannot update chat settings – Twitch userId missing. Connect Twitch first."
    );
  }

  const params = new URLSearchParams({
    broadcaster_id: twitchAuth.userId,
    moderator_id: twitchAuth.userId,
  });

  return twitchFetch("/chat/settings?" + params.toString(), {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(partialSettings || {}),
  });
}

// ───────────────────────────────────────────────
// Twitch badges: global + channel merged
// ───────────────────────────────────────────────

async function getMergedBadgeSets() {
  if (
    badgeCache.sets &&
    badgeCache.userId === twitchAuth.userId &&
    Date.now() - badgeCache.lastFetch < BADGE_CACHE_TTL
  ) {
    return badgeCache.sets;
  }

  if (!twitchAuth.userId) {
    throw new Error(
      "No Twitch userId/accessToken for badges. Connect Twitch first."
    );
  }

  await ensureValidToken();

  const headers = {
    "Client-Id": TWITCH_CLIENT_ID,
    Authorization: "Bearer " + twitchAuth.accessToken,
  };

  const [globalRes, channelRes] = await Promise.all([
    fetch("https://api.twitch.tv/helix/chat/badges/global", { headers }),
    fetch(
      `https://api.twitch.tv/helix/chat/badges?broadcaster_id=${encodeURIComponent(
        twitchAuth.userId
      )}`,
      { headers }
    ),
  ]);

  if (!globalRes.ok) {
    const text = await globalRes.text();
    throw new Error(`Helix global badges failed: ${globalRes.status} ${text}`);
  }
  if (!channelRes.ok) {
    const text = await channelRes.text();
    throw new Error(`Helix channel badges failed: ${channelRes.status} ${text}`);
  }

  const globalJson = await globalRes.json();
  const channelJson = await channelRes.json();

  const badge_sets = {};

  function mergeHelixBadgeData(source, ownerType) {
    if (!source || !Array.isArray(source.data)) return;

    for (const set of source.data) {
      const setId = set.set_id;
      if (!setId) continue;

      const versions = {};
      const helixVersions = Array.isArray(set.versions) ? set.versions : [];

      for (const v of helixVersions) {
        if (!v.id) continue;
        versions[v.id] = {
          id: v.id,
          image_url_1x: v.image_url_1x || null,
          image_url_2x: v.image_url_2x || null,
          image_url_4x: v.image_url_4x || null,
          title: v.title,
          description: v.description,
          click_action: v.click_action,
          click_url: v.click_url,
          ownerType,
        };
      }

      badge_sets[setId] = {
        ...(badge_sets[setId] || {}),
        versions: {
          ...(badge_sets[setId]?.versions || {}),
          ...versions,
        },
      };
    }
  }

  mergeHelixBadgeData(globalJson, "global");
  mergeHelixBadgeData(channelJson, "channel");

  const merged = { badge_sets };

  badgeCache.sets = merged;
  badgeCache.lastFetch = Date.now();
  badgeCache.userId = twitchAuth.userId;

  return merged;
}

// ───────────────────────────────────────────────
// Twitch emotes: global + channel + user entitlements (paginated)
// ───────────────────────────────────────────────

async function fetchAllUserEmotes(userId) {
  const all = [];
  let cursor = null;
  let page = 0;

  while (true) {
    const params = new URLSearchParams({
      user_id: userId,
      first: "100",
    });
    if (cursor) params.set("after", cursor);

    const data = await twitchFetch("/chat/emotes/user?" + params.toString());

    if (data && Array.isArray(data.data)) {
      all.push(...data.data);
    }

    cursor =
      data && data.pagination && data.pagination.cursor
        ? data.pagination.cursor
        : null;

    page += 1;
    if (!cursor) break;
    if (page > 20) {
      console.warn(
        "[OverlayServer] fetchAllUserEmotes: stopping after 20 pages for user",
        userId
      );
      break;
    }
  }

  console.log(
    "[OverlayServer] fetchAllUserEmotes: got",
    all.length,
    "emotes for user",
    userId
  );
  return all;
}

async function getMergedEmotes() {
  if (
    emoteCache.list &&
    emoteCache.userId === twitchAuth.userId &&
    Date.now() - emoteCache.lastFetch < EMOTE_CACHE_TTL
  ) {
    return emoteCache.list;
  }

  if (!twitchAuth.userId) {
    throw new Error("No Twitch userId for emotes. Connect Twitch first.");
  }

  const userId = twitchAuth.userId;

  const [globalRes, channelRes, userEmoteData] = await Promise.all([
    twitchFetch("/chat/emotes/global").catch((err) => {
      console.warn("[OverlayServer] /chat/emotes/global failed:", err.message);
      return null;
    }),
    twitchFetch(`/chat/emotes?broadcaster_id=${userId}`).catch((err) => {
      console.warn("[OverlayServer] /chat/emotes (channel) failed:", err.message);
      return null;
    }),
    fetchAllUserEmotes(userId).catch((err) => {
      console.warn("[OverlayServer] /chat/emotes/user failed:", err.message);
      return [];
    }),
  ]);

  const byId = new Map();

  function addEmoteBatch(list, defaultOwnerType, defaultOwnerId) {
    if (!list || !Array.isArray(list)) return;

    for (const emote of list) {
      if (!emote || !emote.id) continue;
      if (byId.has(emote.id)) continue;

      const emoteType = emote.emote_type || null;
      const ownerId = emote.owner_id || defaultOwnerId || null;

      let ownerType = defaultOwnerType || "unknown";
      if (emoteType === "globals") ownerType = "global";
      else if (
        emoteType === "subscriptions" ||
        emoteType === "bitstier" ||
        emoteType === "follower"
      ) {
        if (!ownerType || ownerType === "unknown") ownerType = "channel";
      }

      byId.set(emote.id, {
        id: emote.id,
        name: emote.name,
        images: emote.images || null,
        emoteType,
        emoteSetId: emote.emote_set_id || null,
        ownerType,
        ownerId,
        ownerLogin: null,
        ownerName: null,
        ownerProfileImageUrl: null,
        ownerIsSelf: ownerId === userId,
      });
    }
  }

  addEmoteBatch(globalRes && globalRes.data, "global", null);
  addEmoteBatch(channelRes && channelRes.data, "channel", userId);
  addEmoteBatch(userEmoteData, null, null);

  const ownerIds = Array.from(
    new Set(
      Array.from(byId.values())
        .map((e) => e.ownerId)
        .filter((id) => !!id)
    )
  );

  const ownersMap = new Map();
  const CHUNK = 100;

  for (let i = 0; i < ownerIds.length; i += CHUNK) {
    const chunk = ownerIds.slice(i, i + CHUNK);
    const params = new URLSearchParams();
    for (const id of chunk) params.append("id", id);

    try {
      const usersRes = await twitchFetch("/users?" + params.toString());
      if (usersRes && Array.isArray(usersRes.data)) {
        for (const u of usersRes.data) ownersMap.set(u.id, u);
      }
    } catch (err) {
      console.warn(
        "[OverlayServer] /users for emote owners failed:",
        err.message
      );
    }
  }

  for (const emote of byId.values()) {
    if (!emote.ownerId) continue;
    const u = ownersMap.get(emote.ownerId);
    if (!u) continue;

    emote.ownerLogin = u.login || null;
    emote.ownerName = u.display_name || u.login || null;
    emote.ownerProfileImageUrl = u.profile_image_url || null;
    if (emote.ownerId === userId) emote.ownerIsSelf = true;
  }

  const mergedList = Array.from(byId.values());

  const ownerCount = new Set(mergedList.map((e) => e.ownerId).filter(Boolean))
    .size;

  console.log(
    "[OverlayServer] Built emote list:",
    mergedList.length,
    "emotes across",
    ownerCount,
    "owners"
  );

  emoteCache.list = mergedList;
  emoteCache.lastFetch = Date.now();
  emoteCache.userId = userId;

  return mergedList;
}

// ───────────────────────────────────────────────
// Twitch chat (tmi.js)
// ───────────────────────────────────────────────

async function startTwitchClient() {
  if (!twitchAuth.accessToken || !twitchAuth.login) {
    console.warn(
      "[OverlayServer] Cannot start Twitch client – tokens or login missing."
    );
    return;
  }

  // Ensure token isn't expired before using it for IRC auth
  await ensureValidToken();

  const username = twitchAuth.login;
  const channel = twitchAuth.login;

  if (twitchClient) {
    try {
      await twitchClient.disconnect();
    } catch (_) {}
    twitchClient = null;
  }

  twitchClient = new tmi.Client({
    options: { debug: false },
    connection: { reconnect: true, secure: true },
    identity: {
      username,
      password: "oauth:" + twitchAuth.accessToken,
    },
    channels: [channel],
  });

  twitchClient.on("connected", (addr, port) => {
    console.log(
      `[OverlayServer] Connected to Twitch chat as ${username} on ${addr}:${port}, channel #${channel}`
    );
    twitchStatus.connected = true;
    twitchStatus.channel = channel;
  });

  twitchClient.on("disconnected", (reason) => {
    console.log("[OverlayServer] Disconnected from Twitch chat:", reason);
    twitchStatus.connected = false;
    twitchStatus.channel = null;
  });

  twitchClient.on("message", (channelName, tags, message, self) => {
    const displayName = tags["display-name"] || tags.username || "Unknown";
    const userColor = tags.color || null;
    const badgesRaw = tags.badges || {};
    const badges = Object.keys(badgesRaw || {});
    const emotesRaw = tags.emotes || null;

    const evt = {
      type: "chat",
      ts: Date.now(),
      user: {
        name: tags.username || displayName,
        displayName,
        color: userColor,
        badges,
        badgesRaw,
      },
      message,
      emotes: emotesRaw,
      self: !!self,
    };

    broadcastEventToAll(evt);
  });

  twitchClient.connect().catch((err) => {
    console.error("[OverlayServer] Twitch connect error:", err);
    twitchStatus.connected = false;
    twitchStatus.channel = null;
  });
}

function stopTwitchClient() {
  if (twitchClient) {
    try {
      twitchClient.disconnect();
    } catch (_) {}
    twitchClient = null;
  }
  twitchStatus.connected = false;
  twitchStatus.channel = null;
}

async function sendChatMessageFromDock(text) {
  if (!text || !text.trim()) return;

  if (!twitchClient || !twitchStatus.connected || !twitchStatus.channel) {
    console.warn(
      "[OverlayServer] Dock tried to send chat but Twitch client is not ready."
    );
    return;
  }

  const trimmed = text.trim();

  if (trimmed.startsWith("/")) {
    await handleDockCommand(trimmed);
    return;
  }

  const channel = twitchStatus.channel;
  try {
    await twitchClient.say(channel, text);
    console.log(`[OverlayServer] Sent message to #${channel} from dock.`);
  } catch (err) {
    console.error("[OverlayServer] Error sending chat from dock:", err);
  }
}

async function handleDockCommand(text) {
  if (!twitchClient || !twitchStatus.connected || !twitchStatus.channel) {
    console.warn(
      "[OverlayServer] Dock tried to run command but Twitch client is not ready."
    );
    return;
  }

  const channel = twitchStatus.channel;
  const parts = text.trim().split(/\s+/);
  const cmd = (parts[0] || "").toLowerCase();
  const args = parts.slice(1);

  console.log("[OverlayServer] handleDockCommand:", cmd, args);

  try {
    switch (cmd) {
      case "/slow": {
        const raw = args[0];

        if (
          !raw ||
          raw === "0" ||
          raw.toLowerCase() === "off" ||
          raw.toLowerCase() === "disable"
        ) {
          await updateChatSettings({ slow_mode: false });
          console.log("[OverlayServer] Disabled slow mode via /slow.");
          break;
        }

        let wait = parseInt(raw, 10);
        if (!Number.isFinite(wait) || wait <= 0) wait = 30;
        wait = Math.max(3, Math.min(180, wait));

        await updateChatSettings({
          slow_mode: true,
          slow_mode_wait_time: wait,
        });

        console.log(
          `[OverlayServer] Enabled slow mode via /slow: ${wait}s interval.`
        );
        break;
      }

      case "/slowoff": {
        await updateChatSettings({ slow_mode: false });
        console.log("[OverlayServer] Disabled slow mode via /slowoff.");
        break;
      }

      case "/ban": {
        const user = args[0];
        const reason = args.slice(1).join(" ");
        if (user) await twitchClient.ban(channel, user, reason || undefined);
        break;
      }

      case "/unban": {
        const user = args[0];
        if (user) await twitchClient.unban(channel, user);
        break;
      }

      case "/timeout": {
        const user = args[0];
        const duration = parseInt(args[1] || "600", 10) || 600;
        const reason = args.slice(2).join(" ");
        if (user) {
          await twitchClient.timeout(
            channel,
            user,
            duration,
            reason || undefined
          );
        }
        break;
      }

      default:
        await twitchClient.say(channel, text);
        break;
    }
  } catch (err) {
    console.error("[OverlayServer] Error handling dock command:", text, err);
  }
}

// ───────────────────────────────────────────────
// EventSub WebSocket ingestion
// ───────────────────────────────────────────────

let eventSubSocket = null;
let eventSubSessionId = null;
let eventSubStartedForUserId = null;

/** Twitch plan tier → display tier 1–3 (1000/2000/3000, Prime, or 1/2/3). */
function twitchTierDisplayNumber(tier) {
  const s = String(tier ?? "").trim();
  if (!s) return null;
  if (s === "1000" || /^prime$/i.test(s)) return 1;
  if (s === "2000") return 2;
  if (s === "3000") return 3;
  const n = Number(s);
  if (n === 1 || n === 2 || n === 3) return n;
  if (n === 1000) return 1;
  if (n === 2000) return 2;
  if (n === 3000) return 3;
  return null;
}

function formatSubDockDetail(user, tier) {
  const tn = twitchTierDisplayNumber(tier);
  if (tn) return `${user} subscribed — Tier ${tn}`;
  return `${user} subscribed`;
}

function formatResubDockDetail(user, months, tier, msg) {
  const tn = twitchTierDisplayNumber(tier);
  let detail = user;
  if (months !== "" && months != null) {
    detail = `${user} — ${months} months`;
    if (tn) detail += ` — Tier ${tn}`;
  } else if (tn) {
    detail += ` — Tier ${tn}`;
  }
  if (msg) detail += `: ${msg}`;
  return detail;
}

function formatGiftDockDetail(gifter, total, tier, recipient) {
  const tn = twitchTierDisplayNumber(tier);
  const qty =
    total !== "" && total != null && !Number.isNaN(Number(total))
      ? Number(total)
      : null;

  if (qty != null && tn) return `${gifter} gifted ${qty} Tier ${tn} subs`;
  if (qty != null) return `${gifter} gifted ${qty} subs`;
  if (recipient && tn) return `${gifter} gifted ${recipient} a Tier ${tn} sub`;
  if (recipient) return `${gifter} gifted ${recipient} a sub`;
  if (tn) return `${gifter} gifted a Tier ${tn} sub`;
  return `${gifter} gifted a sub`;
}

// minimal "variable set" normalization
function normalizeEventVariables(vars) {
  const v = vars || {};
  const name = v.name ?? v.user ?? "";
  const user = v.user ?? v.name ?? "";

  return {
    user,
    name,
    amount: v.amount ?? v.tier ?? v.bits ?? "",
    months: v.months ?? "",
    reward: v.reward ?? v.title ?? "",
    input: v.input ?? v.message ?? "",
    recipient: v.recipient ?? "",
    tier: v.tier ?? v.amount ?? "",
    bits: v.bits ?? "",
    raiders: v.raiders ?? v.viewers ?? "",
  };
}

async function helixPost(pathname, body) {
  await ensureValidToken();
  if (!TWITCH_CLIENT_ID) throw new Error("TWITCH_CLIENT_ID not configured.");

  const res = await fetch("https://api.twitch.tv/helix" + pathname, {
    method: "POST",
    headers: {
      "Client-Id": TWITCH_CLIENT_ID,
      Authorization: "Bearer " + twitchAuth.accessToken,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  // 409 = subscription already exists
  if (res.status === 409) return { ok: true, already: true };

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Helix POST ${pathname} failed: ${res.status} ${txt}`);
  }
  return res.json();
}

async function createWsSubscription(type, version, condition) {
  if (!eventSubSessionId) return;

  return helixPost("/eventsub/subscriptions", {
    type,
    version,
    condition,
    transport: {
      method: "websocket",
      session_id: eventSubSessionId,
    },
  });
}

async function subscribeEventSubTopics() {
  if (!twitchAuth.userId) throw new Error("Missing broadcaster userId.");

  await createWsSubscription("channel.follow", "2", {
    broadcaster_user_id: twitchAuth.userId,
    moderator_user_id: twitchAuth.userId,
  });

  await createWsSubscription("channel.subscribe", "1", {
    broadcaster_user_id: twitchAuth.userId,
  });

  await createWsSubscription("channel.subscription.message", "1", {
    broadcaster_user_id: twitchAuth.userId,
  });

  await createWsSubscription("channel.subscription.gift", "1", {
    broadcaster_user_id: twitchAuth.userId,
  });

  await createWsSubscription("channel.cheer", "1", {
    broadcaster_user_id: twitchAuth.userId,
  });

  await createWsSubscription("channel.raid", "1", {
    to_broadcaster_user_id: twitchAuth.userId,
  });

  await createWsSubscription(
    "channel.channel_points_custom_reward_redemption.add",
    "1",
    {
      broadcaster_user_id: twitchAuth.userId,
    }
  );
}

function handleEventSubNotification(subscriptionType, event) {
  try {
    switch (subscriptionType) {
      case "channel.follow": {
        const user = event?.user_name || "";
        broadcastEventToAll({
          type: "event-alert",
          eventType: "follow",
          data: { variables: normalizeEventVariables({ name: user }) },
        });

        broadcastEventToAll(
          makeDockEvent({
            eventType: "follow",
            label: "Follow",
            detail: `${user} followed`,
          })
        );
        break;
      }

      case "channel.subscribe": {
        const user = event?.user_name || "";
        const tier = event?.tier || "";
        broadcastEventToAll({
          type: "event-alert",
          eventType: "sub",
          data: {
            variables: normalizeEventVariables({
              name: user,
              amount: tier,
            }),
          },
        });

        broadcastEventToAll(
          makeDockEvent({
            eventType: "sub",
            label: "Sub",
            detail: formatSubDockDetail(user, tier),
          })
        );
        break;
      }

      case "channel.subscription.message": {
        const user = event?.user_name || "";
        const months =
          event?.cumulative_months ?? event?.streak_months ?? "";
        const msg = event?.message?.text || "";
        const tier = event?.tier || "";

        broadcastEventToAll({
          type: "event-alert",
          eventType: "resub",
          data: {
            variables: normalizeEventVariables({
              name: user,
              months,
              input: msg,
              amount: tier,
            }),
          },
        });

        broadcastEventToAll(
          makeDockEvent({
            eventType: "resub",
            label: "Resub",
            detail: formatResubDockDetail(user, months, tier, msg),
          })
        );
        break;
      }

      case "channel.subscription.gift": {
        const gifter = event?.user_name || "Anonymous";
        const recipient = event?.recipient_user_name || "";
        const total = event?.total ?? "";

        broadcastEventToAll({
          type: "event-alert",
          eventType: "gift",
          data: {
            variables: normalizeEventVariables({
              name: gifter,
              recipient,
              amount: total,
              tier: event?.tier ?? "",
            }),
          },
        });

        broadcastEventToAll(
          makeDockEvent({
            eventType: "gift",
            label: "Gift",
            detail: formatGiftDockDetail(
              gifter,
              total,
              event?.tier ?? "",
              recipient
            ),
          })
        );
        break;
      }

      case "channel.cheer": {
        const user = event?.user_name || "";
        const bits = event?.bits ?? "";
        const msg = event?.message || "";

        broadcastEventToAll({
          type: "event-alert",
          eventType: "cheer",
          data: {
            variables: normalizeEventVariables({
              name: user,
              amount: bits,
              input: msg,
            }),
          },
        });

        // Dock uses "bits" eventType for consistency with your dock toggles
        broadcastEventToAll(
          makeDockEvent({
            eventType: "bits",
            label: "Bits",
            detail: `${user} cheered ${bits}${msg ? `: ${msg}` : ""}`,
          })
        );
        break;
      }

      case "channel.raid": {
        const from = event?.from_broadcaster_user_name || "";
        const viewers = event?.viewers ?? "";

        broadcastEventToAll({
          type: "event-alert",
          eventType: "raid",
          data: {
            variables: normalizeEventVariables({
              name: from,
              amount: viewers,
            }),
          },
        });

        broadcastEventToAll(
          makeDockEvent({
            eventType: "raid",
            label: "Raid",
            detail: `${from} raided${viewers ? ` with ${viewers}` : ""}`,
          })
        );
        break;
      }

      case "channel.channel_points_custom_reward_redemption.add": {
        const user = event?.user_name || "";
        const rewardTitle = event?.reward?.title || "";
        const input = event?.user_input || "";
        const cost = event?.reward?.cost ?? "";
        // Channel points: events dock only (not events overlays).
        const detail = `${rewardTitle} — ${user}${
          input ? `: ${input}` : ""
        }${cost ? ` (${cost} pts)` : ""}`;

        broadcastEventToAll(
          makeDockEvent({
            eventType: "redeem",
            label: "Channel Points",
            detail,
          })
        );

        break;
      }

      default:
        break;
    }
  } catch (err) {
    console.warn("[EventSub] Notification handling error:", err.message || err);
  }
}

async function startEventSub() {
  if (!twitchAuth.accessToken || !twitchAuth.userId) {
    console.log("[EventSub] Not starting (not authenticated).");
    return;
  }

  if (!TWITCH_CLIENT_ID) {
    console.warn("[EventSub] Not starting (TWITCH_CLIENT_ID missing).");
    return;
  }

  // avoid duplicates if already started for this user
  if (eventSubSocket && eventSubStartedForUserId === twitchAuth.userId) return;

  // stop existing
  try {
    eventSubSocket?.close();
  } catch (_) {}
  eventSubSocket = null;
  eventSubSessionId = null;
  eventSubStartedForUserId = twitchAuth.userId;

  eventSubSocket = new WebSocketLib("wss://eventsub.wss.twitch.tv/ws");

  eventSubSocket.on("open", () => console.log("[EventSub] WS connected"));

  eventSubSocket.on("close", () => {
    console.warn("[EventSub] WS closed");
    eventSubSessionId = null;
    eventSubSocket = null;

    // retry if still connected
    if (twitchAuth.accessToken && twitchAuth.userId) {
      setTimeout(() => startEventSub().catch(() => {}), 3000);
    }
  });

  eventSubSocket.on("error", (e) => {
    console.warn("[EventSub] WS error", e?.message || e);
  });

  eventSubSocket.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString("utf8"));
    } catch {
      return;
    }

    const messageType = msg?.metadata?.message_type;

    if (messageType === "session_welcome") {
      eventSubSessionId = msg?.payload?.session?.id || null;
      console.log("[EventSub] session_id =", eventSubSessionId);

      try {
        await subscribeEventSubTopics();
        console.log("[EventSub] Subscription requests sent.");
      } catch (err) {
        console.warn(
          "[EventSub] Failed to create some subscriptions:",
          err.message || err
        );
      }
      return;
    }

    if (messageType === "session_reconnect") {
      const reconnectUrl = msg?.payload?.session?.reconnect_url;
      if (reconnectUrl) {
        console.log("[EventSub] session_reconnect -> reconnecting");
        try {
          eventSubSocket?.close();
        } catch (_) {}
        eventSubSocket = new WebSocketLib(reconnectUrl);
      }
      return;
    }

    if (messageType !== "notification") return;

    const subscriptionType = msg?.metadata?.subscription_type;
    const event = msg?.payload?.event;

    handleEventSubNotification(subscriptionType, event);
  });
}

function stopEventSub() {
  try {
    eventSubSocket?.close();
  } catch (_) {}
  eventSubSocket = null;
  eventSubSessionId = null;
  eventSubStartedForUserId = null;
}

// ───────────────────────────────────────────────
// Express app + routes
// ───────────────────────────────────────────────

let _started = false;

function startOverlayServer() {
  if (_started) return;
  _started = true;

  // Load configs/tokens (self-heal missing/corrupt)
  loadDockConfig();
  loadOverlayConfig();
  loadEventsOverlayConfig();
  loadTwitchTokens();

  const app = express();
  const server = http.createServer(app);

  app.use(express.json({ limit: "20mb" }));
  // Allow the Electron UI (file://) to call this local server without CORS issues
  app.use((req, res, next) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
    if (req.method === "OPTIONS") return res.sendStatus(204);
    next();
  });



  // Serve uploaded fonts (userData/fonts)
  app.use(
    "/fonts",
    express.static(PATHS.fontsDir, {
      maxAge: "30d",
      immutable: true,
    })
  );

  // Serve static files from project root (boot.html, shell.html, etc.)
  app.use(express.static(path.join(__dirname, "..")));

  // Chat overlay HTML
  app.get("/overlay/chat", (req, res) => {
    const filePath = path.join(__dirname, "chat-overlay.html");
    if (fs.existsSync(filePath)) res.sendFile(filePath);
    else
      res
        .status(404)
        .send("Chat overlay HTML not found (chat-overlay.html missing).");
  });

  // Events overlay HTML (OBS browser source)
  app.get("/overlay/events", (req, res) => {
    const filePath = path.join(__dirname, "events-overlay.html");
    if (fs.existsSync(filePath)) res.sendFile(filePath);
    else
      res
        .status(404)
        .send("Events overlay HTML not found (events-overlay.html missing).");
  });

  // Events studio editor HTML
  app.get("/events-studio.html", (req, res) => {
    const filePath = path.join(__dirname, "events-studio.html");
    if (fs.existsSync(filePath)) res.sendFile(filePath);
    else
      res
        .status(404)
        .send("Events studio HTML not found (events-studio.html missing).");
  });

  // Chat dock HTML
  app.get("/dock/chat", (req, res) => {
    const filePath = path.join(__dirname, "chat-dock.html");
    if (fs.existsSync(filePath)) res.sendFile(filePath);
    else
      res
        .status(404)
        .send("Chat dock HTML not found (chat-dock.html missing).");
  });

  // Events dock HTML
  app.get("/dock/events", (req, res) => {
    const filePath = path.join(__dirname, "events-dock.html");
    if (fs.existsSync(filePath)) res.sendFile(filePath);
    else
      res
        .status(404)
        .send("Events dock HTML not found (events-dock.html missing).");
  });

  // ───────────────────────────────────────────────
  // REQUIRED FLOW: Implicit Grant callback landing page
  // ───────────────────────────────────────────────
  app.get("/auth/twitch/callback", (req, res) => {
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.end(`
      <!doctype html>
      <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width,initial-scale=1" />
        <title>Stream Sync – Twitch Connect</title>
      </head>
      <body style="font-family:system-ui;padding:24px;">
        <h2>Finishing Twitch connection…</h2>
        <p>You can close this window if it doesn’t close automatically.</p>

        <script>
          (function () {
            function parseHash(hash) {
              const out = {};
              const h = (hash || "").replace(/^#/, "");
              if (!h) return out;
              for (const part of h.split("&")) {
                const [k, v] = part.split("=");
                if (!k) continue;
                out[decodeURIComponent(k)] = decodeURIComponent(v || "");
              }
              return out;
            }

            async function run() {
              const h = parseHash(window.location.hash);
              const accessToken = h.access_token || "";
              const expiresIn = h.expires_in ? Number(h.expires_in) : null;
              const scope = h.scope ? h.scope.split(" ") : null;
              const tokenType = h.token_type || "";

              if (!accessToken) {
                document.body.innerHTML += "<p style='color:#b91c1c;'>Missing access_token. (Did Twitch return an error?)</p>";
                return;
              }

              try {
                const resp = await fetch("/api/twitch/set-token", {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                    accessToken,
                    expiresIn,
                    scope,
                    tokenType
                  })
                });

                const data = await resp.json().catch(() => ({}));
                if (!resp.ok || !data || !data.ok) {
                  throw new Error((data && (data.error || data.message)) || ("HTTP " + resp.status));
                }

                document.body.innerHTML = "<h2>Stream Sync connected to Twitch ✅</h2><p>You can close this window and return to the app.</p>";
                setTimeout(() => window.close(), 500);
              } catch (e) {
                console.error(e);
                document.body.innerHTML += "<p style='color:#b91c1c;'>Failed to finalize connection. Check Stream Sync logs.</p>";
              }
            }

            run();
          })();
        </script>
      </body>
      </html>
    `);
  });

  // ───────────────────────────────────────────────
  // Existing /config/:profileId.json for chat dock
  // ───────────────────────────────────────────────

  app.get("/config/:profileId.json", (req, res) => {
    const profileId = req.params.profileId || "chat-default";

    const profileCfg =
      (dockConfig.profiles && dockConfig.profiles[profileId]) || {};

    const fontSize = Number(profileCfg.fontSize) || 13;
    const showBadges = profileCfg.showBadges === false ? false : true;
    const showTimestamps = profileCfg.showTimestamps === false ? false : true;

    res.json({
      id: profileId,
      font: {
        family: "Segoe UI",
        size: fontSize,
        lineHeight: 1.35,
      },
      chat: {
        enabled: true,
        showBadges,
        showEmotes: true,
        showTimestamps,
      },
    });
  });

  // Save chat dock config
  app.post("/api/chat/dock-config", (req, res) => {
    try {
      const {
        profileId = "chat-default",
        fontSize,
        showBadges,
        showTimestamps,
      } = req.body || {};

      const sizeNum = Number(fontSize) || 13;
      dockConfig.profiles = dockConfig.profiles || {};
      const profile = dockConfig.profiles[profileId] || {};

      profile.fontSize = sizeNum;
      if (typeof showBadges === "boolean") profile.showBadges = showBadges;
      if (typeof showTimestamps === "boolean")
        profile.showTimestamps = showTimestamps;

      dockConfig.profiles[profileId] = profile;
      saveDockConfig();

      broadcastEventToAll({
        type: "dock-config-updated",
        profileId,
        profile,
      });

      res.json({ ok: true, profileId, profile });
    } catch (err) {
      console.error("[OverlayServer] Failed to save dock config:", err);
      res.status(500).json({ ok: false, error: String(err.message || err) });
    }
  });

  // ───────────────────────────────────────────────
  // Events dock config (GLOBAL) — autosave + push to OBS in real time
  // ───────────────────────────────────────────────

  app.get("/api/events/dock-config", (req, res) => {
    res.json({ ok: true, config: eventsDockConfig });
  });

  app.post("/api/events/dock-config", (req, res) => {
    try {
      const cfg = req.body || {};
      if (!cfg || typeof cfg !== "object") {
        return res.status(400).json({ ok: false, error: "invalid-config" });
      }

      eventsDockConfig = {
        ...eventsDockConfig,
        ...cfg,
        events: {
          ...eventsDockConfig.events,
          ...(cfg.events || {}),
        },
      };

      saveDockConfig();

      // Push live update to connected docks in OBS
      broadcastEventToAll({
        type: "events-dock-config",
        config: eventsDockConfig,
      });

      res.json({ ok: true, config: eventsDockConfig });
    } catch (err) {
      console.error("[OverlayServer] Failed to save events dock config:", err);
      res.status(500).json({ ok: false, error: String(err.message || err) });
    }
  });

  // Upload a local font for chat overlay
  app.post("/api/chat/upload-font", (req, res) => {
    try {
      const { profileId = "chat-default", fileName, contentBase64 } =
        req.body || {};

      if (!fileName || !contentBase64) {
        return res.status(400).json({ ok: false, error: "missing-fields" });
      }

      const safeBase = String(fileName).replace(/[^a-z0-9_.-]/gi, "_");
      const ext = (path.extname(safeBase) || ".ttf").toLowerCase();

      const allowedExt = new Set([".ttf", ".otf", ".woff", ".woff2"]);
      if (!allowedExt.has(ext)) {
        return res
          .status(400)
          .json({ ok: false, error: "unsupported-font-type" });
      }

      const base = path.basename(safeBase, ext);
      const ts = Date.now();
      const storedName = `${base}-${ts}${ext}`;
      const destPath = path.join(PATHS.fontsDir, storedName);

      storage.ensureDir(PATHS.fontsDir);

      let buffer;
      try {
        buffer = Buffer.from(String(contentBase64), "base64");
      } catch {
        return res.status(400).json({ ok: false, error: "invalid-base64" });
      }

      if (!buffer || buffer.length < 16) {
        return res.status(400).json({ ok: false, error: "invalid-font-data" });
      }

      fs.writeFileSync(destPath, buffer);

      overlayConfig.profiles = overlayConfig.profiles || {};
      const profile =
        overlayConfig.profiles[profileId] ||
        (overlayConfig.profiles[profileId] = {});
      const fontFamily = `OverlayLocal_${profileId}`;
      const fontUrl = `/fonts/${storedName}`;

      profile.fontFamily = fontFamily;
      profile.localFontUrl = fontUrl;

      saveOverlayConfig();

      broadcastEventToAll({
        type: "overlay-config-updated",
        profileId,
      });

      res.json({ ok: true, profileId, fontFamily, fontUrl });
    } catch (err) {
      console.error("[OverlayServer] Failed to upload overlay font:", err);
      res.status(500).json({ ok: false, error: "upload-failed" });
    }
  });

  // Get overlay config for a profile
  
  // ───────────────────────────────────────────────
  // Overlay profile helpers
  // ───────────────────────────────────────────────

  function normalizeChatOverlayProfileId(id) {
    const v = (id || "chat-default").toString().trim();
    // Back-compat: older UI/URLs used "default"
    if (v === "default") return "chat-default";
    return v || "chat-default";
  }

  
// List chat overlay profiles (for Integrations dropdown)
// NOTE: Treat legacy id "default" as an alias of "chat-default".
app.get("/api/chat/overlay-profiles", (req, res) => {
  overlayConfig.profiles = overlayConfig.profiles || {};

  // Backfill/migrate legacy "default" -> "chat-default" so we don't show duplicates.
  if (overlayConfig.profiles.default && !overlayConfig.profiles["chat-default"]) {
    overlayConfig.profiles["chat-default"] = overlayConfig.profiles.default;
    delete overlayConfig.profiles.default;
    try { saveOverlayConfig(); } catch (_) {}
  }

  const rawIds = Object.keys(overlayConfig.profiles || {});

  // Always include the built-in default.
  const ids = new Set(rawIds.map((x) => String(x || "").trim()).filter(Boolean));
  ids.add("chat-default");

  // Filter out obvious non-chat ids
  const filtered = Array.from(ids).filter((id) => {
    if (id === "chat-default") return true;
    if (id.startsWith("chat-")) return true;
    if (id.startsWith("events-") || id === "events-default" || id.startsWith("profile-")) return false;
    // If a weird id exists in the file, keep it (power users)
    return true;
  });

  filtered.sort((a, b) => {
    const rank = (id) => (id === "chat-default" ? 0 : 1);
    const ra = rank(a), rb = rank(b);
    if (ra != rb) return ra - rb;
    return a.localeCompare(b);
  });

  res.json({
    ok: true,
    profiles: filtered.map((id) => {
      const cfg = (overlayConfig.profiles && overlayConfig.profiles[id]) || null;
      const displayName =
        (cfg && (cfg.profileName || cfg.displayName || cfg.name)) ||
        (id === "chat-default" ? "Default" : id);
      return { id, name: String(displayName) };
    }),
  });
});


app.get("/api/chat/overlay-config", (req, res) => {
    const profileId = normalizeChatOverlayProfileId(req.query.profile || "chat-default");

    overlayConfig.profiles = overlayConfig.profiles || {};
    const profile =
      (overlayConfig.profiles && overlayConfig.profiles[profileId]) ||
      overlayConfig.profiles["chat-default"];

    const {
      showTimestamps = true,
      showBadges = true,
      fontSize = 18,
      fontFamily = "system-ui",
      localFontUrl = null,
      textRotate = 0,
      textSkew = 0,
      feedDirection = "up-down",
      messageStyle = "bubble",
      bubbleRadius = 0,
      bubbleColorMode = "fixed",
      bubbleColor = "#000000",
      bgMode = "transparent",
      bgColor = "#000000",
      bgGradient = "",
      displayMode = "solid",
      popupDuration = 8,
      strokeEnabled = false,
      strokeColor = "#000000",
      strokeWidth = 0,
      bubbleAlpha = 1,
      popupExitStyle = "fade",
    } = profile || {};

    res.json({
      profileId,
      showTimestamps,
      showBadges,
      fontSize,
      fontFamily,
      fontUrl: localFontUrl || null,
      textRotate,
      textSkew,
      feedDirection,
      messageStyle,
      bubbleRadius,
      bubbleColorMode,
      bubbleColor,
      bgMode,
      bgColor,
      bgGradient,
      displayMode,
      popupDuration,
      strokeEnabled,
      strokeColor,
      strokeWidth,
      bubbleAlpha,
      popupExitStyle,
    });
  });

  // Update overlay config for a profile
  app.post("/api/chat/overlay-config", (req, res) => {
    try {
      const {
        profileId = "chat-default",
        showTimestamps,
        showBadges,
        fontSize,
        fontFamily,
        localFontUrl,
        textRotate,
        textSkew,
        feedDirection,
        messageStyle,
        bubbleRadius,
        bubbleColorMode,
        bubbleColor,
        bgMode,
        bgColor,
        bgGradient,
        displayMode,
        popupDuration,
        strokeEnabled,
        strokeColor,
        strokeWidth,
        bubbleAlpha,
        popupExitStyle,
      } = req.body || {};

      overlayConfig.profiles = overlayConfig.profiles || {};
      const profile =
        overlayConfig.profiles[profileId] ||
        (overlayConfig.profiles[profileId] = {});

      if (typeof showTimestamps === "boolean")
        profile.showTimestamps = showTimestamps;
      if (typeof showBadges === "boolean") profile.showBadges = showBadges;

      const sizeNum = Number(fontSize);
      if (Number.isFinite(sizeNum) && sizeNum > 0) profile.fontSize = sizeNum;

      if (typeof fontFamily === "string" && fontFamily.trim())
        profile.fontFamily = fontFamily.trim();
      if (typeof localFontUrl === "string") profile.localFontUrl = localFontUrl;

      const rotNum = Number(textRotate);
      if (Number.isFinite(rotNum)) profile.textRotate = rotNum;

      const skewNum = Number(textSkew);
      if (Number.isFinite(skewNum)) profile.textSkew = skewNum;

      if (typeof feedDirection === "string" && feedDirection)
        profile.feedDirection = feedDirection;
      if (typeof messageStyle === "string" && messageStyle)
        profile.messageStyle = messageStyle;

      const radiusNum = Number(bubbleRadius);
      if (Number.isFinite(radiusNum)) profile.bubbleRadius = radiusNum;

      if (typeof bubbleColorMode === "string" && bubbleColorMode)
        profile.bubbleColorMode = bubbleColorMode;
      if (typeof bubbleColor === "string" && bubbleColor)
        profile.bubbleColor = bubbleColor;

      if (typeof bgMode === "string" && bgMode) profile.bgMode = bgMode;
      if (typeof bgColor === "string" && bgColor) profile.bgColor = bgColor;
      if (typeof bgGradient === "string") profile.bgGradient = bgGradient;

      if (typeof displayMode === "string" && displayMode)
        profile.displayMode = displayMode;

      const popupNum = Number(popupDuration);
      if (Number.isFinite(popupNum)) profile.popupDuration = popupNum;

      if (typeof strokeEnabled === "boolean")
        profile.strokeEnabled = strokeEnabled;
      if (typeof strokeColor === "string" && strokeColor)
        profile.strokeColor = strokeColor;

      const strokeWidthNum = Number(strokeWidth);
      if (Number.isFinite(strokeWidthNum) && strokeWidthNum >= 0)
        profile.strokeWidth = strokeWidthNum;

      const alphaNum = Number(bubbleAlpha);
      if (Number.isFinite(alphaNum) && alphaNum >= 0 && alphaNum <= 1)
        profile.bubbleAlpha = alphaNum;

      if (typeof popupExitStyle === "string" && popupExitStyle)
        profile.popupExitStyle = popupExitStyle;

      saveOverlayConfig();
      res.json({ ok: true, profileId });
    } catch (err) {
      console.error("[OverlayServer] Failed to update overlay config:", err);
      res.status(500).json({ ok: false, error: "overlay-config-failed" });
    }
  });

  // Delete a chat overlay profile (server-side persisted config)
  // NOTE: The built-in default profile cannot be deleted.
  app.delete("/api/chat/overlay-config", (req, res) => {
    try {
      const profileId = normalizeChatOverlayProfileId(req.query.profile || "chat-default");
      if (profileId === "chat-default") {
        return res.status(400).json({ ok: false, error: "cannot-delete-default" });
      }
      overlayConfig.profiles = overlayConfig.profiles || {};
      if (Object.prototype.hasOwnProperty.call(overlayConfig.profiles, profileId)) {
        delete overlayConfig.profiles[profileId];
        saveOverlayConfig();
      }
      res.json({ ok: true, profileId });
    } catch (err) {
      console.error("[OverlayServer] Failed to delete chat overlay profile:", err);
      res.status(500).json({ ok: false, error: String(err.message || err) });
    }
  });

  // Health check
  app.get("/health", (req, res) => {
    res.json({ ok: true, service: "overlay-server" });
  });

  // Status endpoint for Connections tab
  app.get("/api/status", (req, res) => {
    res.json({
      twitch: {
        connected: twitchStatus.connected,
        channel: twitchStatus.channel,
        login: twitchAuth.login,
        userId: twitchAuth.userId,
      },
    });
  });

  // ───────────────────────────────────────────────
  // Auth URL for Twitch (Implicit Grant)
  // ───────────────────────────────────────────────
  app.get("/api/twitch/auth-url", (req, res) => {
    if (!TWITCH_CLIENT_ID || !TWITCH_REDIRECT_URI) {
      console.warn(
        "[OverlayServer] TWITCH_CLIENT_ID or TWITCH_REDIRECT_URI not set; auth URL may be invalid"
      );
    }

    const scopes = [
      "chat:read",
      "chat:edit",
      "user:read:chat",
      "user:read:emotes",
      "moderator:read:followers",
      "channel:read:subscriptions",
      "bits:read",
      "channel:read:redemptions",
      "moderator:read:chat_settings",
      "moderator:manage:chat_settings",
      "moderator:manage:chat_messages",
      "moderator:manage:shield_mode",
    ].join(" ");

    const params = new URLSearchParams({
      client_id: TWITCH_CLIENT_ID,
      redirect_uri: TWITCH_REDIRECT_URI,
      response_type: "token",
      scope: scopes,
      force_verify: "true",
    });

    const url = `https://id.twitch.tv/oauth2/authorize?${params.toString()}`;
    res.json({ url });
  });

  // Set token endpoint (Implicit Grant finalize)
  app.post("/api/twitch/set-token", async (req, res) => {
    try {
      const { accessToken, expiresIn, scope } = req.body || {};
      const token = (accessToken || "").toString();

      if (!token) {
        return res
          .status(400)
          .json({ ok: false, error: "missing-access-token" });
      }

      twitchAuth.accessToken = token;
      twitchAuth.refreshToken = null; // implicit flow
      twitchAuth.expiresIn = Number(expiresIn) || null;
      twitchAuth.obtainmentTimestamp = Date.now();
      twitchAuth.scopes = Array.isArray(scope) ? scope : scope || null;

      const v = await validateToken(twitchAuth.accessToken);

      twitchAuth.login = v.login || null;
      twitchAuth.userId = v.user_id || null;
      twitchAuth.scopes = Array.isArray(v.scopes) ? v.scopes : twitchAuth.scopes;

      badgeCache = { sets: null, lastFetch: 0, userId: null };
      emoteCache = { list: null, lastFetch: 0, userId: null };

      saveTwitchTokens();

      await startTwitchClient();
      await startEventSub();

      res.json({ ok: true });
    } catch (err) {
      console.error("[OverlayServer] set-token error:", err);
      res.status(500).json({ ok: false, error: String(err.message || err) });
    }
  });

  // Events overlay config for a profile
  
  // List events overlay profiles (for Integrations dropdown)
  app.get("/api/events/overlay-profiles", (req, res) => {
    eventsOverlayConfig.profiles = eventsOverlayConfig.profiles || {};
    const rawIds = Object.keys(eventsOverlayConfig.profiles || {});

    const ids = new Set(rawIds.map((x) => String(x || "").trim()).filter(Boolean));
    ids.add("default");

    // Filter out obvious chat profile ids
    const filtered = Array.from(ids).filter((id) => {
      if (id === "default") return true;
      if (id.startsWith("chat-") || id === "chat-default") return false;
      return true;
    });

    filtered.sort((a, b) => {
      if (a === "default") return -1;
      if (b === "default") return 1;
      return a.localeCompare(b);
    });

    res.json({
      ok: true,
      profiles: filtered.map((id) => {
        const cfg = (eventsOverlayConfig.profiles && eventsOverlayConfig.profiles[id]) || null;
        const displayName =
          (cfg && (cfg.profileName || cfg.displayName || cfg.name)) ||
          (id === "default" ? "Default" : id);
        return { id, name: String(displayName) };
      }),
    });
  });


app.get("/api/events/overlay-config", (req, res) => {
    const profileId = (req.query.profile || "default").toString();

    const profile =
      (eventsOverlayConfig.profiles && eventsOverlayConfig.profiles[profileId]) ||
      (eventsOverlayConfig.profiles && eventsOverlayConfig.profiles["default"]) ||
      _defaultEventsOverlayProfile();

    const exists = !!(eventsOverlayConfig.profiles && Object.prototype.hasOwnProperty.call(eventsOverlayConfig.profiles, profileId));

    res.json({ ok: true, profileId, exists, config: profile });
  });

  // Save events overlay config for a profile
  app.post("/api/events/overlay-config", (req, res) => {
    try {
      const profileId = (req.query.profile || "default").toString();
      const { config } = req.body || {};

      if (!config || typeof config !== "object") {
        return res.status(400).json({ ok: false, error: "missing-config" });
      }

      eventsOverlayConfig.profiles = eventsOverlayConfig.profiles || {};
      eventsOverlayConfig.profiles[profileId] = config;
      saveEventsOverlayConfig();

      broadcastEventToAll({ type: "events-overlay-config-updated", profileId });

      res.json({ ok: true, profileId });
    } catch (err) {
      console.error("[OverlayServer] Failed to save events overlay config:", err);
      res.status(500).json({ ok: false, error: String(err.message || err) });
    }
  });

  // Delete events overlay profile (server-side persisted config)
  // NOTE: "default" profile cannot be deleted.
  app.delete("/api/events/overlay-config", (req, res) => {
    try {
      const profileId = (req.query.profile || "default").toString();
      if (profileId === "default") {
        return res.status(400).json({ ok: false, error: "cannot-delete-default" });
      }
      eventsOverlayConfig.profiles = eventsOverlayConfig.profiles || {};
      if (Object.prototype.hasOwnProperty.call(eventsOverlayConfig.profiles, profileId)) {
        delete eventsOverlayConfig.profiles[profileId];
        saveEventsOverlayConfig();
      }
      res.json({ ok: true, profileId });
    } catch (err) {
      console.error("[OverlayServer] Failed to delete events overlay profile:", err);
      res.status(500).json({ ok: false, error: String(err.message || err) });
    }
  });

  // Broadcast a test alert to overlays for a profile
  app.post("/api/events/test-alert", (req, res) => {
    try {
      const body = req.body || {};
      const profileId = (req.query.profile || body.profile || "default").toString();
      const eventType = (body.eventType || body.eventKey || "follow").toString();

      const rawVars =
        (body.data && body.data.variables) || body.variables || body.data || {};

      const variables = normalizeEventVariables(rawVars);

      const alertPayload = {
        type: "event-alert",
        eventType,
        data: { variables },
      };
      if (body.variationId) alertPayload.variationId = body.variationId;
      if (body.soundVolume != null && body.soundVolume !== "") {
        alertPayload.soundVolume = Number(body.soundVolume);
      }

      // keep overlay alert behavior
      broadcastEvent(profileId, alertPayload);

      // ALSO feed the dock (global) during tests
      const et = eventType.toLowerCase();
      const name = variables?.name || variables?.user || "Someone";

      let detail = "";
      if (et === "follow") detail = `${name} followed`;
      else if (et === "sub") detail = formatSubDockDetail(name, variables?.tier ?? variables?.amount);
      else if (et === "resub")
        detail = formatResubDockDetail(
          name,
          variables?.months,
          variables?.tier ?? variables?.amount,
          variables?.input
        );
      else if (et === "gift")
        detail = formatGiftDockDetail(
          name,
          variables?.amount,
          variables?.tier,
          variables?.recipient
        );
      else if (et === "cheer" || et === "bits") detail = `${name} cheered ${variables?.amount || variables?.bits || ""}${variables?.input ? `: ${variables.input}` : ""}`;
      else if (et === "raid") detail = `${name} raided${variables?.amount ? ` with ${variables.amount}` : ""}`;
      else if (et === "redeem") detail = `${variables?.reward || "Redeem"} — ${name}${variables?.input ? `: ${variables.input}` : ""}`;
      else detail = `${name} triggered ${eventType}`;

      // map cheer->bits for dock
      const dockType = et === "cheer" ? "bits" : et;

      broadcastEventToAll(
        makeDockEvent({
          eventType: dockType,
          label: eventType,
          detail,
        })
      );

      res.json({ ok: true, profileId, eventType });
    } catch (err) {
      console.error("[OverlayServer] Failed to broadcast test alert:", err);
      res.status(500).json({ ok: false, error: String(err.message || err) });
    }
  });

  // Disconnect Twitch
  app.post("/api/twitch/disconnect", async (req, res) => {
    try {
      stopTwitchClient();
      stopEventSub();

      twitchAuth.accessToken = null;
      twitchAuth.refreshToken = null;
      twitchAuth.expiresIn = null;
      twitchAuth.obtainmentTimestamp = null;
      twitchAuth.login = null;
      twitchAuth.userId = null;
      twitchAuth.scopes = null;

      try {
        fs.unlinkSync(PATHS.twitchTokens);
      } catch (_) {}

      badgeCache = { sets: null, lastFetch: 0, userId: null };
      emoteCache = { list: null, lastFetch: 0, userId: null };

      res.json({ ok: true });
    } catch (err) {
      console.error("[OverlayServer] Failed to disconnect Twitch:", err);
      res.status(500).json({ ok: false, error: String(err.message || err) });
    }
  });

  // Badge endpoint
  app.get("/api/twitch/badges/all", async (req, res) => {
    try {
      const badges = await getMergedBadgeSets();
      res.json(badges);
    } catch (err) {
      console.error("[OverlayServer] Failed to fetch Twitch badges:", err);
      res.status(500).json({ error: String(err.message || err) });
    }
  });

  // Emotes endpoint
  app.get("/api/twitch/emotes/all", async (req, res) => {
    try {
      const emotes = await getMergedEmotes();
      res.json({ emotes });
    } catch (err) {
      console.error("[OverlayServer] Failed to fetch Twitch emotes:", err);
      res.status(500).json({ error: String(err.message || err) });
    }
  });

  // ───────────────────────────────────────────────
  // WebSocket: /ws/feed
  // ───────────────────────────────────────────────

  const wss = new WebSocketServer({
    server,
    path: "/ws/feed",
  });

  wss.on("connection", (socket, req) => {
    try {
      const url = new URL(req.url, "http://localhost:" + PORT);
      const profileId = url.searchParams.get("profile") || "default";

      if (!feedClients.has(profileId)) {
        feedClients.set(profileId, new Set());
      }
      feedClients.get(profileId).add(socket);

      console.log(
        "[OverlayServer] WebSocket client connected for profile:",
        profileId
      );

      // Immediately push events dock config to any connected client (OBS dock)
      try {
        socket.send(
          JSON.stringify({ type: "events-dock-config", config: eventsDockConfig })
        );
      } catch (_) {}

      socket.on("message", async (data) => {
        try {
          const msg = JSON.parse(data);
          if (!msg || typeof msg !== "object") return;

          if (msg.type === "ping") {
            socket.send(JSON.stringify({ type: "pong", ts: Date.now() }));
            return;
          }

          if (msg.type === "chat-send") {
            const text = (msg.message || "").toString();
            console.log("[OverlayServer] Received chat-send from dock:", text);
            await sendChatMessageFromDock(text);
            return;
          }

        } catch (err) {
          console.error("[OverlayServer] WS message error:", err);
        }
      });

      socket.on("close", () => {
        console.log(
          "[OverlayServer] WebSocket client disconnected for profile:",
          profileId
        );
        const clients = feedClients.get(profileId);
        if (clients && clients.has(socket)) {
          clients.delete(socket);
          if (clients.size === 0) feedClients.delete(profileId);
        }
      });
    } catch (err) {
      console.error("[OverlayServer] WS connection error:", err);
      try {
        socket.close();
      } catch {
        // ignore
      }
    }
  });

  server.listen(PORT, () => {
    console.log(`[OverlayServer] Listening on http://localhost:${PORT}`);
  });

  // Auto-connect if we have tokens
  if (twitchAuth.accessToken && twitchAuth.login) {
    console.log(
      "[OverlayServer] Found saved Twitch tokens; trying to start chat + EventSub."
    );
    startTwitchClient()
      .then(() => startEventSub())
      .catch((err) => {
        console.error(
          "[OverlayServer] Failed to start clients from saved tokens:",
          err
        );
      });
  }
}

module.exports = {
  startOverlayServer,
  broadcastEvent,
  broadcastEventToAll,
};
