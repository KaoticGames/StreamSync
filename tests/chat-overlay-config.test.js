"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { describe, it } = require("node:test");

const root = path.join(__dirname, "..");

function read(relativePath) {
  return fs.readFileSync(path.join(root, relativePath), "utf8");
}

function extractPreviewMessagesFromOverlayHtml(html) {
  const marker = "const previewMessages = ";
  const start = html.indexOf(marker);
  assert.ok(start >= 0, "chat-overlay.html must define previewMessages");

  const arrayStart = html.indexOf("[", start);
  assert.ok(arrayStart >= 0, "previewMessages array opener not found");

  let depth = 0;
  for (let i = arrayStart; i < html.length; i++) {
    const ch = html[i];
    if (ch === "[") depth += 1;
    else if (ch === "]") {
      depth -= 1;
      if (depth === 0) {
        const literal = html.slice(arrayStart, i + 1);
        return vm.runInThisContext(literal);
      }
    }
  }

  throw new Error("Failed to parse previewMessages array from chat-overlay.html");
}

function createClassList() {
  const values = new Set();
  return {
    add(name) {
      values.add(name);
    },
    remove(name) {
      values.delete(name);
    },
    contains(name) {
      return values.has(name);
    },
    toggle(name, force) {
      if (force === true) values.add(name);
      else if (force === false) values.delete(name);
      else if (values.has(name)) values.delete(name);
      else values.add(name);
    },
  };
}

function createDomElement(tag, { id, type } = {}) {
  const listeners = new Map();
  const el = {
    tagName: String(tag || "div").toUpperCase(),
    id: id || "",
    type: type || "",
    checked: false,
    hidden: false,
    textContent: "",
    innerHTML: "",
    value: "",
    files: null,
    style: {},
    className: "",
    classList: createClassList(),
    children: [],
    options: [],
    setAttribute() {},
    getAttribute() {
      return null;
    },
    querySelector(sel) {
      const match = String(sel || "").match(/#([\w-]+)/);
      if (!match) return null;
      return el._tree?.get(match[1]) || null;
    },
    querySelectorAll() {
      return [];
    },
    appendChild(child) {
      el.children.push(child);
      if (child.id && el._tree) el._tree.set(child.id, child);
      return child;
    },
    addEventListener(type, handler) {
      if (!listeners.has(type)) listeners.set(type, []);
      listeners.get(type).push(handler);
    },
    dispatchEvent(event) {
      const type = event && event.type;
      const list = listeners.get(type) || [];
      for (const handler of list) handler(event);
      return true;
    },
    click() {
      el.dispatchEvent({ type: "click" });
    },
  };

  if (type === "file") {
    let fileValue = "";
    el._setUserSelectedValue = (next) => {
      fileValue = String(next || "");
    };
    Object.defineProperty(el, "value", {
      configurable: true,
      enumerable: true,
      get() {
        return fileValue;
      },
      set(next) {
        if (next && String(next).length > 0) {
          throw new Error("Cannot programmatically set a nonempty file input value");
        }
        fileValue = next == null ? "" : String(next);
      },
    });
  }

  if (tag === "select") {
    Object.defineProperty(el, "value", {
      configurable: true,
      enumerable: true,
      get() {
        const selected = el.options.find((opt) => opt.selected);
        return selected ? selected.value : el.options[0]?.value || "";
      },
      set(next) {
        for (const opt of el.options) opt.selected = opt.value === next;
      },
    });
    el.appendChild = (opt) => {
      el.options.push(opt);
      el.children.push(opt);
      return opt;
    };
  }

  return el;
}

function createOverlayUiDocument({ includeFontUi = true } = {}) {
  const nodes = new Map();

  function register(node) {
    if (node.id) nodes.set(node.id, node);
    node._tree = nodes;
    return node;
  }

  const overlay = register(createDomElement("section", { id: "chat-overlay" }));
  overlay.querySelector = (sel) => {
    const match = String(sel || "").match(/#([\w-]+)/);
    return match ? nodes.get(match[1]) || null : null;
  };

  const ids = [
    "overlay-toggle-timestamps",
    "overlay-toggle-badges",
    "overlay-font-source-google",
    "overlay-font-source-local",
    "overlay-google-font-row",
    "overlay-local-font-row",
    "overlay-google-font",
    "overlay-font-size",
    "overlay-text-rotate",
    "overlay-text-skew",
    "overlay-stroke-width",
    "overlay-stroke-width-value",
    "overlay-stroke-color",
    "overlay-feed-orientation",
    "overlay-style-plain",
    "overlay-style-bubble",
    "overlay-bubble-radius",
    "overlay-bubble-color-fixed",
    "overlay-bubble-color-user",
    "overlay-bubble-color",
    "overlay-bubble-opacity",
    "overlay-bubble-opacity-value",
    "overlay-bg-transparent",
    "overlay-bg-solid",
    "overlay-bg-color",
    "overlay-bg-color-row",
    "overlay-display-solid",
    "overlay-display-popup",
    "overlay-popup-duration-row",
    "overlay-popup-duration",
    "overlay-bubble-shape-row",
    "overlay-bubble-color-row",
    "overlay-bubble-radius-row",
  ];

  for (const id of ids) {
    const tag =
      id.includes("toggle") ? "button" : id.includes("font-source") ? "input" : "div";
    const type = id.includes("font-source") ? "radio" : undefined;
    register(createDomElement(tag, { id, type }));
  }

  const googleSelect = nodes.get("overlay-google-font");
  googleSelect.tagName = "SELECT";
  googleSelect.appendChild({ value: "Montserrat", textContent: "Montserrat", selected: true });

  if (includeFontUi) {
    register(createDomElement("input", { id: "overlay-font-upload", type: "file" }));
    register(createDomElement("p", { id: "overlay-local-font-status" }));
    register(createDomElement("button", { id: "overlay-local-font-choose" }));
  }

  const head = createDomElement("head");
  head.appendChild = (child) => {
    head.children.push(child);
    return child;
  };

  const document = {
    head,
    hidden: false,
    getElementById(id) {
      return nodes.get(id) || null;
    },
    createElement(tag) {
      return createDomElement(tag);
    },
    addEventListener(type, handler) {
      if (type === "change") document._changeHandlers = document._changeHandlers || [];
      if (type === "change") document._changeHandlers.push(handler);
    },
  };

  return { document, nodes, overlay };
}

function loadChatOverlayConfigTestApi(extraHost = {}) {
  let source = read("chat-overlay-config.js");
  source = source.replace(
    /\}\)\(\);\s*$/,
    `  globalThis.__chatOverlayConfigTest = {
      applyServerConfigToUiSettings,
      applySettingsToUI,
      defaultSettings,
      wireOverlayEvents,
    };
})();`
  );

  const storage = new Map();
  const host = {
    console,
    setTimeout,
    clearTimeout,
    URL: {
      createObjectURL() {
        return "blob:mock-font";
      },
    },
    FontFace: class MockFontFace {
      constructor() {}
      load() {
        return Promise.resolve(this);
      }
    },
    ...extraHost,
  };
  host.window = host;
  host.globalThis = host;
  host.document = extraHost.document;
  host.window.localStorage = {
    getItem(key) {
      return storage.has(key) ? storage.get(key) : null;
    },
    setItem(key, value) {
      storage.set(key, String(value));
    },
  };
  host.window.STREAMSYNC_OVERLAY = { baseUrl: "http://localhost:4040" };
  host.window.streamSyncControlApi = extraHost.streamSyncControlApi || {
    privilegedFetch() {
      return Promise.resolve({ ok: true, json: async () => ({}) });
    },
  };
  host.window.FileReader = extraHost.FileReader || class MockFileReader {
    readAsDataURL() {
      this.result = "data:font/ttf;base64,QUJD";
      if (typeof this.onload === "function") this.onload();
    }
  };
  host.document.fonts = { add() {} };

  vm.runInContext(source, vm.createContext(host));
  return host.__chatOverlayConfigTest;
}

describe("chat overlay preview badge payloads", () => {
  it("uses Twitch-style badgesRaw objects in previewMessages", () => {
    const previewMessages = extractPreviewMessagesFromOverlayHtml(
      read("overlay-server/chat-overlay.html")
    );

    for (const message of previewMessages) {
      const badgesRaw = message.user && message.user.badgesRaw;
      assert.ok(
        badgesRaw !== null && typeof badgesRaw === "object" && !Array.isArray(badgesRaw),
        `preview user ${message.user?.name} badgesRaw must be a plain object, got ${JSON.stringify(badgesRaw)}`
      );
    }

    const byName = Object.fromEntries(
      previewMessages.map((message) => [message.user.name, message.user.badgesRaw])
    );

    assert.deepEqual(byName.ModUser, { moderator: "1" });
    assert.deepEqual(byName.SubUser, { subscriber: "1" });
    assert.deepEqual(byName.ClipQueen, { vip: "1" });
    assert.deepEqual(byName.Viewer123, {});
    assert.deepEqual(byName.NightOwl, {});
    assert.deepEqual(byName.AnonFan, {});
  });
});

describe("chat overlay local font settings UI", () => {
  it("exposes persisted-font status and a choose/replace control beside the native file input", () => {
    const html = read("views/chat.html");

    assert.match(
      html,
      /id=["']overlay-local-font-status["']/,
      "chat.html must expose overlay-local-font-status for persisted local font state"
    );
    assert.match(
      html,
      /id=["']overlay-local-font-choose["']/,
      "chat.html must expose a custom choose/replace font button"
    );
    assert.match(
      html,
      /id=["']overlay-font-upload["'][\s\S]*?type=["']file["']/,
      "chat.html must keep the native file input for actual font selection"
    );
  });

  it("reflects restart-style server local font config without repopulating the file input", () => {
    const { document } = createOverlayUiDocument();
    const api = loadChatOverlayConfigTestApi({ document });

    const serverCfg = {
      fontFamily: "OverlayLocal_example",
      fontUrl: "/fonts/example.ttf",
    };
    const settings = api.applyServerConfigToUiSettings(
      { ...api.defaultSettings },
      serverCfg
    );
    api.applySettingsToUI(settings);

    const localRadio = document.getElementById("overlay-font-source-local");
    const fileInput = document.getElementById("overlay-font-upload");
    const statusEl = document.getElementById("overlay-local-font-status");

    assert.equal(settings.fontSource, "local");
    assert.equal(settings.localFontUrl, "/fonts/example.ttf");
    assert.equal(localRadio.checked, true);
    assert.equal(fileInput.value, "");
    assert.match(
      (statusEl.textContent || "").trim(),
      /uploaded font is active/i,
      "persisted local font must be surfaced in status text after restart-style load"
    );
  });

  it("communicates when no uploaded local font is saved", () => {
    const { document } = createOverlayUiDocument();
    const api = loadChatOverlayConfigTestApi({ document });

    api.applySettingsToUI({
      ...api.defaultSettings,
      fontSource: "local",
      localFontUrl: null,
      localFontName: "system-ui",
    });

    const statusEl = document.getElementById("overlay-local-font-status");
    assert.match(
      (statusEl.textContent || "").trim(),
      /no uploaded font saved/i,
      "local mode without a persisted upload must say no uploaded font is saved"
    );
  });

  it("wires choose/replace to the native picker and clears the input after upload", async () => {
    const { document, nodes } = createOverlayUiDocument();
    const fileInput = nodes.get("overlay-font-upload");
    const chooseBtn = nodes.get("overlay-local-font-choose");

    let fileInputClicked = false;
    fileInput.click = () => {
      fileInputClicked = true;
    };

    const api = loadChatOverlayConfigTestApi({
      document,
      streamSyncControlApi: {
        privilegedFetch() {
          return Promise.resolve({
            ok: true,
            json: async () => ({
              fontFamily: "OverlayLocal_uploaded",
              fontUrl: "/fonts/uploaded.ttf",
            }),
          });
        },
      },
    });

    const state = {
      settings: { ...api.defaultSettings, fontSource: "local" },
    };

    api.wireOverlayEvents(
      () => "chat-default",
      () => state.settings,
      (next) => {
        state.settings = next;
      }
    );

    chooseBtn.click();
    assert.equal(
      fileInputClicked,
      true,
      "overlay-local-font-choose must delegate to the native file input click()"
    );

    fileInput.files = [{ name: "Example.ttf" }];
    fileInput._setUserSelectedValue("C:\\fakepath\\Example.ttf");
    fileInput.dispatchEvent({ type: "change" });
    await new Promise((resolve) => setImmediate(resolve));
    await new Promise((resolve) => setImmediate(resolve));

    assert.equal(
      fileInput.value,
      "",
      "successful upload must clear file input value so the same file can be chosen again"
    );
  });
});
