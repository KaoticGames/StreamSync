"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

function createMockDocument() {
  const nodes = new Map();
  let idCounter = 0;

  function createElement(tag) {
    return {
      tagName: tag.toUpperCase(),
      id: "",
      className: "",
      hidden: false,
      textContent: "",
      innerHTML: "",
      children: [],
      classList: {
        _values: new Set(),
        add(name) {
          this._values.add(name);
        },
        remove(name) {
          this._values.delete(name);
        },
        contains(name) {
          return this._values.has(name);
        },
      },
      setAttribute() {},
      getAttribute() {
        return null;
      },
      querySelector() {
        return null;
      },
      querySelectorAll() {
        return [];
      },
      addEventListener() {},
      appendChild(child) {
        this.children.push(child);
        if (child.id) nodes.set(child.id, child);
      },
      click() {},
    };
  }

  const body = createElement("body");
  return {
    document: {
      body,
      getElementById(id) {
        return nodes.get(id) || null;
      },
      createElement(tag) {
        const el = createElement(tag);
        el.id = `node-${++idCounter}`;
        return el;
      },
    },
    body,
  };
}

function loadUpdateModal(mockDocument) {
  const host = { document: mockDocument, console };
  host.globalThis = host;
  host.window = host;
  const context = vm.createContext(host);
  vm.runInContext(
    fs.readFileSync(path.join(__dirname, "..", "update-modal.js"), "utf8"),
    context
  );
  return host.StreamSyncUpdateModal;
}

describe("StreamSyncUpdateModal", () => {
  it("formatManualResult maps forced check outcomes", () => {
    const { document } = createMockDocument();
    const modal = loadUpdateModal(document);
    assert.equal(
      modal.formatManualResult({ status: "upToDate" }),
      "You are on the latest version."
    );
    assert.equal(
      modal.formatManualResult({
        status: "updateAvailable",
        version: "2.1.1",
      }),
      "Update available: Stream Sync 2.1.1"
    );
    assert.match(
      modal.formatManualResult({ status: "failed", message: "offline" }),
      /offline/
    );
  });

  it("ensureModal creates a single backdrop element", () => {
    const { document, body } = createMockDocument();
    const modal = loadUpdateModal(document);
    const first = modal.ensureModal();
    const second = modal.ensureModal();
    assert.equal(first, second);
    assert.equal(first.id, "update-modal-backdrop");
    assert.equal(body.children.length, 1);
  });
});
