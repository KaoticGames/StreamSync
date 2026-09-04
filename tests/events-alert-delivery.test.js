"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

require(path.join(__dirname, "..", "events-alert-delivery.js"));

const deliveryApi = globalThis.StreamSyncAlertDelivery;
assert.ok(deliveryApi, "StreamSyncAlertDelivery must load onto globalThis");

/**
 * Deferred playOne: each alert gets a promise the test resolves explicitly.
 * No timers / sleep — completion order is fully under test control.
 */
function createDeferredPlayOne() {
  const started = [];
  const completed = [];
  /** @type {Map<string, () => void>} */
  const resolvers = new Map();

  function playOne(alert) {
    const id = alert.id;
    started.push(id);
    return new Promise((resolve) => {
      resolvers.set(id, () => {
        completed.push(id);
        resolve();
      });
    });
  }

  function resolve(id) {
    const fn = resolvers.get(id);
    assert.ok(fn, `playOne for ${id} must have started before resolve`);
    fn();
  }

  return { playOne, started, completed, resolve };
}

describe("StreamSyncAlertDelivery FIFO (Pass 1 — expected to fail)", () => {
  it("three rapid alerts complete in enqueue order A then B then C", async () => {
    const { playOne, started, completed, resolve } = createDeferredPlayOne();
    const delivery = deliveryApi.createDelivery({ playOne });

    // Enqueue three alerts back-to-back (no await between), like the WS handler.
    const pA = delivery.enqueue({ id: "A" });
    const pB = delivery.enqueue({ id: "B" });
    const pC = delivery.enqueue({ id: "C" });

    // Overlap proof: all three playOne calls start before any finishes.
    assert.deepEqual(
      started,
      ["A", "B", "C"],
      "Pass 1 models today's bug: concurrent playOne starts for A, B, and C"
    );
    assert.deepEqual(completed, [], "nothing should have finished yet");

    // Finish out of enqueue order — legal under concurrent play, illegal under FIFO.
    resolve("C");
    resolve("B");
    resolve("A");

    await Promise.all([pA, pB, pC]);

    // Required FIFO contract (Pass 2+): completion must be A → B → C.
    // Pass 1 delivery starts all plays at once, so completions follow resolve
    // order C → B → A and this assertion fails on purpose.
    assert.deepEqual(
      completed,
      ["A", "B", "C"],
      "alerts must complete in FIFO enqueue order A then B then C"
    );
  });
});
