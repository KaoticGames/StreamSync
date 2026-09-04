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

function trackSettled(promise) {
  const state = { settled: false };
  promise.then(
    () => {
      state.settled = true;
    },
    () => {
      state.settled = true;
    }
  );
  return state;
}

describe("StreamSyncAlertDelivery FIFO (Pass 2)", () => {
  it("serializes rapid enqueue: only one playOne in flight; completions A→B→C", async () => {
    const { playOne, started, completed, resolve } = createDeferredPlayOne();
    const delivery = deliveryApi.createDelivery({ playOne });

    // Enqueue three alerts back-to-back (no await between), like the WS handler.
    const pA = delivery.enqueue({ id: "A" });
    const pB = delivery.enqueue({ id: "B" });
    const pC = delivery.enqueue({ id: "C" });

    const settledA = trackSettled(pA);
    const settledB = trackSettled(pB);
    const settledC = trackSettled(pC);
    await Promise.resolve();

    // Only A started until A is resolved; B and C wait in the FIFO.
    assert.deepEqual(started, ["A"], "only A playOne starts while A is in flight");
    assert.deepEqual(completed, [], "nothing should have finished yet");
    assert.equal(settledA.settled, false, "enqueue A still pending until playOne resolves");
    assert.equal(settledB.settled, false, "enqueue B still pending");
    assert.equal(settledC.settled, false, "enqueue C still pending");

    // C's playOne must not have started — resolving C is impossible until A and B finish.
    assert.equal(started.includes("C"), false, "C playOne must not start before A and B");

    resolve("A");
    await pA;
    assert.equal(settledA.settled, true);
    assert.deepEqual(started, ["A", "B"], "B starts only after A finishes");
    assert.deepEqual(completed, ["A"]);
    assert.equal(settledB.settled, false, "B enqueue still pending while B playOne runs");
    assert.equal(started.includes("C"), false, "C still must not have started");

    resolve("B");
    await pB;
    assert.deepEqual(started, ["A", "B", "C"], "C starts only after B finishes");
    assert.deepEqual(completed, ["A", "B"]);
    assert.equal(settledC.settled, false, "C enqueue still pending until C playOne resolves");

    resolve("C");
    await Promise.all([pA, pB, pC]);

    assert.deepEqual(
      completed,
      ["A", "B", "C"],
      "alerts must complete in FIFO enqueue order A then B then C"
    );
  });
});
