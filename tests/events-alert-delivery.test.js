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

describe("StreamSyncAlertDelivery bounded pending queue (Pass 4)", () => {
  it("drops newest when 32 pending + 1 in flight; active play and FIFO hold", async () => {
    const { playOne, started, completed, resolve } = createDeferredPlayOne();
    const drops = [];
    const warns = [];
    const originalWarn = console.warn;
    console.warn = (...args) => {
      warns.push(args);
    };

    try {
      const delivery = deliveryApi.createDelivery({
        playOne,
        maxPending: 32,
        onDrop(info) {
          drops.push(info);
        },
      });

      // 1 in flight + 32 pending = capacity full; next enqueue is drop-newest.
      const accepted = [];
      for (let i = 0; i < 33; i++) {
        const id = `a${i}`;
        accepted.push({ id, p: delivery.enqueue({ id, eventType: "cheer" }) });
      }
      await Promise.resolve();

      assert.deepEqual(started, ["a0"], "only first alert is in flight");
      assert.equal(started.length, 1, "exactly one playOne in flight");

      const droppedId = "drop-me";
      const pDrop = delivery.enqueue({ id: droppedId, eventType: "cheer" });
      let dropErr;
      try {
        await pDrop;
      } catch (e) {
        dropErr = e;
      }

      assert.ok(dropErr, "dropped enqueue must reject");
      assert.match(
        String(dropErr && dropErr.message),
        /queue_full_drop_newest/,
        "reject Error message must include queue_full_drop_newest"
      );
      assert.equal(started.includes(droppedId), false, "dropped id must never playOne");
      assert.equal(warns.length >= 1, true, "console.warn must be invoked on drop");
      assert.equal(
        warns.some((args) => args.some((a) => String(a).includes("queue_full_drop_newest"))),
        true,
        "console.warn must include queue_full_drop_newest"
      );
      assert.equal(drops.length, 1, "onDrop hook must fire");
      assert.equal(drops[0].reason, "queue_full_drop_newest");
      assert.equal(drops[0].pending, 32);
      assert.equal(drops[0].eventType, "cheer");
      assert.equal(drops[0].alert.id, droppedId);

      // Drain FIFO of accepted alerts; dropped never appears.
      for (const { id, p } of accepted) {
        assert.equal(
          started[started.length - 1],
          id,
          `playOne head must be ${id} before resolve`
        );
        resolve(id);
        await p;
      }

      const acceptedIds = accepted.map((x) => x.id);
      assert.deepEqual(completed, acceptedIds, "accepted complete in FIFO order");
      assert.equal(started.includes(droppedId), false, "dropped id still never playOne'd");
      assert.deepEqual(started, acceptedIds, "playOne order matches accepted FIFO");
    } finally {
      console.warn = originalWarn;
    }
  });
});

describe("StreamSyncAlertDelivery generation gate (Pass 3)", () => {
  it("stale play continuation must not mutate after a newer gen begins", async () => {
    const gate = deliveryApi.createGenerationGate();
    assert.ok(gate, "createGenerationGate must be exported");

    const mutations = [];
    /** @type {((v?: unknown) => void) | null} */
    let resolveAwaitA = null;
    const awaitPointA = new Promise((resolve) => {
      resolveAwaitA = resolve;
    });

    // Mirrors overlay showAlert: begin() at play start, check after each await
    // before mutating DOM/audio for this play.
    async function playWithGate(id, awaitPoint) {
      const gen = gate.begin();
      mutations.push(`${id}:start`);

      await awaitPoint;
      if (!gate.isCurrent(gen)) {
        mutations.push(`${id}:stale-bail`);
        return { completed: false, gen };
      }

      mutations.push(`${id}:mutate`);
      return { completed: true, gen };
    }

    const pA = playWithGate("A", awaitPointA);
    await Promise.resolve();

    assert.deepEqual(mutations, ["A:start"], "A has begun and is parked at await");
    assert.equal(gate.isCurrent(1), true);

    // Newer play bumps gen (as hideAll / accepted play start does).
    const pB = playWithGate("B", Promise.resolve());
    const resultB = await pB;

    assert.equal(resultB.completed, true, "B owns the slot and may mutate");
    assert.equal(resultB.gen, 2);
    assert.deepEqual(mutations, ["A:start", "B:start", "B:mutate"]);
    assert.equal(gate.isCurrent(1), false, "A's gen is no longer current");
    assert.equal(gate.isCurrent(2), true);

    // Stale continuation of A resumes — must not record a mutation / complete as owner.
    resolveAwaitA();
    const resultA = await pA;

    assert.equal(resultA.completed, false, "stale A must not complete as slot owner");
    assert.equal(resultA.gen, 1);
    assert.deepEqual(
      mutations,
      ["A:start", "B:start", "B:mutate", "A:stale-bail"],
      "A must bail without a mutate after B began"
    );
  });
});

describe("StreamSyncAlertDelivery edge lifecycle (Pass 5)", () => {
  it("playOne rejection: B still plays; queue does not stick busy", async () => {
    const started = [];
    const completed = [];
    /** @type {((err: Error) => void) | null} */
    let rejectA = null;
    /** @type {(() => void) | null} */
    let resolveB = null;

    function playOne(alert) {
      const id = alert.id;
      started.push(id);
      if (id === "A") {
        return new Promise((_, reject) => {
          rejectA = reject;
        });
      }
      return new Promise((resolve) => {
        resolveB = () => {
          completed.push(id);
          resolve();
        };
      });
    }

    const delivery = deliveryApi.createDelivery({ playOne });
    const pA = delivery.enqueue({ id: "A" });
    const pB = delivery.enqueue({ id: "B" });
    await Promise.resolve();

    assert.deepEqual(started, ["A"], "only A starts while in flight");
    assert.ok(rejectA, "A playOne must expose reject");

    rejectA(new Error("playOne A failed"));
    let aErr;
    try {
      await pA;
    } catch (e) {
      aErr = e;
    }
    assert.ok(aErr, "enqueue A must reject when playOne rejects");
    assert.match(String(aErr && aErr.message), /playOne A failed/);

    // Microtask: drain must clear busy and start B.
    await Promise.resolve();
    assert.deepEqual(started, ["A", "B"], "B must start after A rejects");
    assert.ok(resolveB, "B playOne must have started");

    resolveB();
    await pB;
    assert.deepEqual(completed, ["B"], "B completes after A rejection");
    assert.deepEqual(started, ["A", "B"], "no stuck re-play of A");
  });

  it("same delivery instance survives simulated reconnect; in-flight A then C", async () => {
    const { playOne, started, completed, resolve } = createDeferredPlayOne();
    // One module-level delivery — reconnect must not create a new worker.
    const delivery = deliveryApi.createDelivery({ playOne });

    const pA = delivery.enqueue({ id: "A" });
    await Promise.resolve();
    assert.deepEqual(started, ["A"], "A in flight before reconnect");

    // Simulated WS close/reconnect: overlay only reopens the socket; it must
    // NOT clear the queue, reset generation, or replace alertDelivery.
    // Enqueue C on the same instance while A is still playing.
    const pC = delivery.enqueue({ id: "C" });
    await Promise.resolve();

    assert.deepEqual(started, ["A"], "reconnect must not interrupt in-flight A");
    assert.equal(started.includes("C"), false, "C waits until A finishes");

    resolve("A");
    await pA;
    assert.deepEqual(completed, ["A"], "in-flight A completes after reconnect");
    assert.deepEqual(started, ["A", "C"], "C starts only after A on same queue");

    resolve("C");
    await pC;
    assert.deepEqual(completed, ["A", "C"], "C plays after A; queue not reset");
  });

  it("clampDurationMs matches overlay bounds 800..30000", () => {
    assert.equal(typeof deliveryApi.clampDurationMs, "function");
    assert.equal(deliveryApi.clampDurationMs(0), 800);
    assert.equal(deliveryApi.clampDurationMs(799), 800);
    assert.equal(deliveryApi.clampDurationMs(800), 800);
    assert.equal(deliveryApi.clampDurationMs(6000), 6000);
    assert.equal(deliveryApi.clampDurationMs(30000), 30000);
    assert.equal(deliveryApi.clampDurationMs(999999), 30000);
    assert.equal(deliveryApi.clampDurationMs(Number.NaN), 800);
  });
});
