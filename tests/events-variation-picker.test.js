"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

require(path.join(__dirname, "..", "events-variation-picker.js"));

const picker = globalThis.StreamSyncVariationPicker;
assert.ok(picker, "StreamSyncVariationPicker must load onto globalThis");

function cheerVars(bits) {
  return { bits, amount: bits };
}

function variation(id, trigger, chancePct) {
  return {
    id,
    name: id,
    trigger,
    ...(chancePct != null ? { chancePct } : {}),
  };
}

describe("StreamSyncVariationPicker cheer eligibility", () => {
  it("numeric threshold 100 matches a 100-bit cheer event", () => {
    const v = variation("cheer-100", { mode: "exact", value: 100 });
    assert.equal(
      picker.variationMatchesTrigger(v, "cheer", cheerVars(100)),
      true
    );
  });

  it("numeric threshold 250 does not match a 100-bit cheer event", () => {
    const v = variation("cheer-250", { mode: "exact", value: 250 });
    assert.equal(
      picker.variationMatchesTrigger(v, "cheer", cheerVars(100)),
      false
    );
  });

  it('string threshold "100" is normalized and matches 100 bits', () => {
    const v = variation("cheer-100-str", { mode: "exact", value: "100" });
    assert.equal(
      picker.variationMatchesTrigger(v, "cheer", cheerVars(100)),
      true
    );
  });

  it('string threshold "250" is normalized and does not match 100 bits', () => {
    const v = variation("cheer-250-str", { mode: "exact", value: "250" });
    assert.equal(
      picker.variationMatchesTrigger(v, "cheer", cheerVars(100)),
      false,
      'exact "250" must not match a 100-bit cheer (fail-closed after normalize)'
    );
  });

  it("invalid exact/min values fail closed (do not match)", () => {
    const cases = [
      { mode: "exact", value: "nope" },
      { mode: "exact", value: NaN },
      { mode: "exact", value: null },
      { mode: "exact", value: undefined },
      { mode: "exact", value: "" },
      { mode: "min", value: "nope" },
      { mode: "min", value: {} },
      { mode: "min", value: [] },
    ];
    for (const trigger of cases) {
      const v = variation(`bad-${trigger.mode}-${String(trigger.value)}`, trigger);
      assert.equal(
        picker.variationMatchesTrigger(v, "cheer", cheerVars(100)),
        false,
        `invalid ${trigger.mode} value ${JSON.stringify(trigger.value)} must fail closed`
      );
    }
  });

  it("weighted choice occurs only after eligibility filtering", () => {
    const eventConfig = {
      variations: [
        variation("base", { mode: "none" }),
        variation("eligible-100", { mode: "exact", value: 100 }, 1),
        // Ineligible numeric 250 must never enter the weighted pool for 100 bits.
        variation("ineligible-250", { mode: "exact", value: 250 }, 99),
      ],
    };

    const originalRandom = Math.random;
    Math.random = () => 0.99;
    try {
      for (let i = 0; i < 20; i++) {
        const picked = picker.pickVariation({
          eventConfig,
          eventType: "cheer",
          variables: cheerVars(100),
        });
        assert.equal(
          picked?.id,
          "eligible-100",
          "ineligible 250-bit variation must not be selected for 100 bits"
        );
      }
    } finally {
      Math.random = originalRandom;
    }
  });

  it("string thresholds: 100-bit event must not select ineligible 250 variation", () => {
    // Current bug: Number.isFinite("250") is false, so variationMatchesTrigger
    // fail-opens and the 250-bit rule enters the pool. Controlled RNG forces
    // the ineligible variation when both string rules incorrectly match.
    const eventConfig = {
      variations: [
        variation("base", { mode: "none" }),
        variation("eligible-100-str", { mode: "exact", value: "100" }, 1),
        variation("ineligible-250-str", { mode: "exact", value: "250" }, 99),
      ],
    };

    const originalRandom = Math.random;
    // Bias toward the higher-weight (ineligible) entry if it leaked into the pool.
    Math.random = () => 0.99;
    try {
      const picked = picker.pickVariation({
        eventConfig,
        eventType: "cheer",
        variables: cheerVars(100),
      });
      assert.notEqual(
        picked?.id,
        "ineligible-250-str",
        "100-bit cheer must not select 250-bit variation under string thresholds"
      );
      assert.equal(
        picked?.id,
        "eligible-100-str",
        "100-bit cheer must select the eligible 100-bit string-threshold variation"
      );
    } finally {
      Math.random = originalRandom;
    }
  });

  it("multiple variations at the same eligible threshold respect weights", () => {
    const eventConfig = {
      variations: [
        variation("base", { mode: "none" }),
        variation("a-100", { mode: "exact", value: 100 }, 25),
        variation("b-100", { mode: "exact", value: 100 }, 75),
      ],
    };

    const originalRandom = Math.random;
    try {
      Math.random = () => 0.1; // within first 25 of total 100 → a-100
      assert.equal(
        picker.pickVariation({
          eventConfig,
          eventType: "cheer",
          variables: cheerVars(100),
        })?.id,
        "a-100"
      );

      Math.random = () => 0.5; // within remaining 75 → b-100
      assert.equal(
        picker.pickVariation({
          eventConfig,
          eventType: "cheer",
          variables: cheerVars(100),
        })?.id,
        "b-100"
      );
    } finally {
      Math.random = originalRandom;
    }
  });
});
