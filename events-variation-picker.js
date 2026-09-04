// Shared variation trigger matching + weighted chance (events overlay + studio tests).
(function (root) {
  function variationsForEvent(eventConfig) {
    const vars = Array.isArray(eventConfig?.variations) ? eventConfig.variations : [];
    return vars.filter(Boolean);
  }

  function isFallbackBaseVariation(v) {
    if (!v) return false;
    const mode = v?.trigger?.mode || "none";
    const val = v?.trigger?.value;
    return mode === "none" && (val == null || val === "");
  }

  function baseVariationForEvent(eventConfig, eventType) {
    if (!eventConfig) return null;

    const directBase =
      eventConfig.base || eventConfig.root || eventConfig.defaultVariation || null;
    if (directBase) return directBase;

    const vars = variationsForEvent(eventConfig);
    if (!vars.length) return null;

    const marked =
      vars.find((v) => v && (v.isBase === true || v.isRoot === true || v.base === true)) ||
      vars.find((v) => v && String(v.id || "").toLowerCase() === "base") ||
      vars.find((v) => v && String(v.name || "").toLowerCase().includes("base"));
    if (marked) return marked;

    const explicitNone = vars.find(isFallbackBaseVariation);
    if (explicitNone) return explicitNone;

    // Native profiles: first row is the base alert. SE imports used to put a conditional
    // variation first — do not treat it as the catch-all fallback.
    if (isFallbackBaseVariation(vars[0])) return vars[0];
    return null;
  }

  function normalizeTier(tierOrAmount) {
    const n = Number(tierOrAmount);
    if (n === 1 || n === 2 || n === 3) return n;
    if (n === 1000) return 1;
    if (n === 2000) return 2;
    if (n === 3000) return 3;
    const s = String(tierOrAmount ?? "").trim();
    if (s === "1000" || s === "Prime") return 1;
    if (s === "2000") return 2;
    if (s === "3000") return 3;
    return null;
  }

  function eventNumericValue(eventType, variables) {
    const v = variables || {};
    if (eventType === "sub" || eventType === "resub") return 1;
    if (eventType === "gift") {
      const q = Number(v.amount ?? v.total);
      return Number.isFinite(q) && q > 0 ? q : 1;
    }
    if (eventType === "cheer") return Number(v.bits ?? v.amount) || 0;
    if (eventType === "raid") return Number(v.raiders ?? v.viewers ?? v.amount) || 0;
    return Number(v.amount) || 0;
  }

  /** Coerce finite numbers and numeric strings; reject objects/arrays/empty/non-finite. */
  function coerceTriggerThreshold(val) {
    if (typeof val === "number") return Number.isFinite(val) ? val : NaN;
    if (typeof val === "string") {
      const trimmed = val.trim();
      if (trimmed === "") return NaN;
      const n = Number(trimmed);
      return Number.isFinite(n) ? n : NaN;
    }
    return NaN;
  }

  function variationMatchesTrigger(v, eventType, variables) {
    const t = v?.trigger || {};
    const eventTier = normalizeTier(variables?.tier ?? variables?.amount);

    if (eventType === "sub" || eventType === "resub") {
      const reqTier = Number(t.tier);
      if (![1, 2, 3].includes(reqTier)) return true;
      return eventTier === reqTier;
    }

    if (eventType === "gift") {
      const qty = eventNumericValue(eventType, variables);
      const mode = t.mode || "none";
      const val = coerceTriggerThreshold(t.value);
      let qtyOk = true;
      if (mode === "exact") {
        qtyOk = Number.isFinite(val) && qty === val;
      } else if (mode === "min") {
        qtyOk = Number.isFinite(val) && qty >= val;
      }

      const reqTier = Number(t.tier);
      let tierOk = true;
      if ([1, 2, 3].includes(reqTier)) tierOk = eventTier === reqTier;

      return qtyOk && tierOk;
    }

    const mode = t.mode || "none";
    const val = coerceTriggerThreshold(t.value);
    const num = eventNumericValue(eventType, variables);
    if (mode === "none") return true;
    if (mode === "exact") {
      if (!Number.isFinite(val)) return false;
      return num === val;
    }
    if (mode === "min") {
      if (!Number.isFinite(val)) return false;
      return num >= val;
    }
    return true;
  }

  /**
   * When several variations match, narrow the pool before chance:
   * - exact beats min/none
   * - among multiple min rules, highest threshold wins (e.g. min 1 + min 100 @ 150 bits → min 100)
   */
  function narrowMatchingPool(matching, eventType, variables) {
    if (!matching.length) return matching;
    if (matching.length === 1) return matching;

    const exact = matching.filter((v) => (v?.trigger?.mode || "none") === "exact");
    if (exact.length) return exact;

    const min = matching.filter((v) => (v?.trigger?.mode || "none") === "min");
    if (min.length) {
      const num = eventNumericValue(eventType, variables);
      let bestThreshold = -Infinity;
      for (const v of min) {
        const val = coerceTriggerThreshold(v?.trigger?.value);
        if (Number.isFinite(val) && num >= val && val > bestThreshold) {
          bestThreshold = val;
        }
      }
      if (Number.isFinite(bestThreshold)) {
        const top = min.filter(
          (v) => coerceTriggerThreshold(v?.trigger?.value) === bestThreshold
        );
        if (top.length) return top;
      }
      return min;
    }

    return matching;
  }

  function weightedPick(items) {
    if (!items.length) return null;
    if (items.length === 1) return items[0];

    const weights = items.map((v) => {
      const c = v?.chancePct;
      return Number.isFinite(c) && c > 0 ? c : null;
    });
    const hasWeights = weights.some((w) => w != null);
    if (!hasWeights) {
      return items[Math.floor(Math.random() * items.length)];
    }

    const ws = weights.map((w) => (w == null ? 0 : w));
    const total = ws.reduce((a, b) => a + b, 0);
    if (total <= 0) return items[Math.floor(Math.random() * items.length)];

    let r = Math.random() * total;
    for (let i = 0; i < items.length; i++) {
      r -= ws[i];
      if (r <= 0) return items[i];
    }
    return items[items.length - 1];
  }

  function pickVariation({ eventConfig, eventType, variationId, variables }) {
    const vars = variationsForEvent(eventConfig);
    if (!vars.length) return null;

    if (variationId) {
      const found = vars.find((v) => v && v.id === variationId);
      if (found) return found;
    }

    const base = baseVariationForEvent(eventConfig, eventType);
    const extras = vars.filter((v) => v && (!base || v.id !== base.id));
    const matching = extras.filter((v) =>
      variationMatchesTrigger(v, eventType, variables || {})
    );
    const pool = matching.length
      ? narrowMatchingPool(matching, eventType, variables || {})
      : base
        ? [base]
        : [];
    return weightedPick(pool) || base || null;
  }

  function describePick({ eventConfig, eventType, variables, variationId }) {
    const picked = pickVariation({ eventConfig, eventType, variationId, variables });
    const vars = variationsForEvent(eventConfig);
    const base = baseVariationForEvent(eventConfig, eventType);
    const extras = vars.filter((v) => v && (!base || v.id !== base.id));
    const matching = extras.filter((v) =>
      variationMatchesTrigger(v, eventType, variables || {})
    );
    return {
      picked,
      matchingCount: matching.length,
      totalVariations: vars.length,
    };
  }

  root.StreamSyncVariationPicker = {
    variationsForEvent,
    baseVariationForEvent,
    normalizeTier,
    eventNumericValue,
    variationMatchesTrigger,
    narrowMatchingPool,
    weightedPick,
    pickVariation,
    describePick,
  };
})(typeof window !== "undefined" ? window : globalThis);
