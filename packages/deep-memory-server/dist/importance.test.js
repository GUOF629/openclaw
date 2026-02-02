import { describe, expect, it } from "vitest";
import { computeImportance } from "./importance.js";
describe("computeImportance", () => {
    it("returns 0 for empty input", () => {
        expect(computeImportance(null)).toBe(0);
        expect(computeImportance(undefined)).toBe(0);
    });
    it("clamps to [0,1] and respects weights", () => {
        const high = computeImportance({
            frequency: 10,
            novelty: 1,
            user_intent: 1,
            length: 2000,
        });
        expect(high).toBeGreaterThan(0.8);
        const low = computeImportance({
            frequency: 0,
            novelty: 0,
            user_intent: 0,
            length: 0,
        });
        expect(low).toBe(0);
    });
});
//# sourceMappingURL=importance.test.js.map