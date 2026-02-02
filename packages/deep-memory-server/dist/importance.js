import { clamp } from "./utils.js";
/**
 * Requirement weights:
 * - frequency 30%
 * - novelty 25%
 * - user_intent 30%
 * - length 15%
 */
export function computeImportance(input) {
    if (!input) {
        return 0;
    }
    const frequency = clamp(Number(input.frequency ?? 0), 0, 100);
    const novelty = clamp(Number(input.novelty ?? 0), 0, 1);
    const userIntent = clamp(Number(input.user_intent ?? 0), 0, 1);
    const length = clamp(Number(input.length ?? 0), 0, 10_000);
    // Normalize frequency/length to 0..1 with simple saturating curves.
    const freqScore = clamp(frequency / 10, 0, 1);
    const lenScore = clamp(length / 2000, 0, 1);
    const score = 0.3 * freqScore + 0.25 * novelty + 0.3 * userIntent + 0.15 * lenScore;
    return clamp(score, 0, 1);
}
//# sourceMappingURL=importance.js.map