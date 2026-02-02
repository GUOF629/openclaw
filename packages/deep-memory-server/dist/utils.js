export function clamp(value, min, max) {
    if (!Number.isFinite(value)) {
        return min;
    }
    return Math.max(min, Math.min(max, value));
}
export function safeTrim(value) {
    return typeof value === "string" ? value.trim() : "";
}
export function stableHash(input) {
    // Cheap stable hash for ids; not cryptographic.
    let h = 2166136261;
    for (let i = 0; i < input.length; i += 1) {
        h ^= input.charCodeAt(i);
        h = Math.imul(h, 16777619);
    }
    return (h >>> 0).toString(16);
}
//# sourceMappingURL=utils.js.map