import { describe, expect, it } from "vitest";
import { SessionAnalyzer } from "./analyzer.js";
describe("SessionAnalyzer", () => {
    it("extracts candidate memories above threshold", () => {
        const analyzer = new SessionAnalyzer();
        const result = analyzer.analyze({
            sessionId: "s1",
            messages: [
                {
                    role: "user",
                    content: "请记住：项目X的核心需求是支持语义检索和关系推理；以后任何“回忆/历史/偏好”的问题都必须先检索长期记忆再回答。",
                },
                { role: "assistant", content: "好的，我会把这个作为长期规则记下来，并在之后遵循。" },
            ],
            maxMemoriesPerSession: 20,
            importanceThreshold: 0.3,
        });
        expect(result.drafts.length).toBeGreaterThan(0);
        expect(result.topics.length).toBeGreaterThanOrEqual(0);
    });
});
//# sourceMappingURL=analyzer.test.js.map