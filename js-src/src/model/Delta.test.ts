import { Delta } from "./Delta";
describe("Delta", () => {
    test("mergeWithPriority removes no-op update fields after cancellation", () => {
        const requested = new Delta({
            entity1: ["entity1", { geometry: [0, 0, 100, 100] }, { geometry: [0, 0, 120, 140] }],
        });

        const applied = new Delta({
            entity1: ["entity1", { geometry: [0, 0, 100, 100] }, { geometry: [0, 0, 120, 140] }],
        });

        requested.mergeWithPriority(applied.reversed());

        expect(requested.isEmpty()).toBe(true);
        expect(requested.change("entity1")).toBeUndefined();
    });

    test("mergeWithPriority keeps non-cancelled update fields", () => {
        const aggregated = new Delta({
            entity1: ["entity1", { geometry: [0, 0, 100, 100] }, { geometry: [0, 0, 120, 140] }],
        });

        aggregated.mergeWithPriority(new Delta({
            entity1: ["entity1", { color: "red" }, { color: "blue" }],
        }));

        const change = aggregated.change("entity1");
        expect(change).toBeDefined();
        expect(change?.reverseComponent).toEqual({
            color: "red",
            geometry: [0, 0, 100, 100],
        });
        expect(change?.forwardComponent).toEqual({
            color: "blue",
            geometry: [0, 0, 120, 140],
        });
    });
});
