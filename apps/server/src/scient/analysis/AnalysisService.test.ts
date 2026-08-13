import type { ExecutionOutputChunk } from "@scientfactory/execution";
import { describe, expect, it } from "@effect/vitest";

import { recoveredOutputContentHash } from "./AnalysisService.ts";

const output: ReadonlyArray<ExecutionOutputChunk> = [
  {
    sequence: 0,
    stream: "stdout",
    text: "partial output\n",
    observedAt: "2026-08-13T00:00:00.000Z",
  },
];

describe("analysis restart recovery", () => {
  it("preserves a fidelity hash only when recovered output is known complete", () => {
    expect(recoveredOutputContentHash(output, false)).toMatch(/^sha256:[0-9a-f]{64}$/u);
    expect(recoveredOutputContentHash(output, true)).toBeNull();
  });
});
