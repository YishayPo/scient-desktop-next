import { describe, expect, it } from "vite-plus/test";

import { nextPreviewImageZoom } from "./previewImageZoom";

describe("nextPreviewImageZoom", () => {
  it("maps smooth trackpad deltas into a bounded image scale", () => {
    expect(nextPreviewImageZoom(1, -20)).toBeGreaterThan(1);
    expect(nextPreviewImageZoom(1, 20)).toBe(1);
    expect(nextPreviewImageZoom(4.9, -1_000)).toBe(5);
    expect(nextPreviewImageZoom(Number.NaN, 0)).toBe(1);
  });
});
