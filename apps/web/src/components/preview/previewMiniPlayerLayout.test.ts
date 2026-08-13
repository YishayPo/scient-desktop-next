import { describe, expect, it } from "vite-plus/test";

import {
  clampPreviewMiniPlayerPosition,
  clampPreviewMiniPlayerSize,
  PREVIEW_MINI_PLAYER_DEFAULT_SIZE,
  PREVIEW_MINI_PLAYER_DEFAULT_TOP,
  PREVIEW_MINI_PLAYER_EDGE_GAP,
  resolvePreviewMiniPlayerDefaultPosition,
  resizePreviewMiniPlayerRect,
} from "./previewMiniPlayerLayout";

describe("clampPreviewMiniPlayerPosition", () => {
  it("opens at a useful default size", () => {
    expect(PREVIEW_MINI_PLAYER_DEFAULT_SIZE).toEqual({ width: 400, height: 260 });
  });

  it("opens centered across the upper portion of the app window", () => {
    expect(
      resolvePreviewMiniPlayerDefaultPosition(
        { width: 1_200, height: 800 },
        PREVIEW_MINI_PLAYER_DEFAULT_SIZE,
      ),
    ).toEqual({ x: 400, y: PREVIEW_MINI_PLAYER_DEFAULT_TOP });
  });

  it("keeps a dragged player within the app viewport", () => {
    expect(
      clampPreviewMiniPlayerPosition(
        { x: 900, y: -40 },
        { width: 1_000, height: 700 },
        { width: 360, height: 240 },
      ),
    ).toEqual({
      x: 628,
      y: PREVIEW_MINI_PLAYER_EDGE_GAP,
    });
  });

  it("keeps an edge gap when the player is larger than its container", () => {
    expect(
      clampPreviewMiniPlayerPosition(
        { x: 20, y: 30 },
        { width: 200, height: 160 },
        { width: 360, height: 240 },
      ),
    ).toEqual({
      x: PREVIEW_MINI_PLAYER_EDGE_GAP,
      y: PREVIEW_MINI_PLAYER_EDGE_GAP,
    });
  });

  it("can keep the player above a reserved bottom inset", () => {
    expect(
      clampPreviewMiniPlayerPosition(
        { x: 500, y: 448 },
        { width: 1_000, height: 700 },
        { width: 360, height: 240 },
        160,
      ),
    ).toEqual({
      x: 500,
      y: 288,
    });
  });
});

describe("clampPreviewMiniPlayerSize", () => {
  it("allows resizing within the available viewport", () => {
    expect(
      clampPreviewMiniPlayerSize({ width: 520, height: 360 }, { width: 1_000, height: 700 }, 120),
    ).toEqual({ width: 520, height: 360 });
  });

  it("bounds oversized players above a reserved bottom inset", () => {
    expect(
      clampPreviewMiniPlayerSize(
        { width: 2_000, height: 2_000 },
        { width: 1_000, height: 700 },
        120,
      ),
    ).toEqual({ width: 976, height: 556 });
  });

  it("lets a tiny container win over the preferred minimum", () => {
    expect(
      clampPreviewMiniPlayerSize({ width: 360, height: 239 }, { width: 250, height: 180 }, 20),
    ).toEqual({ width: 226, height: 136 });
  });
});

describe("resizePreviewMiniPlayerRect", () => {
  const rect = { position: { x: 200, y: 160 }, size: { width: 320, height: 200 } };
  const container = { width: 1_000, height: 700 };

  it("anchors the opposite corner when resizing from the top left", () => {
    expect(
      resizePreviewMiniPlayerRect({
        rect,
        direction: "nw",
        delta: { x: -80, y: -40 },
        container,
      }),
    ).toEqual({ position: { x: 120, y: 120 }, size: { width: 400, height: 240 } });
  });

  it("resizes from individual edges without moving unrelated edges", () => {
    expect(
      resizePreviewMiniPlayerRect({
        rect,
        direction: "w",
        delta: { x: 40, y: 90 },
        container,
      }),
    ).toEqual({ position: { x: 240, y: 160 }, size: { width: 280, height: 200 } });
    expect(
      resizePreviewMiniPlayerRect({
        rect,
        direction: "s",
        delta: { x: 90, y: 60 },
        container,
      }),
    ).toEqual({ position: { x: 200, y: 160 }, size: { width: 320, height: 260 } });
  });

  it("honors viewport, reserved-inset, and minimum-size bounds from every direction", () => {
    expect(
      resizePreviewMiniPlayerRect({
        rect,
        direction: "nw",
        delta: { x: -1_000, y: -1_000 },
        container,
      }),
    ).toEqual({ position: { x: 12, y: 12 }, size: { width: 508, height: 348 } });
    expect(
      resizePreviewMiniPlayerRect({
        rect,
        direction: "se",
        delta: { x: 1_000, y: 1_000 },
        container,
        bottomInset: 120,
      }),
    ).toEqual({ position: { x: 200, y: 160 }, size: { width: 788, height: 408 } });
    expect(
      resizePreviewMiniPlayerRect({
        rect,
        direction: "nw",
        delta: { x: 1_000, y: 1_000 },
        container,
      }),
    ).toEqual({ position: { x: 280, y: 210 }, size: { width: 240, height: 150 } });
  });
});
