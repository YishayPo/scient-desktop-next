import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { type EnvironmentId, ThreadId } from "@t3tools/contracts";
import { beforeEach, describe, expect, it } from "vite-plus/test";

import { selectThreadPreviewMiniPlayer, usePreviewMiniPlayerStore } from "./previewMiniPlayerStore";

const refA = scopeThreadRef("env-1" as EnvironmentId, ThreadId.make("thread-A"));
const refB = scopeThreadRef("env-1" as EnvironmentId, ThreadId.make("thread-B"));

beforeEach(() => {
  usePreviewMiniPlayerStore.setState({ byThreadKey: {} });
});

describe("previewMiniPlayerStore", () => {
  it("keeps floating previews scoped to their thread", () => {
    usePreviewMiniPlayerStore.getState().open(refA, "tab-a");
    usePreviewMiniPlayerStore.getState().open(refB, "tab-b");

    expect(
      selectThreadPreviewMiniPlayer(usePreviewMiniPlayerStore.getState().byThreadKey, refA),
    ).toMatchObject({ tabId: "tab-a" });
    expect(
      selectThreadPreviewMiniPlayer(usePreviewMiniPlayerStore.getState().byThreadKey, refB),
    ).toMatchObject({ tabId: "tab-b" });
  });

  it("preserves position when switching the floating tab within one thread", () => {
    usePreviewMiniPlayerStore.getState().open(refA, "tab-a");
    usePreviewMiniPlayerStore.getState().move(refA, "tab-a", { x: 24, y: 48 });
    usePreviewMiniPlayerStore.getState().open(refA, "tab-b");

    expect(
      selectThreadPreviewMiniPlayer(usePreviewMiniPlayerStore.getState().byThreadKey, refA),
    ).toEqual({
      tabId: "tab-b",
      position: { x: 24, y: 48 },
      size: null,
      imageSource: null,
    });
  });

  it("ignores stale drag updates after the floating tab changes", () => {
    usePreviewMiniPlayerStore.getState().open(refA, "tab-a");
    usePreviewMiniPlayerStore.getState().open(refA, "tab-b");
    usePreviewMiniPlayerStore.getState().move(refA, "tab-a", { x: 100, y: 100 });

    expect(
      selectThreadPreviewMiniPlayer(usePreviewMiniPlayerStore.getState().byThreadKey, refA),
    ).toEqual({
      tabId: "tab-b",
      position: null,
      size: null,
      imageSource: null,
    });
  });

  it("preserves a thread-bound size while switching tabs", () => {
    usePreviewMiniPlayerStore.getState().open(refA, "tab-a");
    usePreviewMiniPlayerStore.getState().resize(refA, "tab-a", { width: 480, height: 320 });
    usePreviewMiniPlayerStore.getState().open(refA, "tab-b");

    expect(
      selectThreadPreviewMiniPlayer(usePreviewMiniPlayerStore.getState().byThreadKey, refA),
    ).toMatchObject({ tabId: "tab-b", size: { width: 480, height: 320 } });
  });

  it("opens a floating preview at an explicit drag-drop position", () => {
    usePreviewMiniPlayerStore.getState().open(refA, "tab-a", { x: 180, y: 96 });

    expect(
      selectThreadPreviewMiniPlayer(usePreviewMiniPlayerStore.getState().byThreadKey, refA),
    ).toEqual({
      tabId: "tab-a",
      position: { x: 180, y: 96 },
      size: null,
      imageSource: null,
    });
  });

  it("updates resize position and size atomically", () => {
    usePreviewMiniPlayerStore.getState().open(refA, "tab-a");
    let updateCount = 0;
    const unsubscribe = usePreviewMiniPlayerStore.subscribe(() => {
      updateCount += 1;
    });
    usePreviewMiniPlayerStore.getState().setRect(refA, "tab-a", {
      position: { x: 80, y: 64 },
      size: { width: 520, height: 360 },
    });
    unsubscribe();

    expect(updateCount).toBe(1);
    expect(
      selectThreadPreviewMiniPlayer(usePreviewMiniPlayerStore.getState().byThreadKey, refA),
    ).toEqual({
      tabId: "tab-a",
      position: { x: 80, y: 64 },
      size: { width: 520, height: 360 },
      imageSource: null,
    });
  });

  it("updates a generic image source without losing the card layout", () => {
    usePreviewMiniPlayerStore
      .getState()
      .open(
        refA,
        "tab-a",
        { x: 90, y: 70 },
        { url: "https://example.test/first.png", alt: "First image" },
      );
    usePreviewMiniPlayerStore.getState().resize(refA, "tab-a", { width: 500, height: 340 });
    usePreviewMiniPlayerStore.getState().open(refA, "tab-a", undefined, {
      url: "https://example.test/second.png",
      alt: "Updated image",
    });

    expect(
      selectThreadPreviewMiniPlayer(usePreviewMiniPlayerStore.getState().byThreadKey, refA),
    ).toEqual({
      tabId: "tab-a",
      position: { x: 90, y: 70 },
      size: { width: 500, height: 340 },
      imageSource: { url: "https://example.test/second.png", alt: "Updated image" },
    });
  });
});
