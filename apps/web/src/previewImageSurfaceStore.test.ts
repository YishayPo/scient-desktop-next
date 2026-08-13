import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { type EnvironmentId, ThreadId } from "@t3tools/contracts";
import { beforeEach, describe, expect, it } from "vite-plus/test";

import { selectPreviewImageSource, usePreviewImageSurfaceStore } from "./previewImageSurfaceStore";

const refA = scopeThreadRef("env-1" as EnvironmentId, ThreadId.make("thread-A"));
const refB = scopeThreadRef("env-1" as EnvironmentId, ThreadId.make("thread-B"));

beforeEach(() => {
  usePreviewImageSurfaceStore.setState({ byThreadKey: {} });
});

describe("previewImageSurfaceStore", () => {
  it("keeps image metadata scoped by thread and preview tab", () => {
    usePreviewImageSurfaceStore
      .getState()
      .register(refA, "tab-a", { url: "https://example.test/a.png", alt: "A" });
    usePreviewImageSurfaceStore
      .getState()
      .register(refB, "tab-a", { url: "https://example.test/b.png", alt: "B" });

    expect(
      selectPreviewImageSource(usePreviewImageSurfaceStore.getState().byThreadKey, refA, "tab-a"),
    ).toEqual({ url: "https://example.test/a.png", alt: "A" });
    expect(
      selectPreviewImageSource(usePreviewImageSurfaceStore.getState().byThreadKey, refB, "tab-a"),
    ).toEqual({ url: "https://example.test/b.png", alt: "B" });
  });

  it("updates a followed image without affecting peer tabs", () => {
    const store = usePreviewImageSurfaceStore.getState();
    store.register(refA, "tab-a", { url: "https://example.test/old.png", alt: "Old" });
    store.register(refA, "tab-b", { url: "https://example.test/peer.png", alt: "Peer" });
    store.register(refA, "tab-a", { url: "https://example.test/new.png", alt: "New" });

    expect(
      selectPreviewImageSource(usePreviewImageSurfaceStore.getState().byThreadKey, refA, "tab-a"),
    ).toEqual({ url: "https://example.test/new.png", alt: "New" });
    expect(
      selectPreviewImageSource(usePreviewImageSurfaceStore.getState().byThreadKey, refA, "tab-b"),
    ).toEqual({ url: "https://example.test/peer.png", alt: "Peer" });
  });

  it("removes individual tabs and whole thread scopes", () => {
    const store = usePreviewImageSurfaceStore.getState();
    store.register(refA, "tab-a", { url: "https://example.test/a.png", alt: "A" });
    store.register(refA, "tab-b", { url: "https://example.test/b.png", alt: "B" });
    store.remove(refA, "tab-a");

    expect(
      selectPreviewImageSource(usePreviewImageSurfaceStore.getState().byThreadKey, refA, "tab-a"),
    ).toBeNull();
    expect(
      selectPreviewImageSource(usePreviewImageSurfaceStore.getState().byThreadKey, refA, "tab-b"),
    ).not.toBeNull();

    usePreviewImageSurfaceStore.getState().removeThread(refA);
    expect(
      selectPreviewImageSource(usePreviewImageSurfaceStore.getState().byThreadKey, refA, "tab-b"),
    ).toBeNull();
  });
});
