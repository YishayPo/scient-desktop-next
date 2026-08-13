import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { create } from "zustand";

import type { PreviewImageSource } from "./previewImageSurfaceStore";

export interface PreviewMiniPlayerPosition {
  readonly x: number;
  readonly y: number;
}

export interface PreviewMiniPlayerSize {
  readonly width: number;
  readonly height: number;
}

export interface PreviewMiniPlayerRect {
  readonly position: PreviewMiniPlayerPosition;
  readonly size: PreviewMiniPlayerSize;
}

export type PreviewMiniPlayerImageSource = PreviewImageSource;

export interface PreviewMiniPlayerState {
  readonly tabId: string;
  readonly position: PreviewMiniPlayerPosition | null;
  readonly size: PreviewMiniPlayerSize | null;
  readonly imageSource: PreviewMiniPlayerImageSource | null;
}

interface PreviewMiniPlayerStoreState {
  readonly byThreadKey: Record<string, PreviewMiniPlayerState>;
  readonly open: (
    ref: ScopedThreadRef,
    tabId: string,
    position?: PreviewMiniPlayerPosition,
    imageSource?: PreviewMiniPlayerImageSource,
  ) => void;
  readonly close: (ref: ScopedThreadRef) => void;
  readonly move: (ref: ScopedThreadRef, tabId: string, position: PreviewMiniPlayerPosition) => void;
  readonly resize: (ref: ScopedThreadRef, tabId: string, size: PreviewMiniPlayerSize) => void;
  readonly setRect: (ref: ScopedThreadRef, tabId: string, rect: PreviewMiniPlayerRect) => void;
  readonly removeThread: (ref: ScopedThreadRef) => void;
}

export const usePreviewMiniPlayerStore = create<PreviewMiniPlayerStoreState>()((set) => ({
  byThreadKey: {},
  open: (ref, tabId, position, imageSource) =>
    set((state) => {
      const threadKey = scopedThreadKey(ref);
      const current = state.byThreadKey[threadKey];
      const nextImageSource = imageSource ?? null;
      if (
        current?.tabId === tabId &&
        (position === undefined ||
          (current.position?.x === position.x && current.position.y === position.y)) &&
        current.imageSource?.url === nextImageSource?.url &&
        current.imageSource?.alt === nextImageSource?.alt
      ) {
        return state;
      }
      return {
        byThreadKey: {
          ...state.byThreadKey,
          [threadKey]: {
            tabId,
            position: position ?? current?.position ?? null,
            size: current?.size ?? null,
            imageSource: nextImageSource,
          },
        },
      };
    }),
  close: (ref) =>
    set((state) => {
      const threadKey = scopedThreadKey(ref);
      if (!(threadKey in state.byThreadKey)) return state;
      const { [threadKey]: _closed, ...byThreadKey } = state.byThreadKey;
      return { byThreadKey };
    }),
  move: (ref, tabId, position) =>
    set((state) => {
      const threadKey = scopedThreadKey(ref);
      const current = state.byThreadKey[threadKey];
      if (!current || current.tabId !== tabId) return state;
      if (current.position?.x === position.x && current.position.y === position.y) return state;
      return {
        byThreadKey: {
          ...state.byThreadKey,
          [threadKey]: { ...current, position },
        },
      };
    }),
  resize: (ref, tabId, size) =>
    set((state) => {
      const threadKey = scopedThreadKey(ref);
      const current = state.byThreadKey[threadKey];
      if (!current || current.tabId !== tabId) return state;
      if (current.size?.width === size.width && current.size.height === size.height) return state;
      return {
        byThreadKey: {
          ...state.byThreadKey,
          [threadKey]: { ...current, size },
        },
      };
    }),
  setRect: (ref, tabId, rect) =>
    set((state) => {
      const threadKey = scopedThreadKey(ref);
      const current = state.byThreadKey[threadKey];
      if (!current || current.tabId !== tabId) return state;
      if (
        current.position?.x === rect.position.x &&
        current.position.y === rect.position.y &&
        current.size?.width === rect.size.width &&
        current.size.height === rect.size.height
      ) {
        return state;
      }
      return {
        byThreadKey: {
          ...state.byThreadKey,
          [threadKey]: { ...current, position: rect.position, size: rect.size },
        },
      };
    }),
  removeThread: (ref) =>
    set((state) => {
      const threadKey = scopedThreadKey(ref);
      if (!(threadKey in state.byThreadKey)) return state;
      const { [threadKey]: _removed, ...byThreadKey } = state.byThreadKey;
      return { byThreadKey };
    }),
}));

export function selectThreadPreviewMiniPlayer(
  byThreadKey: Record<string, PreviewMiniPlayerState>,
  ref: ScopedThreadRef | null | undefined,
): PreviewMiniPlayerState | null {
  if (!ref) return null;
  return byThreadKey[scopedThreadKey(ref)] ?? null;
}
