import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { create } from "zustand";

export interface PreviewImageSource {
  readonly url: string;
  readonly alt: string;
}

type PreviewImageSourcesByThread = Readonly<
  Record<string, Readonly<Record<string, PreviewImageSource>>>
>;

interface PreviewImageSurfaceStoreState {
  readonly byThreadKey: PreviewImageSourcesByThread;
  readonly register: (ref: ScopedThreadRef, tabId: string, source: PreviewImageSource) => void;
  readonly remove: (ref: ScopedThreadRef, tabId: string) => void;
  readonly removeThread: (ref: ScopedThreadRef) => void;
}

export const usePreviewImageSurfaceStore = create<PreviewImageSurfaceStoreState>()((set) => ({
  byThreadKey: {},
  register: (ref, tabId, source) =>
    set((state) => {
      const threadKey = scopedThreadKey(ref);
      const current = state.byThreadKey[threadKey];
      const previous = current?.[tabId];
      if (previous?.url === source.url && previous.alt === source.alt) return state;
      return {
        byThreadKey: {
          ...state.byThreadKey,
          [threadKey]: { ...current, [tabId]: source },
        },
      };
    }),
  remove: (ref, tabId) =>
    set((state) => {
      const threadKey = scopedThreadKey(ref);
      const current = state.byThreadKey[threadKey];
      if (!current || !(tabId in current)) return state;
      const { [tabId]: _removed, ...remaining } = current;
      if (Object.keys(remaining).length === 0) {
        const { [threadKey]: _thread, ...byThreadKey } = state.byThreadKey;
        return { byThreadKey };
      }
      return { byThreadKey: { ...state.byThreadKey, [threadKey]: remaining } };
    }),
  removeThread: (ref) =>
    set((state) => {
      const threadKey = scopedThreadKey(ref);
      if (!(threadKey in state.byThreadKey)) return state;
      const { [threadKey]: _removed, ...byThreadKey } = state.byThreadKey;
      return { byThreadKey };
    }),
}));

export function selectPreviewImageSource(
  byThreadKey: PreviewImageSourcesByThread,
  ref: ScopedThreadRef | null | undefined,
  tabId: string | null | undefined,
): PreviewImageSource | null {
  if (!ref || !tabId) return null;
  return byThreadKey[scopedThreadKey(ref)]?.[tabId] ?? null;
}
