"use client";

import type { ScopedThreadRef } from "@t3tools/contracts";
import { PanelRightIcon, PictureInPicture2, XIcon } from "lucide-react";
import { type PointerEvent as ReactPointerEvent, useLayoutEffect, useRef } from "react";

import { BrowserSurfaceSlot } from "~/browser/BrowserSurfaceSlot";
import { previewRuntimeTabId } from "~/browser/previewRuntimeTabId";
import { Button } from "~/components/ui/button";
import { toastManager } from "~/components/ui/toast";
import { useThreadPreviewState } from "~/previewStateStore";
import { selectThreadPreviewMiniPlayer, usePreviewMiniPlayerStore } from "~/previewMiniPlayerStore";
import { useRightPanelStore } from "~/rightPanelStore";

import { previewBridge } from "./previewBridge";
import {
  clampPreviewMiniPlayerPosition,
  clampPreviewMiniPlayerSize,
  PREVIEW_MINI_PLAYER_DEFAULT_SIZE,
  type PreviewMiniPlayerResizeDirection,
  resizePreviewMiniPlayerRect,
} from "./previewMiniPlayerLayout";

interface DragState {
  readonly pointerId: number;
  readonly pointerX: number;
  readonly pointerY: number;
  readonly playerX: number;
  readonly playerY: number;
}

interface ResizeState {
  readonly pointerId: number;
  readonly pointerX: number;
  readonly pointerY: number;
  readonly direction: PreviewMiniPlayerResizeDirection;
  readonly playerX: number;
  readonly playerY: number;
  readonly width: number;
  readonly height: number;
}

const RESIZE_HANDLES: ReadonlyArray<{
  readonly direction: PreviewMiniPlayerResizeDirection;
  readonly label: string;
  readonly className: string;
}> = [
  {
    direction: "nw",
    label: "top left",
    className: "left-0 top-0 size-4 cursor-nwse-resize",
  },
  {
    direction: "n",
    label: "top",
    className: "left-4 right-4 top-0 h-2 cursor-ns-resize",
  },
  {
    direction: "ne",
    label: "top right",
    className: "right-0 top-0 size-4 cursor-nesw-resize",
  },
  {
    direction: "e",
    label: "right",
    className: "bottom-4 right-0 top-4 w-2 cursor-ew-resize",
  },
  {
    direction: "se",
    label: "bottom right",
    className: "bottom-0 right-0 size-4 cursor-nwse-resize",
  },
  {
    direction: "s",
    label: "bottom",
    className: "bottom-0 left-4 right-4 h-2 cursor-ns-resize",
  },
  {
    direction: "sw",
    label: "bottom left",
    className: "bottom-0 left-0 size-4 cursor-nesw-resize",
  },
  {
    direction: "w",
    label: "left",
    className: "bottom-4 left-0 top-4 w-2 cursor-ew-resize",
  },
];

interface Props {
  readonly threadRef: ScopedThreadRef;
  readonly tabId: string;
  readonly bottomInset: number;
}

export function ThreadPreviewMiniPlayer({ threadRef, tabId, bottomInset }: Props) {
  const rootRef = useRef<HTMLElement | null>(null);
  const dragRef = useRef<DragState | null>(null);
  const resizeRef = useRef<ResizeState | null>(null);
  const miniPlayer = usePreviewMiniPlayerStore((state) =>
    selectThreadPreviewMiniPlayer(state.byThreadKey, threadRef),
  );
  const previewState = useThreadPreviewState(threadRef);
  const snapshot = previewState.sessions[tabId] ?? null;
  const runtimeTabId = previewRuntimeTabId(threadRef, previewState.serverEpoch, tabId);
  const desktopOverlay = previewState.desktopByTabId[tabId] ?? null;
  const position = miniPlayer?.tabId === tabId ? miniPlayer.position : null;
  const size =
    miniPlayer?.tabId === tabId && miniPlayer.size
      ? miniPlayer.size
      : PREVIEW_MINI_PLAYER_DEFAULT_SIZE;
  const close = () => {
    usePreviewMiniPlayerStore.getState().close(threadRef);
  };

  const openInPanel = () => {
    usePreviewMiniPlayerStore.getState().close(threadRef);
    useRightPanelStore.getState().openBrowser(threadRef, tabId);
  };

  const toggleNativePictureInPicture = () => {
    if (!previewBridge) return;
    const operation = desktopOverlay?.pictureInPicture
      ? previewBridge.pictureInPicture.close
      : previewBridge.pictureInPicture.open;
    void operation(runtimeTabId).catch((error) => {
      toastManager.add({
        type: "error",
        title: "Unable to update popped-out preview",
        description: error instanceof Error ? error.message : "An error occurred.",
      });
    });
  };

  useLayoutEffect(() => {
    const clampAndMove = () => {
      const root = rootRef.current;
      const parent = root?.offsetParent;
      if (!root || !(parent instanceof HTMLElement)) return;
      const nextSize = clampPreviewMiniPlayerSize(
        { width: root.offsetWidth, height: root.offsetHeight },
        { width: parent.clientWidth, height: parent.clientHeight },
        bottomInset,
      );
      const store = usePreviewMiniPlayerStore.getState();
      const current = selectThreadPreviewMiniPlayer(store.byThreadKey, threadRef);
      const next = clampPreviewMiniPlayerPosition(
        current?.tabId === tabId && current.position
          ? current.position
          : { x: root.offsetLeft, y: root.offsetTop },
        { width: parent.clientWidth, height: parent.clientHeight },
        nextSize,
        bottomInset,
      );
      store.setRect(threadRef, tabId, { position: next, size: nextSize });
    };
    clampAndMove();
    const root = rootRef.current;
    const parent = root?.offsetParent;
    if (!root || !(parent instanceof HTMLElement) || typeof ResizeObserver === "undefined") {
      return;
    }
    const observer = new ResizeObserver(clampAndMove);
    observer.observe(root);
    observer.observe(parent);
    return () => observer.disconnect();
  }, [bottomInset, tabId, threadRef]);

  const handlePointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return;
    const root = rootRef.current;
    const parent = root?.offsetParent;
    if (!root || !(parent instanceof HTMLElement)) return;
    const rootRect = root.getBoundingClientRect();
    const parentRect = parent.getBoundingClientRect();
    dragRef.current = {
      pointerId: event.pointerId,
      pointerX: event.clientX,
      pointerY: event.clientY,
      playerX: rootRect.left - parentRect.left,
      playerY: rootRect.top - parentRect.top,
    };
    event.currentTarget.setPointerCapture(event.pointerId);
    event.preventDefault();
  };

  const handlePointerMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    const drag = dragRef.current;
    const root = rootRef.current;
    const parent = root?.offsetParent;
    if (!drag || drag.pointerId !== event.pointerId || !root || !(parent instanceof HTMLElement)) {
      return;
    }
    const next = clampPreviewMiniPlayerPosition(
      {
        x: drag.playerX + event.clientX - drag.pointerX,
        y: drag.playerY + event.clientY - drag.pointerY,
      },
      { width: parent.clientWidth, height: parent.clientHeight },
      { width: root.offsetWidth, height: root.offsetHeight },
      bottomInset,
    );
    usePreviewMiniPlayerStore.getState().move(threadRef, tabId, next);
  };

  const endDrag = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (dragRef.current?.pointerId !== event.pointerId) return;
    dragRef.current = null;
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
  };

  const handleResizePointerDown = (
    event: ReactPointerEvent<HTMLButtonElement>,
    direction: PreviewMiniPlayerResizeDirection,
  ) => {
    if (event.button !== 0) return;
    const root = rootRef.current;
    const parent = root?.offsetParent;
    if (!root || !(parent instanceof HTMLElement)) return;
    const rootRect = root.getBoundingClientRect();
    const parentRect = parent.getBoundingClientRect();
    resizeRef.current = {
      pointerId: event.pointerId,
      pointerX: event.clientX,
      pointerY: event.clientY,
      direction,
      playerX: rootRect.left - parentRect.left,
      playerY: rootRect.top - parentRect.top,
      width: root.offsetWidth,
      height: root.offsetHeight,
    };
    event.currentTarget.setPointerCapture(event.pointerId);
    event.preventDefault();
    event.stopPropagation();
  };

  const handleResizePointerMove = (event: ReactPointerEvent<HTMLButtonElement>) => {
    const resize = resizeRef.current;
    const root = rootRef.current;
    const parent = root?.offsetParent;
    if (
      !resize ||
      resize.pointerId !== event.pointerId ||
      !root ||
      !(parent instanceof HTMLElement)
    ) {
      return;
    }
    const next = resizePreviewMiniPlayerRect({
      rect: {
        position: { x: resize.playerX, y: resize.playerY },
        size: { width: resize.width, height: resize.height },
      },
      direction: resize.direction,
      delta: {
        x: event.clientX - resize.pointerX,
        y: event.clientY - resize.pointerY,
      },
      container: { width: parent.clientWidth, height: parent.clientHeight },
      bottomInset,
    });
    usePreviewMiniPlayerStore.getState().setRect(threadRef, tabId, next);
  };

  const endResize = (event: ReactPointerEvent<HTMLButtonElement>) => {
    if (resizeRef.current?.pointerId !== event.pointerId) return;
    resizeRef.current = null;
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
  };

  if (!snapshot || miniPlayer?.tabId !== tabId) return null;

  return (
    <section
      ref={rootRef}
      aria-label="Floating browser preview"
      data-preview-mini-player={tabId}
      className="pointer-events-none absolute select-none"
      style={
        position
          ? { left: position.x, top: position.y, width: size.width, height: size.height }
          : {
              right: 16,
              top: 16,
              width: size.width,
              height: size.height,
            }
      }
    >
      <div className="group pointer-events-auto absolute right-2 top-2 z-[34] size-3">
        <div
          aria-hidden="true"
          className="absolute right-0 top-0 size-2 rounded-full bg-foreground/25 shadow-sm ring-1 ring-background/70 transition-opacity group-hover:opacity-0 group-focus-within:opacity-0"
        />
        <div
          className="pointer-events-none absolute right-0 top-0 flex h-8 cursor-grab items-center gap-0.5 rounded-lg border border-border/80 bg-popover/92 p-0.5 opacity-0 shadow-lg/20 backdrop-blur-xl transition-opacity group-hover:pointer-events-auto group-hover:opacity-100 group-focus-within:pointer-events-auto group-focus-within:opacity-100 active:cursor-grabbing"
          onPointerDown={handlePointerDown}
          onPointerMove={handlePointerMove}
          onPointerUp={endDrag}
          onPointerCancel={endDrag}
        >
          <Button
            variant="ghost"
            size="icon-xs"
            aria-label="Open preview in right panel"
            title="Open in right panel"
            onPointerDown={(event) => event.stopPropagation()}
            onClick={openInPanel}
          >
            <PanelRightIcon />
          </Button>
          <Button
            variant={desktopOverlay?.pictureInPicture ? "secondary" : "ghost"}
            size="icon-xs"
            aria-label={
              desktopOverlay?.pictureInPicture
                ? "Close popped-out preview"
                : "Pop preview into separate window"
            }
            title={
              desktopOverlay?.pictureInPicture
                ? "Close separate window"
                : "Pop into separate window"
            }
            disabled={!desktopOverlay?.hasWebContents}
            onPointerDown={(event) => event.stopPropagation()}
            onClick={toggleNativePictureInPicture}
          >
            <PictureInPicture2 />
          </Button>
          <Button
            variant="ghost"
            size="icon-xs"
            aria-label="Close floating preview"
            title="Close floating preview"
            onPointerDown={(event) => event.stopPropagation()}
            onClick={close}
          >
            <XIcon />
          </Button>
        </div>
      </div>

      <div className="relative h-full min-h-0">
        <div className="absolute inset-0 z-[29] rounded-xl bg-muted shadow-2xl/35" />
        <BrowserSurfaceSlot
          tabId={runtimeTabId}
          visible={Boolean(desktopOverlay?.hasWebContents)}
          cornerRadius={12}
          fitSourceContent
          layoutVersion={position ? `${position.x}:${position.y}` : `initial:${bottomInset}`}
          className="absolute inset-0"
        />
        <div className="pointer-events-none absolute inset-0 z-[31] rounded-xl ring-1 ring-inset ring-border/80" />
        {!desktopOverlay?.hasWebContents ? (
          <div className="pointer-events-none absolute inset-0 z-[32] flex items-center justify-center rounded-xl bg-muted text-xs text-muted-foreground">
            Reconnecting preview…
          </div>
        ) : null}
        {RESIZE_HANDLES.map((handle) => (
          <button
            key={handle.direction}
            type="button"
            aria-label={`Resize floating preview from ${handle.label}`}
            title={`Resize from ${handle.label}`}
            className={`pointer-events-auto absolute z-[33] touch-none ${handle.className}`}
            onPointerDown={(event) => handleResizePointerDown(event, handle.direction)}
            onPointerMove={handleResizePointerMove}
            onPointerUp={endResize}
            onPointerCancel={endResize}
          />
        ))}
      </div>
    </section>
  );
}
