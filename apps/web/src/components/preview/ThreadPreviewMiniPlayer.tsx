"use client";

import type { ScopedThreadRef } from "@t3tools/contracts";
import { PanelRightIcon, PictureInPicture2, XIcon } from "lucide-react";
import { type PointerEvent as ReactPointerEvent, useEffect, useLayoutEffect, useRef } from "react";
import { createPortal } from "react-dom";

import { BrowserSurfaceSlot } from "~/browser/BrowserSurfaceSlot";
import { previewRuntimeTabId } from "~/browser/previewRuntimeTabId";
import { Button } from "~/components/ui/button";
import { toastManager } from "~/components/ui/toast";
import { useThreadPreviewState } from "~/previewStateStore";
import { selectThreadPreviewMiniPlayer, usePreviewMiniPlayerStore } from "~/previewMiniPlayerStore";
import { useRightPanelStore } from "~/rightPanelStore";

import { previewBridge } from "./previewBridge";
import { PreviewMiniPlayerImageSurface } from "./PreviewMiniPlayerImageSurface";
import {
  clampPreviewMiniPlayerPosition,
  clampPreviewMiniPlayerSize,
  PREVIEW_MINI_PLAYER_DEFAULT_SIZE,
  type PreviewMiniPlayerResizeDirection,
  resizePreviewMiniPlayerRect,
  resolvePreviewMiniPlayerDefaultPosition,
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
}

export function ThreadPreviewMiniPlayer({ threadRef, tabId }: Props) {
  const rootRef = useRef<HTMLElement | null>(null);
  const dragRef = useRef<DragState | null>(null);
  const resizeRef = useRef<ResizeState | null>(null);
  const dragFrameRef = useRef<number | null>(null);
  const pendingDragPositionRef = useRef<{ readonly x: number; readonly y: number } | null>(null);
  const resizeFrameRef = useRef<number | null>(null);
  const pendingResizeRectRef = useRef<{
    readonly position: { readonly x: number; readonly y: number };
    readonly size: { readonly width: number; readonly height: number };
  } | null>(null);
  const previousBodyCursorRef = useRef<string | null>(null);
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
  const imageSource = miniPlayer?.tabId === tabId ? miniPlayer.imageSource : null;
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

  const restoreBodyCursor = () => {
    if (previousBodyCursorRef.current === null) return;
    document.body.style.cursor = previousBodyCursorRef.current;
    previousBodyCursorRef.current = null;
  };

  useEffect(
    () => () => {
      if (dragFrameRef.current !== null) window.cancelAnimationFrame(dragFrameRef.current);
      if (resizeFrameRef.current !== null) window.cancelAnimationFrame(resizeFrameRef.current);
      if (previousBodyCursorRef.current === null) return;
      document.body.style.cursor = previousBodyCursorRef.current;
      previousBodyCursorRef.current = null;
    },
    [],
  );

  const flushDragPosition = () => {
    if (dragFrameRef.current !== null) {
      window.cancelAnimationFrame(dragFrameRef.current);
      dragFrameRef.current = null;
    }
    const next = pendingDragPositionRef.current;
    pendingDragPositionRef.current = null;
    if (next) usePreviewMiniPlayerStore.getState().move(threadRef, tabId, next);
  };
  const scheduleDragPosition = (next: { readonly x: number; readonly y: number }) => {
    pendingDragPositionRef.current = next;
    if (dragFrameRef.current !== null) return;
    dragFrameRef.current = window.requestAnimationFrame(() => {
      dragFrameRef.current = null;
      const pending = pendingDragPositionRef.current;
      pendingDragPositionRef.current = null;
      if (pending) usePreviewMiniPlayerStore.getState().move(threadRef, tabId, pending);
    });
  };

  const flushResizeRect = () => {
    if (resizeFrameRef.current !== null) {
      window.cancelAnimationFrame(resizeFrameRef.current);
      resizeFrameRef.current = null;
    }
    const next = pendingResizeRectRef.current;
    pendingResizeRectRef.current = null;
    if (next) usePreviewMiniPlayerStore.getState().setRect(threadRef, tabId, next);
  };

  const scheduleResizeRect = (next: {
    readonly position: { readonly x: number; readonly y: number };
    readonly size: { readonly width: number; readonly height: number };
  }) => {
    pendingResizeRectRef.current = next;
    if (resizeFrameRef.current !== null) return;
    resizeFrameRef.current = window.requestAnimationFrame(() => {
      resizeFrameRef.current = null;
      const pending = pendingResizeRectRef.current;
      pendingResizeRectRef.current = null;
      if (pending) usePreviewMiniPlayerStore.getState().setRect(threadRef, tabId, pending);
    });
  };

  useLayoutEffect(() => {
    const clampAndMove = () => {
      const root = rootRef.current;
      if (!root) return;
      const viewport = { width: window.innerWidth, height: window.innerHeight };
      const nextSize = clampPreviewMiniPlayerSize(
        { width: root.offsetWidth, height: root.offsetHeight },
        viewport,
      );
      const store = usePreviewMiniPlayerStore.getState();
      const current = selectThreadPreviewMiniPlayer(store.byThreadKey, threadRef);
      const rootRect = root.getBoundingClientRect();
      const next = clampPreviewMiniPlayerPosition(
        current?.tabId === tabId && current.position
          ? current.position
          : { x: rootRect.left, y: rootRect.top },
        viewport,
        nextSize,
      );
      store.setRect(threadRef, tabId, { position: next, size: nextSize });
    };
    clampAndMove();
    const root = rootRef.current;
    const observer =
      root && typeof ResizeObserver !== "undefined" ? new ResizeObserver(clampAndMove) : null;
    if (root) observer?.observe(root);
    window.addEventListener("resize", clampAndMove);
    return () => {
      observer?.disconnect();
      window.removeEventListener("resize", clampAndMove);
    };
  }, [tabId, threadRef]);

  const handlePointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return;
    const root = rootRef.current;
    if (!root) return;
    const rootRect = root.getBoundingClientRect();
    dragRef.current = {
      pointerId: event.pointerId,
      pointerX: event.clientX,
      pointerY: event.clientY,
      playerX: rootRect.left,
      playerY: rootRect.top,
    };
    previousBodyCursorRef.current ??= document.body.style.cursor;
    document.body.style.cursor = "grabbing";
    event.currentTarget.setPointerCapture(event.pointerId);
    event.preventDefault();
  };

  const handlePointerMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    const drag = dragRef.current;
    const root = rootRef.current;
    if (!drag || drag.pointerId !== event.pointerId || !root) {
      return;
    }
    const next = clampPreviewMiniPlayerPosition(
      {
        x: drag.playerX + event.clientX - drag.pointerX,
        y: drag.playerY + event.clientY - drag.pointerY,
      },
      { width: window.innerWidth, height: window.innerHeight },
      { width: root.offsetWidth, height: root.offsetHeight },
    );
    scheduleDragPosition(next);
  };

  const endDrag = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (dragRef.current?.pointerId !== event.pointerId) return;
    dragRef.current = null;
    flushDragPosition();
    restoreBodyCursor();
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
    if (!root) return;
    const rootRect = root.getBoundingClientRect();
    resizeRef.current = {
      pointerId: event.pointerId,
      pointerX: event.clientX,
      pointerY: event.clientY,
      direction,
      playerX: rootRect.left,
      playerY: rootRect.top,
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
    if (!resize || resize.pointerId !== event.pointerId || !root) {
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
      container: { width: window.innerWidth, height: window.innerHeight },
    });
    scheduleResizeRect(next);
  };

  const endResize = (event: ReactPointerEvent<HTMLButtonElement>) => {
    if (resizeRef.current?.pointerId !== event.pointerId) return;
    resizeRef.current = null;
    flushResizeRect();
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
  };

  if (!snapshot || miniPlayer?.tabId !== tabId || typeof document === "undefined") return null;
  const defaultPosition = resolvePreviewMiniPlayerDefaultPosition(
    { width: window.innerWidth, height: window.innerHeight },
    size,
  );

  return createPortal(
    <section
      ref={rootRef}
      aria-label="Floating preview"
      data-preview-mini-player={tabId}
      className="pointer-events-none fixed z-[29] select-none"
      style={
        position
          ? { left: position.x, top: position.y, width: size.width, height: size.height }
          : {
              left: defaultPosition.x,
              top: defaultPosition.y,
              width: size.width,
              height: size.height,
            }
      }
    >
      <div
        className="group pointer-events-auto absolute inset-x-0 top-0 z-[34] flex h-7 touch-none cursor-grab items-center rounded-t-xl border-b border-border/60 bg-popover/95 px-2 active:cursor-grabbing"
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={endDrag}
        onPointerCancel={endDrag}
        onLostPointerCapture={endDrag}
      >
        <div aria-hidden="true" className="mx-auto h-1 w-8 rounded-full bg-foreground/20" />
        <div className="absolute right-1 top-0.5 flex items-center gap-0.5">
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

      <div
        className="pointer-events-auto absolute inset-x-2 bottom-0 z-[34] h-3 touch-none cursor-grab rounded-b-xl border-t border-border/60 bg-popover/95 active:cursor-grabbing"
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={endDrag}
        onPointerCancel={endDrag}
        onLostPointerCapture={endDrag}
      />

      <div className="relative h-full min-h-0">
        <div className="absolute inset-0 z-[29] rounded-xl bg-muted shadow-2xl/35" />
        {imageSource ? (
          <PreviewMiniPlayerImageSurface source={imageSource} />
        ) : (
          <BrowserSurfaceSlot
            tabId={runtimeTabId}
            visible={Boolean(desktopOverlay?.hasWebContents)}
            cornerRadius={8}
            fitSourceContent
            layoutVersion={position ? `${position.x}:${position.y}` : "initial"}
            className="absolute inset-x-2 bottom-3 top-7"
          />
        )}
        <div className="pointer-events-none absolute inset-0 z-[31] rounded-xl ring-1 ring-inset ring-border/80" />
        {!imageSource && !desktopOverlay?.hasWebContents ? (
          <div className="pointer-events-none absolute inset-x-2 bottom-3 top-7 z-[32] flex items-center justify-center bg-muted text-xs text-muted-foreground">
            Reconnecting preview…
          </div>
        ) : null}
        {RESIZE_HANDLES.map((handle) => (
          <button
            key={handle.direction}
            type="button"
            aria-label={`Resize floating preview from ${handle.label}`}
            title={`Resize from ${handle.label}`}
            className={`pointer-events-auto absolute z-[35] touch-none ${handle.className}`}
            onPointerDown={(event) => handleResizePointerDown(event, handle.direction)}
            onPointerMove={handleResizePointerMove}
            onPointerUp={endResize}
            onPointerCancel={endResize}
          />
        ))}
      </div>
    </section>,
    document.body,
  );
}
