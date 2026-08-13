"use client";

import { useEffect, useLayoutEffect, useRef, useState } from "react";

import type { PreviewMiniPlayerImageSource } from "~/previewMiniPlayerStore";

import { nextPreviewMiniPlayerImageZoom } from "./previewMiniPlayerLayout";

interface ImageZoomAnchor {
  readonly contentX: number;
  readonly contentY: number;
  readonly localX: number;
  readonly localY: number;
}

export function PreviewMiniPlayerImageSurface({
  source,
}: {
  readonly source: PreviewMiniPlayerImageSource;
}) {
  const viewportRef = useRef<HTMLDivElement | null>(null);
  const zoomFrameRef = useRef<number | null>(null);
  const pendingZoomDeltaRef = useRef(0);
  const pendingZoomPointRef = useRef<{ readonly x: number; readonly y: number } | null>(null);
  const pendingZoomAnchorRef = useRef<ImageZoomAnchor | null>(null);
  const zoomRef = useRef(1);
  const [zoom, setZoom] = useState(1);
  const [loadState, setLoadState] = useState<"loading" | "loaded" | "failed">("loading");

  useEffect(
    () => () => {
      if (zoomFrameRef.current !== null) window.cancelAnimationFrame(zoomFrameRef.current);
    },
    [],
  );

  useEffect(() => {
    if (zoomFrameRef.current !== null) {
      window.cancelAnimationFrame(zoomFrameRef.current);
      zoomFrameRef.current = null;
    }
    pendingZoomDeltaRef.current = 0;
    pendingZoomPointRef.current = null;
    pendingZoomAnchorRef.current = null;
    zoomRef.current = 1;
    setZoom(1);
    setLoadState("loading");
    viewportRef.current?.scrollTo({ left: 0, top: 0 });
  }, [source.url]);

  useEffect(() => {
    const viewport = viewportRef.current;
    if (!viewport) return;
    const handleWheel = (event: WheelEvent) => {
      if (!event.ctrlKey) return;
      event.preventDefault();
      event.stopPropagation();
      const rect = viewport.getBoundingClientRect();
      pendingZoomDeltaRef.current += event.deltaY;
      pendingZoomPointRef.current = {
        x: event.clientX - rect.left,
        y: event.clientY - rect.top,
      };
      if (zoomFrameRef.current !== null) return;
      zoomFrameRef.current = window.requestAnimationFrame(() => {
        zoomFrameRef.current = null;
        const point = pendingZoomPointRef.current;
        const delta = pendingZoomDeltaRef.current;
        pendingZoomPointRef.current = null;
        pendingZoomDeltaRef.current = 0;
        if (!point) return;
        const current = zoomRef.current;
        const next = nextPreviewMiniPlayerImageZoom(current, delta);
        if (Math.abs(next - current) < 0.001) return;
        pendingZoomAnchorRef.current = {
          contentX: (viewport.scrollLeft + point.x) / current,
          contentY: (viewport.scrollTop + point.y) / current,
          localX: point.x,
          localY: point.y,
        };
        zoomRef.current = next;
        setZoom(next);
      });
    };
    viewport.addEventListener("wheel", handleWheel, { passive: false });
    return () => viewport.removeEventListener("wheel", handleWheel);
  }, [source.url]);

  useLayoutEffect(() => {
    const viewport = viewportRef.current;
    const anchor = pendingZoomAnchorRef.current;
    if (!viewport || !anchor) return;
    pendingZoomAnchorRef.current = null;
    viewport.scrollLeft = anchor.contentX * zoom - anchor.localX;
    viewport.scrollTop = anchor.contentY * zoom - anchor.localY;
  }, [zoom]);

  return (
    <div
      ref={viewportRef}
      className="pointer-events-auto absolute inset-x-2 bottom-3 top-7 z-[30] overflow-auto overscroll-contain bg-background"
      title="Pinch with two fingers to zoom"
    >
      <div
        className="relative shrink-0 bg-background"
        style={{ width: `${zoom * 100}%`, height: `${zoom * 100}%` }}
      >
        <img
          key={source.url}
          src={source.url}
          alt={source.alt}
          draggable={false}
          className={`absolute inset-0 size-full select-none object-contain transition-opacity duration-100 ${loadState === "loaded" ? "opacity-100" : "opacity-0"}`}
          onLoad={() => setLoadState("loaded")}
          onError={() => setLoadState("failed")}
        />
      </div>
      {loadState !== "loaded" ? (
        <div className="pointer-events-none absolute inset-0 flex items-center justify-center bg-background text-xs text-muted-foreground">
          {loadState === "failed" ? "Unable to load figure" : "Loading figure…"}
        </div>
      ) : null}
    </div>
  );
}
