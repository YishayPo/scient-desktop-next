export const PREVIEW_IMAGE_MIN_ZOOM = 1;
export const PREVIEW_IMAGE_MAX_ZOOM = 5;

export function nextPreviewImageZoom(current: number, wheelDeltaY: number): number {
  const normalizedCurrent = Number.isFinite(current) ? current : PREVIEW_IMAGE_MIN_ZOOM;
  return Math.min(
    PREVIEW_IMAGE_MAX_ZOOM,
    Math.max(PREVIEW_IMAGE_MIN_ZOOM, normalizedCurrent * Math.exp(-wheelDeltaY * 0.01)),
  );
}
