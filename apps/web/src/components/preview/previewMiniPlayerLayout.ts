import type { PreviewMiniPlayerPosition, PreviewMiniPlayerSize } from "~/previewMiniPlayerStore";

export const PREVIEW_MINI_PLAYER_EDGE_GAP = 12;
export const PREVIEW_MINI_PLAYER_DEFAULT_SIZE = { width: 320, height: 200 } as const;
export const PREVIEW_MINI_PLAYER_MIN_SIZE = { width: 240, height: 150 } as const;

export type PreviewMiniPlayerResizeDirection = "n" | "ne" | "e" | "se" | "s" | "sw" | "w" | "nw";

export interface PreviewMiniPlayerRect {
  readonly position: PreviewMiniPlayerPosition;
  readonly size: PreviewMiniPlayerSize;
}

export function clampPreviewMiniPlayerSize(
  size: PreviewMiniPlayerSize,
  container: PreviewMiniPlayerSize,
  bottomInset = 0,
): PreviewMiniPlayerSize {
  const availableWidth = Math.max(1, container.width - PREVIEW_MINI_PLAYER_EDGE_GAP * 2);
  const availableHeight = Math.max(
    1,
    container.height - Math.max(0, bottomInset) - PREVIEW_MINI_PLAYER_EDGE_GAP * 2,
  );
  return {
    width: Math.round(
      Math.min(Math.max(PREVIEW_MINI_PLAYER_MIN_SIZE.width, size.width), availableWidth),
    ),
    height: Math.round(
      Math.min(Math.max(PREVIEW_MINI_PLAYER_MIN_SIZE.height, size.height), availableHeight),
    ),
  };
}

export function clampPreviewMiniPlayerPosition(
  position: PreviewMiniPlayerPosition,
  container: PreviewMiniPlayerSize,
  player: PreviewMiniPlayerSize,
  bottomInset = 0,
): PreviewMiniPlayerPosition {
  const reservedBottomSpace = Math.max(0, bottomInset);
  const maxX = Math.max(
    PREVIEW_MINI_PLAYER_EDGE_GAP,
    container.width - player.width - PREVIEW_MINI_PLAYER_EDGE_GAP,
  );
  const maxY = Math.max(
    PREVIEW_MINI_PLAYER_EDGE_GAP,
    container.height - reservedBottomSpace - player.height - PREVIEW_MINI_PLAYER_EDGE_GAP,
  );
  return {
    x: Math.min(Math.max(position.x, PREVIEW_MINI_PLAYER_EDGE_GAP), maxX),
    y: Math.min(Math.max(position.y, PREVIEW_MINI_PLAYER_EDGE_GAP), maxY),
  };
}

/** Resize one edge or corner while keeping the opposite edges anchored. */
export function resizePreviewMiniPlayerRect(input: {
  readonly rect: PreviewMiniPlayerRect;
  readonly direction: PreviewMiniPlayerResizeDirection;
  readonly delta: PreviewMiniPlayerPosition;
  readonly container: PreviewMiniPlayerSize;
  readonly bottomInset?: number;
}): PreviewMiniPlayerRect {
  const leftBound = PREVIEW_MINI_PLAYER_EDGE_GAP;
  const topBound = PREVIEW_MINI_PLAYER_EDGE_GAP;
  const rightBound = Math.max(leftBound + 1, input.container.width - PREVIEW_MINI_PLAYER_EDGE_GAP);
  const bottomBound = Math.max(
    topBound + 1,
    input.container.height - Math.max(0, input.bottomInset ?? 0) - PREVIEW_MINI_PLAYER_EDGE_GAP,
  );
  const availableWidth = rightBound - leftBound;
  const availableHeight = bottomBound - topBound;
  const minWidth = Math.min(PREVIEW_MINI_PLAYER_MIN_SIZE.width, availableWidth);
  const minHeight = Math.min(PREVIEW_MINI_PLAYER_MIN_SIZE.height, availableHeight);
  const startLeft = input.rect.position.x;
  const startTop = input.rect.position.y;
  const startRight = startLeft + input.rect.size.width;
  const startBottom = startTop + input.rect.size.height;
  let left = startLeft;
  let right = startRight;
  let top = startTop;
  let bottom = startBottom;

  if (input.direction.includes("w")) {
    left = Math.min(Math.max(startLeft + input.delta.x, leftBound), startRight - minWidth);
  } else if (input.direction.includes("e")) {
    right = Math.max(Math.min(startRight + input.delta.x, rightBound), startLeft + minWidth);
  }

  if (input.direction.includes("n")) {
    top = Math.min(Math.max(startTop + input.delta.y, topBound), startBottom - minHeight);
  } else if (input.direction.includes("s")) {
    bottom = Math.max(Math.min(startBottom + input.delta.y, bottomBound), startTop + minHeight);
  }

  return {
    position: { x: Math.round(left), y: Math.round(top) },
    size: { width: Math.round(right - left), height: Math.round(bottom - top) },
  };
}
