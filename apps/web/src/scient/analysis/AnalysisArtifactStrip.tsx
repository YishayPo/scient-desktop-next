import type {
  AnalysisArtifact,
  AnalysisRunSnapshot,
  AnalysisRunSummary,
  AssetResource,
  EnvironmentId,
  ScopedThreadRef,
} from "@t3tools/contracts";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { Download, ExternalLink, Image, LoaderCircle, Pin } from "lucide-react";
import {
  type PointerEvent as ReactPointerEvent,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";

import { resolveAssetUrl, useAssetUrls } from "~/assets/assetUrls";
import {
  applyPreviewServerSnapshot,
  isPreviewSupportedInRuntime,
  readThreadPreviewState,
  rememberPreviewUrl,
} from "~/previewStateStore";
import { selectThreadPreviewMiniPlayer, usePreviewMiniPlayerStore } from "~/previewMiniPlayerStore";
import { useRightPanelStore } from "~/rightPanelStore";
import { Button } from "~/components/ui/button";
import { Menu, MenuItem, MenuPopup } from "~/components/ui/menu";
import { PREVIEW_MINI_PLAYER_DEFAULT_SIZE } from "~/components/preview/previewMiniPlayerLayout";
import { stackedThreadToast, toastManager } from "~/components/ui/toast";
import { assetEnvironment } from "~/state/assets";
import { previewEnvironment } from "~/state/preview";
import { useEnvironmentHttpBaseUrl } from "~/state/environments";
import { useAtomCommand } from "~/state/use-atom-command";
import { useAtomQueryRunner } from "~/state/use-atom-query-runner";

import {
  analysisArtifactResource,
  canFollowArtifactInTab,
  floatingArtifactPositionForDrop,
  interactiveArtifactRepresentation,
  nativeArtifactRepresentation,
  preferredArtifactPreview,
  preferredArtifactThumbnail,
} from "./analysisArtifactPresentation";

type AnalysisRun = AnalysisRunSnapshot | AnalysisRunSummary;
type ArtifactDisplayStatus = "current" | "updating" | "stale" | "partial" | "failed-latest";
type AtomCommandFailure = Parameters<typeof squashAtomCommandFailure>[0];

interface ArtifactDragState {
  readonly pointerId: number;
  readonly originX: number;
  readonly originY: number;
  readonly artifact: AnalysisArtifact;
  readonly representation: AnalysisArtifact["representations"][number];
  readonly thumbnailUrl: string | null;
  readonly dragging: boolean;
}

interface ArtifactDragPreview {
  readonly artifactId: AnalysisArtifact["artifactId"];
  readonly label: string;
  readonly thumbnailUrl: string | null;
  readonly x: number;
  readonly y: number;
}

interface ArtifactChoiceMenu {
  readonly artifactId: AnalysisArtifact["artifactId"];
  readonly anchor: HTMLElement;
}

const ARTIFACT_DRAG_THRESHOLD = 7;

const STATUS_LABELS: Readonly<Record<ArtifactDisplayStatus, string>> = {
  current: "Current",
  updating: "Updating…",
  stale: "Source changed",
  partial: "Partial run",
  "failed-latest": "Last good · latest failed",
};

function actionFailureMessage(result: AtomCommandFailure): string {
  const error = squashAtomCommandFailure(result);
  return error instanceof Error ? error.message : "The artifact operation failed.";
}

export function AnalysisArtifactStrip(props: {
  readonly environmentId: EnvironmentId;
  readonly threadRef: ScopedThreadRef;
  readonly run: AnalysisRun;
  readonly status: ArtifactDisplayStatus;
}) {
  const httpBaseUrl = useEnvironmentHttpBaseUrl(props.environmentId);
  const createAssetUrl = useAtomQueryRunner(assetEnvironment.createUrl, { reportFailure: false });
  const openPreview = useAtomCommand(previewEnvironment.open, { reportFailure: false });
  const navigatePreview = useAtomCommand(previewEnvironment.navigate, { reportFailure: false });
  const [pendingAction, setPendingAction] = useState<string | null>(null);
  const [dragPreview, setDragPreview] = useState<ArtifactDragPreview | null>(null);
  const [choiceMenu, setChoiceMenu] = useState<ArtifactChoiceMenu | null>(null);
  const artifactDragRef = useRef<ArtifactDragState | null>(null);
  const suppressClickRef = useRef<AnalysisArtifact["artifactId"] | null>(null);
  const previousBodyCursorRef = useRef<string | null>(null);
  const followedTabsRef = useRef(
    new Map<
      string,
      {
        readonly artifactId: AnalysisArtifact["artifactId"];
        readonly representationId: AnalysisArtifact["representations"][number]["representationId"];
        readonly lastArtifactUrl: string;
      }
    >(),
  );
  const followedRunIdRef = useRef(props.run.receipt.runId);
  const cards = useMemo(() => {
    let thumbnailIndex = 0;
    return props.run.artifacts.map((artifact) => {
      const preview = preferredArtifactPreview(artifact);
      const thumbnail = preferredArtifactThumbnail(artifact);
      const interactive = interactiveArtifactRepresentation(artifact);
      const native = nativeArtifactRepresentation(artifact);
      return {
        artifact,
        preview,
        thumbnail,
        interactive,
        native,
        thumbnailIndex: thumbnail ? thumbnailIndex++ : null,
      };
    });
  }, [props.run.artifacts]);
  const thumbnailResources = useMemo(
    () =>
      cards.flatMap((card) =>
        card.thumbnail ? [analysisArtifactResource(props.run, card.artifact, card.thumbnail)] : [],
      ),
    [cards, props.run],
  );
  const thumbnailUrls = useAssetUrls(props.environmentId, thumbnailResources);
  const choiceCard = choiceMenu
    ? (cards.find((card) => card.artifact.artifactId === choiceMenu.artifactId) ?? null)
    : null;

  useEffect(
    () => () => {
      if (previousBodyCursorRef.current === null) return;
      document.body.style.cursor = previousBodyCursorRef.current;
      previousBodyCursorRef.current = null;
    },
    [],
  );

  const restoreDragCursor = () => {
    if (previousBodyCursorRef.current === null) return;
    document.body.style.cursor = previousBodyCursorRef.current;
    previousBodyCursorRef.current = null;
  };

  useEffect(() => {
    if (!httpBaseUrl || followedRunIdRef.current === props.run.receipt.runId) return;
    followedRunIdRef.current = props.run.receipt.runId;
    let disposed = false;

    void (async () => {
      for (const [tabId, followed] of followedTabsRef.current) {
        const snapshot = readThreadPreviewState(props.threadRef).sessions[tabId];
        if (!canFollowArtifactInTab(snapshot, followed.lastArtifactUrl)) {
          followedTabsRef.current.delete(tabId);
          continue;
        }
        const artifact = props.run.artifacts.find(
          (candidate) => candidate.artifactId === followed.artifactId,
        );
        const representation = artifact
          ? (artifact.representations.find(
              (candidate) => candidate.representationId === followed.representationId,
            ) ?? preferredArtifactPreview(artifact))
          : null;
        if (!artifact || !representation) {
          followedTabsRef.current.delete(tabId);
          continue;
        }
        const assetResult = await createAssetUrl({
          environmentId: props.environmentId,
          input: { resource: analysisArtifactResource(props.run, artifact, representation) },
        });
        if (disposed || assetResult._tag !== "Success") continue;
        const url = resolveAssetUrl(httpBaseUrl, assetResult.value.relativeUrl);
        if (!url) continue;
        const navigation = await navigatePreview({
          environmentId: props.environmentId,
          input: { threadId: props.threadRef.threadId, tabId, url },
        });
        if (disposed) return;
        if (navigation._tag !== "Success") {
          followedTabsRef.current.delete(tabId);
          continue;
        }
        applyPreviewServerSnapshot(props.threadRef, navigation.value);
        rememberPreviewUrl(props.threadRef, url);
        followedTabsRef.current.set(tabId, {
          artifactId: artifact.artifactId,
          representationId: representation.representationId,
          lastArtifactUrl: url,
        });
      }
    })();

    return () => {
      disposed = true;
    };
  }, [
    createAssetUrl,
    httpBaseUrl,
    navigatePreview,
    props.environmentId,
    props.run,
    props.threadRef,
  ]);

  const reportFailure = (title: string, result?: AtomCommandFailure) => {
    toastManager.add(
      stackedThreadToast({
        type: "error",
        title,
        description: result ? actionFailureMessage(result) : "The environment is unavailable.",
      }),
    );
  };

  const createUrl = async (resource: AssetResource): Promise<string | null> => {
    if (!httpBaseUrl) return null;
    const result = await createAssetUrl({
      environmentId: props.environmentId,
      input: { resource },
    });
    if (result._tag !== "Success") {
      if (!isAtomCommandInterrupted(result)) reportFailure("Unable to load figure", result);
      return null;
    }
    const url = resolveAssetUrl(httpBaseUrl, result.value.relativeUrl);
    if (url === null) reportFailure("Unable to load figure");
    return url;
  };

  const presentArtifact = async (
    artifact: AnalysisArtifact,
    representation: AnalysisArtifact["representations"][number] | null,
    mode: "open" | "pin" | "interactive",
    initialPosition?: { readonly x: number; readonly y: number },
  ) => {
    if (!representation || !isPreviewSupportedInRuntime()) {
      reportFailure("Figure preview is unavailable");
      return;
    }
    const actionKey = `${mode}:${artifact.artifactId}`;
    setPendingAction(actionKey);
    try {
      const url = await createUrl(analysisArtifactResource(props.run, artifact, representation));
      if (!url) return;
      const result = await openPreview({
        environmentId: props.environmentId,
        input: { threadId: props.threadRef.threadId, url },
      });
      if (result._tag !== "Success") {
        if (!isAtomCommandInterrupted(result)) reportFailure("Unable to open figure", result);
        return;
      }
      applyPreviewServerSnapshot(props.threadRef, result.value);
      rememberPreviewUrl(props.threadRef, url);
      followedTabsRef.current.set(result.value.tabId, {
        artifactId: artifact.artifactId,
        representationId: representation.representationId,
        lastArtifactUrl: url,
      });
      if (mode === "pin") {
        usePreviewMiniPlayerStore
          .getState()
          .open(props.threadRef, result.value.tabId, initialPosition);
      } else {
        useRightPanelStore.getState().openBrowser(props.threadRef, result.value.tabId);
      }
    } finally {
      setPendingAction(null);
    }
  };

  const beginArtifactDrag = (
    event: ReactPointerEvent<HTMLButtonElement>,
    artifact: AnalysisArtifact,
    representation: AnalysisArtifact["representations"][number] | null,
    thumbnailUrl: string | null,
  ) => {
    if (event.button !== 0 || !representation || pendingAction !== null) return;
    setChoiceMenu(null);
    artifactDragRef.current = {
      pointerId: event.pointerId,
      originX: event.clientX,
      originY: event.clientY,
      artifact,
      representation,
      thumbnailUrl,
      dragging: false,
    };
    event.currentTarget.setPointerCapture(event.pointerId);
  };

  const moveArtifactDrag = (event: ReactPointerEvent<HTMLButtonElement>) => {
    const drag = artifactDragRef.current;
    if (!drag || drag.pointerId !== event.pointerId) return;
    const distance = Math.hypot(event.clientX - drag.originX, event.clientY - drag.originY);
    if (!drag.dragging && distance < ARTIFACT_DRAG_THRESHOLD) return;
    if (!drag.dragging) {
      artifactDragRef.current = { ...drag, dragging: true };
      previousBodyCursorRef.current = document.body.style.cursor;
      document.body.style.cursor = "grabbing";
    }
    setDragPreview({
      artifactId: drag.artifact.artifactId,
      label: drag.artifact.label,
      thumbnailUrl: drag.thumbnailUrl,
      x: event.clientX,
      y: event.clientY,
    });
    event.preventDefault();
  };

  const endArtifactDrag = (event: ReactPointerEvent<HTMLButtonElement>, cancelled = false) => {
    const drag = artifactDragRef.current;
    if (!drag || drag.pointerId !== event.pointerId) return;
    artifactDragRef.current = null;
    setDragPreview(null);
    restoreDragCursor();
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    if (!drag.dragging) return;

    suppressClickRef.current = drag.artifact.artifactId;
    event.preventDefault();
    event.stopPropagation();
    if (cancelled) return;

    const hit = document.elementFromPoint(event.clientX, event.clientY);
    const chatColumn = hit?.closest<HTMLElement>("[data-chat-column-maximized-away]");
    const host = chatColumn?.parentElement;
    if (!chatColumn || !(host instanceof HTMLElement)) return;
    const hostRect = host.getBoundingClientRect();
    const current = selectThreadPreviewMiniPlayer(
      usePreviewMiniPlayerStore.getState().byThreadKey,
      props.threadRef,
    );
    const initialPosition = floatingArtifactPositionForDrop({
      clientPoint: { x: event.clientX, y: event.clientY },
      hostOrigin: { x: hostRect.left, y: hostRect.top },
      playerSize: current?.size ?? PREVIEW_MINI_PLAYER_DEFAULT_SIZE,
    });
    void presentArtifact(drag.artifact, drag.representation, "pin", initialPosition);
  };

  const downloadNative = async (artifact: AnalysisArtifact) => {
    const representation = nativeArtifactRepresentation(artifact);
    if (!representation) return;
    const actionKey = `download:${artifact.artifactId}`;
    setPendingAction(actionKey);
    try {
      const url = await createUrl(analysisArtifactResource(props.run, artifact, representation));
      if (!url) return;
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = representation.fileName;
      anchor.click();
    } finally {
      setPendingAction(null);
    }
  };

  return (
    <div className="border-t border-border bg-background" aria-label="Analysis figures">
      <div className="flex items-center justify-between gap-3 px-3 py-1.5 text-xs">
        <span className="font-medium text-foreground">
          {props.run.artifacts.length === 1 ? "1 figure" : `${props.run.artifacts.length} figures`}
        </span>
        <span className="text-muted-foreground" role="status">
          {STATUS_LABELS[props.status]}
        </span>
      </div>
      <div className="flex gap-2 overflow-x-auto border-t border-border/60 p-2">
        {cards.map((card) => {
          const thumbnailUrl =
            card.thumbnailIndex === null ? null : (thumbnailUrls[card.thumbnailIndex] ?? null);
          const openPending = pendingAction?.endsWith(`:${card.artifact.artifactId}`) ?? false;
          const downloadPending = pendingAction === `download:${card.artifact.artifactId}`;
          return (
            <article
              key={card.artifact.artifactId}
              className="w-36 shrink-0 overflow-hidden rounded-md border border-border/70 bg-muted/20"
            >
              <button
                type="button"
                className="block w-full cursor-pointer text-left outline-none hover:bg-accent/40 focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-default"
                disabled={(!card.preview && !card.interactive) || pendingAction !== null}
                onPointerDown={(event) =>
                  beginArtifactDrag(event, card.artifact, card.preview, thumbnailUrl)
                }
                onPointerMove={moveArtifactDrag}
                onPointerUp={(event) => endArtifactDrag(event)}
                onPointerCancel={(event) => endArtifactDrag(event, true)}
                onClick={(event) => {
                  if (suppressClickRef.current === card.artifact.artifactId) {
                    suppressClickRef.current = null;
                    return;
                  }
                  setChoiceMenu({
                    artifactId: card.artifact.artifactId,
                    anchor: event.currentTarget,
                  });
                }}
                aria-label={`Choose how to open ${card.artifact.label}`}
                aria-haspopup="menu"
                aria-expanded={choiceMenu?.artifactId === card.artifact.artifactId}
                title={card.preview ? "Choose view · drag into chat to float" : "Choose view"}
              >
                <span className="flex h-20 items-center justify-center overflow-hidden bg-background">
                  {openPending ? (
                    <LoaderCircle className="size-4 animate-spin text-muted-foreground" />
                  ) : thumbnailUrl ? (
                    <img
                      src={thumbnailUrl}
                      alt=""
                      loading="lazy"
                      draggable={false}
                      className="size-full object-contain"
                    />
                  ) : (
                    <Image className="size-5 text-muted-foreground" aria-hidden="true" />
                  )}
                </span>
                <span className="block truncate border-t border-border/60 px-2 py-1.5 text-xs font-medium">
                  {card.artifact.label}
                </span>
              </button>
              {card.native ? (
                <div className="flex justify-end border-t border-border/60 px-1 py-0.5">
                  <Button
                    size="icon-xs"
                    variant="ghost"
                    disabled={pendingAction !== null}
                    onClick={() => void downloadNative(card.artifact)}
                    aria-label={`Download native ${card.artifact.label}`}
                    title="Download MATLAB figure"
                  >
                    {downloadPending ? <LoaderCircle className="animate-spin" /> : <Download />}
                  </Button>
                </div>
              ) : null}
            </article>
          );
        })}
      </div>
      <Menu
        open={choiceCard !== null}
        onOpenChange={(open) => {
          if (!open) setChoiceMenu(null);
        }}
      >
        {choiceCard && choiceMenu ? (
          <MenuPopup
            anchor={choiceMenu.anchor}
            align="center"
            side="bottom"
            sideOffset={4}
            className="min-w-36"
          >
            <MenuItem
              disabled={!choiceCard.preview || pendingAction !== null}
              onClick={() => {
                setChoiceMenu(null);
                void presentArtifact(choiceCard.artifact, choiceCard.preview, "open");
              }}
            >
              <Image />
              <span>Static</span>
            </MenuItem>
            <MenuItem
              disabled={!choiceCard.interactive || pendingAction !== null}
              onClick={() => {
                setChoiceMenu(null);
                void presentArtifact(choiceCard.artifact, choiceCard.interactive, "interactive");
              }}
            >
              <ExternalLink />
              <span>Interactive</span>
              {!choiceCard.interactive ? (
                <span className="ml-auto text-[10px] text-muted-foreground">Unavailable</span>
              ) : null}
            </MenuItem>
            <MenuItem
              disabled={!choiceCard.preview || pendingAction !== null}
              onClick={() => {
                setChoiceMenu(null);
                void presentArtifact(choiceCard.artifact, choiceCard.preview, "pin");
              }}
            >
              <Pin />
              <span>Floating card</span>
            </MenuItem>
          </MenuPopup>
        ) : null}
      </Menu>
      {dragPreview
        ? createPortal(
            <div
              aria-hidden="true"
              className="pointer-events-none fixed z-[100] w-36 -translate-x-1/2 -translate-y-1/2 overflow-hidden rounded-md border border-border bg-popover/95 shadow-xl backdrop-blur-sm"
              style={{ left: dragPreview.x, top: dragPreview.y }}
            >
              <div className="flex h-20 items-center justify-center overflow-hidden bg-background">
                {dragPreview.thumbnailUrl ? (
                  <img
                    src={dragPreview.thumbnailUrl}
                    alt=""
                    draggable={false}
                    className="size-full object-contain opacity-90"
                  />
                ) : (
                  <Image className="size-5 text-muted-foreground" />
                )}
              </div>
              <div className="truncate border-t border-border px-2 py-1.5 text-xs font-medium">
                {dragPreview.label}
              </div>
            </div>,
            document.body,
          )
        : null}
    </div>
  );
}
