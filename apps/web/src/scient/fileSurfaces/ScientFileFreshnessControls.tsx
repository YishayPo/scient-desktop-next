import { AlertTriangle, RotateCw } from "lucide-react";

import { Button } from "~/components/ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "~/components/ui/tooltip";
import { cn } from "~/lib/utils";

import type { FileReloadNotice } from "./useWorkspaceFileRefresh";

export function ScientFileReloadButton(props: {
  readonly isPending: boolean;
  readonly onReload: () => void;
}) {
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            className="shrink-0"
            onClick={props.onReload}
            aria-label="Reload file from disk"
            aria-busy={props.isPending}
            disabled={props.isPending}
            variant="ghost"
            size="icon-sm"
          >
            <RotateCw className={cn("size-3.5", props.isPending && "animate-spin")} />
          </Button>
        }
      />
      <TooltipPopup>Reload file from disk</TooltipPopup>
    </Tooltip>
  );
}

export function ScientFileFreshnessNotices(props: {
  readonly relativePath: string | null;
  readonly notice: FileReloadNotice | null;
  readonly readError: string | null;
  readonly hasFallbackData: boolean;
  readonly onCancel: () => void;
  readonly onReload: () => void;
  readonly onRequestOverwrite: () => void;
  readonly onResolve: (action: "discard" | "retry") => void;
}) {
  const visibleNotice = props.notice?.relativePath === props.relativePath ? props.notice : null;

  return (
    <>
      {visibleNotice ? (
        <div
          className="flex shrink-0 items-center gap-2 border-b border-warning/24 bg-warning-surface px-3 py-2 text-[11px] text-warning-foreground"
          role="status"
        >
          <AlertTriangle className="size-3.5 shrink-0" aria-hidden="true" />
          <div className="min-w-0 flex-1 leading-relaxed">
            {visibleNotice.kind === "manual-reload"
              ? "This file has unsaved edits. Reloading will discard your local buffer."
              : visibleNotice.kind === "confirm-overwrite"
                ? "This will replace the newer file on disk with your local buffer. A further concurrent change will still be rejected."
                : "This file changed on disk while you had unsaved edits. Your local buffer was kept and was not allowed to overwrite the newer file."}
          </div>
          {visibleNotice.kind === "external-change" ? (
            <Button size="xs" variant="ghost" onClick={props.onRequestOverwrite}>
              Use my edits
            </Button>
          ) : (
            <Button size="xs" variant="ghost" onClick={props.onCancel}>
              Cancel
            </Button>
          )}
          <Button
            size="xs"
            variant={visibleNotice.kind === "confirm-overwrite" ? "destructive" : "outline"}
            onClick={() =>
              props.onResolve(visibleNotice.kind === "confirm-overwrite" ? "retry" : "discard")
            }
          >
            {visibleNotice.kind === "confirm-overwrite" ? "Overwrite disk" : "Reload from disk"}
          </Button>
        </div>
      ) : null}
      {props.relativePath && props.readError && props.hasFallbackData ? (
        <div
          className="flex shrink-0 items-center gap-2 border-b border-destructive/20 bg-destructive/5 px-3 py-2 text-[11px] text-destructive"
          role="alert"
        >
          <AlertTriangle className="size-3.5 shrink-0" aria-hidden="true" />
          <span className="min-w-0 flex-1 truncate">
            The latest version could not be loaded. Showing the last available copy.
          </span>
          <Button size="xs" variant="outline" onClick={props.onReload}>
            Try again
          </Button>
        </div>
      ) : null}
    </>
  );
}
