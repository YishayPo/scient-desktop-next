import type {
  PdfSourceActions,
  PdfSourceDescriptor,
  PdfSourceResolver,
} from "@scientfactory/document-artifacts";
import { LegendList } from "@legendapp/list/react";
import {
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  Download,
  Ellipsis,
  FileText,
  ListTree,
  LoaderCircle,
  Maximize2,
  Minus,
  PanelLeftClose,
  PanelLeftOpen,
  Plus,
  RotateCw,
  Search,
  FolderSearch,
  X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "~/components/ui/menu";
import { ensureLocalApi } from "~/localApi";
import { cn } from "~/lib/utils";

import { PdfOutline } from "./PdfOutline";
import { PdfThumbnail } from "./PdfThumbnail";
import { webPdfSourceActions, webPdfSourceResolver } from "./pdfSource";
import {
  formatPdfZoom,
  parsePdfPageInput,
  parseSafePdfExternalUrl,
  stepPdfZoom,
  type PdfSidebarMode,
} from "./pdfReaderModel";
import { useScientPdfReader } from "./useScientPdfReader";

import "pdfjs-dist/legacy/web/pdf_viewer.css";
import "./scientPdfReader.css";

function ReaderButton(props: React.ButtonHTMLAttributes<HTMLButtonElement> & { label: string }) {
  const { label, className, children, ...buttonProps } = props;
  return (
    <button
      {...buttonProps}
      type="button"
      className={cn("scient-pdf-toolbar-button", className)}
      aria-label={label}
      title={label}
    >
      {children}
    </button>
  );
}

function PdfPasswordPrompt(props: {
  readonly incorrect: boolean;
  readonly onSubmit: (password: string) => boolean;
}) {
  const [password, setPassword] = useState("");
  return (
    <form
      className="scient-pdf-state-card"
      onSubmit={(event) => {
        event.preventDefault();
        if (props.onSubmit(password)) setPassword("");
      }}
    >
      <FileText className="size-6 text-muted-foreground" aria-hidden="true" />
      <h2>Password protected PDF</h2>
      <p>
        {props.incorrect
          ? "That password is incorrect. Try again."
          : "Enter the password to open this PDF."}
      </p>
      <div className="flex w-full max-w-72 gap-2">
        <input
          autoFocus
          type="password"
          value={password}
          onChange={(event) => setPassword(event.target.value)}
          className="scient-pdf-password-input"
          aria-label="PDF password"
        />
        <button type="submit" className="scient-pdf-primary-button" disabled={!password}>
          Open
        </button>
      </div>
    </form>
  );
}

export function ScientPdfReader(props: {
  readonly actions?: PdfSourceActions;
  readonly refreshKey?: number;
  readonly resolver?: PdfSourceResolver;
  readonly source: PdfSourceDescriptor;
}) {
  const resolver = props.resolver ?? webPdfSourceResolver;
  const asset = resolver.useResolve(props.source);
  const previousRefreshKey = useRef(props.refreshKey);
  useEffect(() => {
    if (previousRefreshKey.current === props.refreshKey) return;
    previousRefreshKey.current = props.refreshKey;
    asset.refresh();
  }, [asset.refresh, props.refreshKey]);
  if (asset._tag === "Failure") {
    return (
      <div className="scient-pdf-reader">
        <div className="scient-pdf-state-card text-destructive">
          <FileText className="size-6" aria-hidden="true" />
          <h2>Unable to open PDF</h2>
          <p>Scient could not create an authorized preview for this file.</p>
        </div>
      </div>
    );
  }
  if (asset._tag !== "Success") {
    return (
      <div className="scient-pdf-reader">
        <div className="scient-pdf-state-card">
          <LoaderCircle className="size-6 animate-spin text-muted-foreground" aria-hidden="true" />
          <p>Preparing PDF…</p>
        </div>
      </div>
    );
  }
  return (
    <LoadedScientPdfReader
      source={props.source}
      sourceUrl={asset.url}
      sourceExpiresAt={asset.expiresAt}
      refreshSource={asset.refresh}
      actions={props.actions ?? webPdfSourceActions}
    />
  );
}

function LoadedScientPdfReader(props: {
  readonly actions: PdfSourceActions;
  readonly source: PdfSourceDescriptor;
  readonly refreshSource: () => void;
  readonly sourceExpiresAt: number;
  readonly sourceUrl: string;
}) {
  const [container, setContainer] = useState<HTMLDivElement | null>(null);
  const [viewerElement, setViewerElement] = useState<HTMLDivElement | null>(null);
  const [sidebar, setSidebar] = useState<PdfSidebarMode>("closed");
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const searchRef = useRef<HTMLInputElement>(null);
  const rootRef = useRef<HTMLDivElement>(null);
  const [pageInput, setPageInput] = useState("1");
  const reader = useScientPdfReader({
    documentKey: JSON.stringify([props.source.authority, props.source.logicalDocumentKey]),
    onSourceInvalidated: props.refreshSource,
    sourceUrl: props.sourceUrl,
    container,
    viewerElement,
  });
  const { state } = reader;
  const thumbnailPages = useMemo(
    () => Array.from({ length: state.pageCount }, (_, index) => index + 1),
    [state.pageCount],
  );

  useEffect(() => setPageInput(String(state.page)), [state.page]);
  useEffect(() => {
    if (!searchOpen) return;
    reader.prepareSearch();
    searchRef.current?.focus();
  }, [reader.prepareSearch, searchOpen]);

  const closeSearch = useCallback(() => {
    setSearchOpen(false);
    setSearchQuery("");
    reader.closeSearch();
  }, [reader.closeSearch]);

  const onReaderKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
    const modified = event.metaKey || event.ctrlKey;
    if (modified && event.key.toLowerCase() === "f") {
      event.preventDefault();
      setSearchOpen(true);
      return;
    }
    if (event.key === "Escape" && searchOpen) {
      event.preventDefault();
      closeSearch();
      return;
    }
    const target = event.target as HTMLElement;
    if (!modified || target.matches("input, textarea, [contenteditable='true']")) return;
    if (event.key === "+" || event.key === "=") {
      event.preventDefault();
      reader.setZoom(stepPdfZoom(state.scale, "in"));
    } else if (event.key === "-") {
      event.preventDefault();
      reader.setZoom(stepPdfZoom(state.scale, "out"));
    } else if (event.key === "0") {
      event.preventDefault();
      reader.setZoomMode("page-actual");
    }
  };

  const commitPage = () => {
    const page = parsePdfPageInput(pageInput, state.pageCount);
    if (page === null) {
      setPageInput(String(state.page));
      return;
    }
    reader.goToPage(page);
  };

  return (
    <div
      ref={rootRef}
      className="scient-pdf-reader"
      aria-label={`PDF reader: ${props.source.fileName}`}
      onKeyDown={onReaderKeyDown}
    >
      <div className="scient-pdf-toolbar" role="toolbar" aria-label="PDF controls">
        <ReaderButton
          label={sidebar === "closed" ? "Show thumbnails" : "Hide PDF sidebar"}
          onClick={() => setSidebar((current) => (current === "closed" ? "thumbnails" : "closed"))}
        >
          {sidebar === "closed" ? <PanelLeftOpen /> : <PanelLeftClose />}
        </ReaderButton>
        <div className="scient-pdf-toolbar-separator" />
        <ReaderButton
          label="Previous page"
          disabled={state.phase !== "ready" || state.page <= 1}
          onClick={() => reader.goToPage(state.page - 1)}
        >
          <ChevronLeft />
        </ReaderButton>
        <div className="scient-pdf-page-control">
          <input
            value={pageInput}
            inputMode="numeric"
            aria-label="Page number"
            disabled={state.phase !== "ready"}
            onChange={(event) => setPageInput(event.target.value)}
            onBlur={commitPage}
            onKeyDown={(event) => {
              if (event.key === "Enter") commitPage();
            }}
          />
          <span aria-label={`${state.pageCount} pages`}>/ {state.pageCount || "–"}</span>
        </div>
        <ReaderButton
          label="Next page"
          disabled={state.phase !== "ready" || state.page >= state.pageCount}
          onClick={() => reader.goToPage(state.page + 1)}
        >
          <ChevronRight />
        </ReaderButton>
        <div className="scient-pdf-toolbar-separator" />
        <ReaderButton
          label="Zoom out"
          disabled={state.phase !== "ready"}
          onClick={() => reader.setZoom(stepPdfZoom(state.scale, "out"))}
        >
          <Minus />
        </ReaderButton>
        <button
          type="button"
          className="scient-pdf-zoom-label"
          disabled={state.phase !== "ready"}
          onClick={() => reader.setZoomMode("page-actual")}
          title="Actual size"
        >
          {formatPdfZoom(state.scale)}
        </button>
        <ReaderButton
          label="Zoom in"
          disabled={state.phase !== "ready"}
          onClick={() => reader.setZoom(stepPdfZoom(state.scale, "in"))}
        >
          <Plus />
        </ReaderButton>
        <ReaderButton
          label="Fit width"
          disabled={state.phase !== "ready"}
          onClick={() => reader.setZoomMode("page-width")}
        >
          <Maximize2 />
        </ReaderButton>
        <ReaderButton
          label="Rotate clockwise"
          disabled={state.phase !== "ready"}
          onClick={reader.rotate}
        >
          <RotateCw />
        </ReaderButton>
        <div className="min-w-1 flex-1" />
        <ReaderButton
          label="Search PDF"
          aria-pressed={searchOpen}
          onClick={() => (searchOpen ? closeSearch() : setSearchOpen(true))}
        >
          <Search />
        </ReaderButton>
        <DropdownMenu>
          <DropdownMenuTrigger
            className="scient-pdf-toolbar-button"
            aria-label="More PDF actions"
            title="More PDF actions"
          >
            <Ellipsis />
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            {props.source.capabilities.canSaveCopy ? (
              <DropdownMenuItem
                onClick={() => {
                  props.actions.saveCopy(props.source, {
                    url: props.sourceUrl,
                    expiresAt: props.sourceExpiresAt,
                    refresh: props.refreshSource,
                  });
                }}
              >
                <Download /> Save a copy…
              </DropdownMenuItem>
            ) : null}
            {props.source.capabilities.canRevealSource && props.actions.revealSource ? (
              <DropdownMenuItem
                onClick={() =>
                  props.actions.revealSource?.(props.source, {
                    url: props.sourceUrl,
                    expiresAt: props.sourceExpiresAt,
                    refresh: props.refreshSource,
                  })
                }
              >
                <FolderSearch /> Reveal source
              </DropdownMenuItem>
            ) : null}
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
      {props.source._tag === "generated-pdf" && props.source.bindingStatus === "stale" ? (
        <div className="scient-pdf-notice" role="status">
          The latest build failed. Showing the last successful PDF.
          {props.source.staleReason ? ` ${props.source.staleReason}` : ""}
        </div>
      ) : null}
      {searchOpen ? (
        <form
          className="scient-pdf-searchbar"
          onSubmit={(event) => {
            event.preventDefault();
            reader.findAgain(false);
          }}
        >
          <Search className="size-3.5 text-muted-foreground" aria-hidden="true" />
          <input
            ref={searchRef}
            value={searchQuery}
            placeholder="Search this PDF"
            aria-label="Search this PDF"
            onChange={(event) => {
              const value = event.target.value;
              setSearchQuery(value);
              reader.setSearchQuery(value);
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                event.preventDefault();
                reader.findAgain(event.shiftKey);
              } else if (event.key === "ArrowDown") {
                event.preventDefault();
                reader.findAgain(false);
              } else if (event.key === "ArrowUp") {
                event.preventDefault();
                reader.findAgain(true);
              }
            }}
          />
          <span className="scient-pdf-find-count">
            {state.findCount.total > 0
              ? `${state.findCount.current} of ${state.findCount.total}`
              : state.findPhase === "not-found"
                ? "0 of 0"
                : ""}
          </span>
          <ReaderButton
            label="Previous result"
            disabled={!searchQuery}
            onClick={() => reader.findAgain(true)}
          >
            <ChevronDown className="rotate-180" />
          </ReaderButton>
          <ReaderButton
            label="Next result"
            disabled={!searchQuery}
            onClick={() => reader.findAgain(false)}
          >
            <ChevronDown />
          </ReaderButton>
          <ReaderButton label="Close search" onClick={closeSearch}>
            <X />
          </ReaderButton>
        </form>
      ) : null}
      {state.scanned === true && state.phase === "ready" ? (
        <div className="scient-pdf-notice">
          No selectable text was detected on the opening pages. Search and copying may be limited.
        </div>
      ) : null}
      <div className="scient-pdf-body">
        {sidebar !== "closed" && state.phase === "ready" && reader.runtimeRef.current ? (
          <aside className="scient-pdf-sidebar" aria-label="PDF navigation">
            <div className="scient-pdf-sidebar-tabs">
              <button
                type="button"
                data-active={sidebar === "thumbnails" || undefined}
                onClick={() => setSidebar("thumbnails")}
              >
                <FileText /> Pages
              </button>
              <button
                type="button"
                data-active={sidebar === "outline" || undefined}
                onClick={() => setSidebar("outline")}
              >
                <ListTree /> Outline
              </button>
            </div>
            <div className="scient-pdf-sidebar-content">
              {sidebar === "thumbnails" ? (
                <LegendList<number>
                  data={thumbnailPages}
                  keyExtractor={(pageNumber) => String(pageNumber)}
                  estimatedItemSize={205}
                  drawDistance={410}
                  className="scient-pdf-thumbnails"
                  renderItem={({ item: pageNumber }) => (
                    <PdfThumbnail
                      pageNumber={pageNumber}
                      active={state.page === pageNumber}
                      runtime={reader.runtimeRef.current!}
                      onSelect={reader.goToPage}
                    />
                  )}
                />
              ) : (
                <PdfOutline
                  items={state.outline}
                  onDestination={reader.goToDestination}
                  onExternalUrl={(rawUrl) => {
                    const url = parseSafePdfExternalUrl(rawUrl);
                    if (url)
                      void ensureLocalApi()
                        .shell.openExternal(url)
                        .catch(() => undefined);
                  }}
                />
              )}
            </div>
          </aside>
        ) : null}
        <div className="scient-pdf-content">
          <div ref={setContainer} className="scient-pdf-viewer-container" tabIndex={0}>
            <div ref={setViewerElement} className="pdfViewer" />
          </div>
          {state.phase === "loading" ? (
            <div className="scient-pdf-state-overlay">
              <div className="scient-pdf-state-card">
                <LoaderCircle
                  className="size-6 animate-spin text-muted-foreground"
                  aria-hidden="true"
                />
                <p>Loading PDF…</p>
                {state.progress !== null && state.progress < 1 ? (
                  <div
                    className="scient-pdf-progress"
                    role="progressbar"
                    aria-valuenow={Math.round(state.progress * 100)}
                  >
                    <span style={{ width: `${Math.round(state.progress * 100)}%` }} />
                  </div>
                ) : null}
              </div>
            </div>
          ) : state.phase === "password" ? (
            <div className="scient-pdf-state-overlay">
              <PdfPasswordPrompt
                incorrect={state.passwordReason === "incorrect"}
                onSubmit={reader.submitPassword}
              />
            </div>
          ) : state.phase === "error" ? (
            <div className="scient-pdf-state-overlay">
              <div className="scient-pdf-state-card text-destructive">
                <FileText className="size-6" aria-hidden="true" />
                <h2>Unable to open PDF</h2>
                <p>{state.error}</p>
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
