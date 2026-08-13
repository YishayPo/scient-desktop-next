import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";

type AtomCommandFailure<A, E> = Extract<AtomCommandResult<A, E>, { readonly _tag: "Failure" }>;

export interface FileSaveCoordinatorOptions<A, E> {
  readonly debounceMs: number;
  readonly initialRevision: string;
  readonly persist: (
    contents: string,
    expectedRevision: string,
  ) => Promise<AtomCommandResult<A, E>>;
  readonly revisionFromResult: (value: A) => string;
  readonly onPendingChange: (pending: boolean) => void;
  readonly onConfirmed: (contents: string, value: A) => void;
  readonly onFailure?: (contents: string, result: AtomCommandFailure<A, E>) => void;
  readonly onResolutionApplied?: () => void;
}

type PendingResolution =
  | { readonly _tag: "discard"; readonly revision: string }
  | { readonly _tag: "retry"; readonly revision: string };

export class FileSaveCoordinator<A = unknown, E = unknown> {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private latestContents = "";
  private latestRevision = 0;
  private confirmedEditRevision = 0;
  private lastChangeAt = 0;
  private saving = false;
  private disposed = false;
  private pendingResolution: PendingResolution | null = null;
  private confirmedFileRevision: string;

  constructor(private readonly options: FileSaveCoordinatorOptions<A, E>) {
    this.confirmedFileRevision = options.initialRevision;
  }

  change(contents: string): void {
    this.latestContents = contents;
    this.latestRevision += 1;
    this.lastChangeAt = Date.now();
    this.options.onPendingChange(true);
    this.schedule(this.options.debounceMs);
  }

  dispose(): void {
    this.disposed = true;
    this.clearTimer();
    if (this.latestRevision > this.confirmedEditRevision) void this.persistLatest();
  }

  syncConfirmedFileRevision(revision: string): void {
    if (!this.saving && this.latestRevision === this.confirmedEditRevision) {
      this.confirmedFileRevision = revision;
    }
  }

  /** Discard the unsaved buffer and adopt the authoritative on-disk revision. */
  discardPending(revision: string): void {
    this.resolvePending({ _tag: "discard", revision });
  }

  /**
   * Explicitly retry the local buffer against a newly read disk revision.
   * The compare-and-swap write still rejects a third concurrent change.
   */
  retryPending(revision: string): void {
    this.resolvePending({ _tag: "retry", revision });
  }

  private resolvePending(resolution: PendingResolution): void {
    this.clearTimer();
    if (this.saving) {
      this.pendingResolution = resolution;
      return;
    }
    this.applyResolution(resolution);
  }

  private applyResolution(resolution: PendingResolution): void {
    this.confirmedFileRevision = resolution.revision;
    if (resolution._tag === "discard") {
      this.confirmedEditRevision = this.latestRevision;
      this.options.onPendingChange(false);
      this.options.onResolutionApplied?.();
      return;
    }
    this.schedule(0);
    this.options.onResolutionApplied?.();
  }

  private schedule(delay: number): void {
    this.clearTimer();
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.persistLatest();
    }, delay);
  }

  private clearTimer(): void {
    if (this.timer === null) return;
    clearTimeout(this.timer);
    this.timer = null;
  }

  private async persistLatest(): Promise<void> {
    if (this.saving || this.latestRevision === this.confirmedEditRevision) return;

    this.saving = true;
    const contents = this.latestContents;
    const revision = this.latestRevision;
    const result = await this.options.persist(contents, this.confirmedFileRevision);
    const succeeded = result._tag === "Success";
    if (result._tag === "Success") {
      this.confirmedFileRevision = this.options.revisionFromResult(result.value);
      this.confirmedEditRevision = revision;
      this.options.onConfirmed(contents, result.value);
    } else {
      this.options.onFailure?.(contents, result);
    }

    this.saving = false;
    if (this.pendingResolution !== null) {
      const resolution = this.pendingResolution;
      this.pendingResolution = null;
      this.applyResolution(resolution);
      return;
    }
    if (revision === this.latestRevision) {
      if (succeeded) this.options.onPendingChange(false);
      return;
    }

    const remainingDebounce = Math.max(
      0,
      this.options.debounceMs - (Date.now() - this.lastChangeAt),
    );
    if (this.disposed) {
      void this.persistLatest();
    } else {
      this.schedule(remainingDebounce);
    }
  }
}
