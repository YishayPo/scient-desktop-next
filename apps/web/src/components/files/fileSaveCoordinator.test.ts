import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/unstable/reactivity";

import { FileSaveCoordinator } from "./fileSaveCoordinator";

function deferred<E = never>() {
  let resolve!: (result: AtomCommandResult<void, E>) => void;
  const promise = new Promise<AtomCommandResult<void, E>>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

describe("FileSaveCoordinator", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("debounces edits and persists only the latest contents", async () => {
    vi.useFakeTimers();
    const persist = vi
      .fn<(contents: string) => Promise<AtomCommandResult<void, never>>>()
      .mockResolvedValue(AsyncResult.success(undefined));
    const onPendingChange = vi.fn();
    const onConfirmed = vi.fn();
    const coordinator = new FileSaveCoordinator({
      debounceMs: 500,
      initialRevision: "revision-1",
      persist,
      revisionFromResult: () => "revision-2",
      onPendingChange,
      onConfirmed,
    });

    coordinator.change("first");
    await vi.advanceTimersByTimeAsync(300);
    coordinator.change("latest");
    await vi.advanceTimersByTimeAsync(499);
    expect(persist).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    expect(persist).toHaveBeenCalledOnce();
    expect(persist).toHaveBeenCalledWith("latest", "revision-1");
    expect(onConfirmed).toHaveBeenCalledWith("latest", undefined);
    expect(onPendingChange.mock.calls).toEqual([[true], [true], [false]]);
  });

  it("keeps pending state until an edit made during a write is also saved", async () => {
    vi.useFakeTimers();
    const firstWrite = deferred();
    const persist = vi
      .fn<(contents: string) => Promise<AtomCommandResult<void, never>>>()
      .mockReturnValueOnce(firstWrite.promise)
      .mockResolvedValueOnce(AsyncResult.success(undefined));
    const onPendingChange = vi.fn();
    const coordinator = new FileSaveCoordinator({
      debounceMs: 500,
      initialRevision: "revision-1",
      persist,
      revisionFromResult: () => "revision-2",
      onPendingChange,
      onConfirmed: vi.fn(),
    });

    coordinator.change("first");
    await vi.advanceTimersByTimeAsync(500);
    coordinator.change("latest");
    await vi.advanceTimersByTimeAsync(500);
    expect(persist).toHaveBeenCalledTimes(1);

    firstWrite.resolve(AsyncResult.success(undefined));
    await vi.runAllTimersAsync();
    expect(persist).toHaveBeenCalledTimes(2);
    expect(persist).toHaveBeenLastCalledWith("latest", "revision-2");
    expect(onPendingChange.mock.calls.at(-1)).toEqual([false]);
  });

  it("leaves the file pending when the latest write fails", async () => {
    vi.useFakeTimers();
    const onPendingChange = vi.fn();
    const coordinator = new FileSaveCoordinator({
      debounceMs: 500,
      initialRevision: "revision-1",
      persist: vi
        .fn()
        .mockResolvedValue(AsyncResult.failure(Cause.fail(new Error("write failed")))),
      onPendingChange,
      revisionFromResult: () => "revision-2",
      onConfirmed: vi.fn(),
    });

    coordinator.change("latest");
    await vi.advanceTimersByTimeAsync(500);
    await Promise.resolve();
    expect(onPendingChange).toHaveBeenCalledWith(true);
    expect(onPendingChange).not.toHaveBeenCalledWith(false);
  });

  it("adopts external revisions only while there is no pending local edit", async () => {
    vi.useFakeTimers();
    const persist = vi.fn().mockResolvedValue(AsyncResult.success(undefined));
    const coordinator = new FileSaveCoordinator({
      debounceMs: 500,
      initialRevision: "revision-1",
      persist,
      revisionFromResult: () => "revision-4",
      onPendingChange: vi.fn(),
      onConfirmed: vi.fn(),
    });

    coordinator.syncConfirmedFileRevision("revision-2");
    coordinator.change("local edit");
    coordinator.syncConfirmedFileRevision("revision-3");
    await vi.advanceTimersByTimeAsync(500);

    expect(persist).toHaveBeenCalledWith("local edit", "revision-2");
    coordinator.dispose();
    await vi.runAllTimersAsync();
    expect(persist).toHaveBeenCalledOnce();
  });

  it("discards a pending local buffer without persisting it", async () => {
    vi.useFakeTimers();
    const persist = vi.fn().mockResolvedValue(AsyncResult.success(undefined));
    const onPendingChange = vi.fn();
    const onResolutionApplied = vi.fn();
    const coordinator = new FileSaveCoordinator({
      debounceMs: 500,
      initialRevision: "revision-1",
      persist,
      revisionFromResult: () => "revision-2",
      onPendingChange,
      onConfirmed: vi.fn(),
      onResolutionApplied,
    });

    coordinator.change("local edit");
    coordinator.discardPending("revision-agent");
    await vi.runAllTimersAsync();

    expect(persist).not.toHaveBeenCalled();
    expect(onPendingChange.mock.calls.at(-1)).toEqual([false]);
    expect(onResolutionApplied).toHaveBeenCalledOnce();
    coordinator.dispose();
    await vi.runAllTimersAsync();
    expect(persist).not.toHaveBeenCalled();
  });

  it("retries a conflicted buffer against the explicitly accepted disk revision", async () => {
    vi.useFakeTimers();
    const conflict = AsyncResult.failure(Cause.fail(new Error("revision conflict")));
    const persist = vi
      .fn()
      .mockResolvedValueOnce(conflict)
      .mockResolvedValueOnce(AsyncResult.success(undefined));
    const onFailure = vi.fn();
    const onResolutionApplied = vi.fn();
    const coordinator = new FileSaveCoordinator({
      debounceMs: 500,
      initialRevision: "revision-1",
      persist,
      revisionFromResult: () => "revision-local",
      onPendingChange: vi.fn(),
      onConfirmed: vi.fn(),
      onFailure,
      onResolutionApplied,
    });

    coordinator.change("local edit");
    await vi.advanceTimersByTimeAsync(500);
    expect(onFailure).toHaveBeenCalledWith("local edit", conflict);

    coordinator.retryPending("revision-agent");
    await vi.runAllTimersAsync();
    expect(persist.mock.calls).toEqual([
      ["local edit", "revision-1"],
      ["local edit", "revision-agent"],
    ]);
    expect(onResolutionApplied).toHaveBeenCalledOnce();
  });

  it("applies discard only after an in-flight write settles", async () => {
    vi.useFakeTimers();
    const firstWrite = deferred<Error>();
    const persist = vi.fn().mockReturnValue(firstWrite.promise);
    const onPendingChange = vi.fn();
    const onResolutionApplied = vi.fn();
    const coordinator = new FileSaveCoordinator({
      debounceMs: 500,
      initialRevision: "revision-1",
      persist,
      revisionFromResult: () => "revision-local",
      onPendingChange,
      onConfirmed: vi.fn(),
      onResolutionApplied,
    });

    coordinator.change("local edit");
    await vi.advanceTimersByTimeAsync(500);
    coordinator.discardPending("revision-agent");
    expect(onResolutionApplied).not.toHaveBeenCalled();

    firstWrite.resolve(AsyncResult.failure(Cause.fail(new Error("revision conflict"))));
    await vi.runAllTimersAsync();

    expect(persist).toHaveBeenCalledOnce();
    expect(onPendingChange.mock.calls.at(-1)).toEqual([false]);
    expect(onResolutionApplied).toHaveBeenCalledOnce();
  });

  it("retries after an in-flight conflict and remains pending after a second conflict", async () => {
    vi.useFakeTimers();
    const firstWrite = deferred<Error>();
    const conflict: AtomCommandResult<void, Error> = AsyncResult.failure(
      Cause.fail(new Error("revision conflict")),
    );
    const persist = vi.fn().mockReturnValueOnce(firstWrite.promise).mockResolvedValueOnce(conflict);
    const onFailure = vi.fn();
    const onPendingChange = vi.fn();
    const onResolutionApplied = vi.fn();
    const coordinator = new FileSaveCoordinator({
      debounceMs: 500,
      initialRevision: "revision-1",
      persist,
      revisionFromResult: () => "revision-local",
      onPendingChange,
      onConfirmed: vi.fn(),
      onFailure,
      onResolutionApplied,
    });

    coordinator.change("local edit");
    await vi.advanceTimersByTimeAsync(500);
    coordinator.retryPending("revision-agent");
    firstWrite.resolve(conflict);
    await vi.runAllTimersAsync();

    expect(persist.mock.calls).toEqual([
      ["local edit", "revision-1"],
      ["local edit", "revision-agent"],
    ]);
    expect(onFailure).toHaveBeenCalledTimes(2);
    expect(onPendingChange.mock.calls.at(-1)).toEqual([true]);
    expect(onPendingChange).not.toHaveBeenCalledWith(false);
    expect(onResolutionApplied).toHaveBeenCalledOnce();
  });
});
