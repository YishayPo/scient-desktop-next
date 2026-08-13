# Scient analysis runtime foundation

Status: local implementation candidate for review. This is not a release record or cross-platform
acceptance claim. The realigned candidate includes the PDF foundation and current shared Scient
file/viewer seams. A real installed MATLAB R2026a has passed the focused local verification and
hidden PNG/FIG capture acceptance on the current Apple Silicon host.

## Decision

The first vertical slice runs a saved, project-owned `.m` file with a user-installed MATLAB,
streams output, supports Stop and per-file history, and publishes captured figures. It also
establishes the runtime-neutral contracts needed by later Python, R, notebook, typesetting, and
agent-triggered execution without pretending those integrations already exist.

The candidate follows the Scientific Computing and Data Analysis roadmap owned in the Scient
documentation repository. The user explicitly selected MATLAB as the first analysis proof. That
changes adapter sequencing, not the shared architecture or the later Python/R parity gate.

## Owned boundaries

| Boundary                   | Owner                                           | Responsibility                                                                                                                                      |
| -------------------------- | ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| Execution vocabulary       | `packages/scient-execution`                     | Run IDs, statuses, transition rules, bounded output records, receipt base, process port, and deterministic simulator                                |
| Analysis specialization    | `packages/scient-analysis`                      | Runtime profiles, adapter contract/registry fields, source revisions, run actions, analysis receipts, stream events, and simulator                  |
| Host execution             | `apps/server/src/scient/execution`              | No-shell process spawn, stdout/stderr transport, exit observation, and owned process-tree cancellation                                              |
| Analysis coordination      | `apps/server/src/scient/analysis`               | Adapter inspection, source/revision checks, lifecycle, cancellation race handling, output bounds, local journal, recovery, history, and RPC service |
| Client projection          | `packages/client-runtime/src/state/analysis.ts` | Summary/snapshot/delta folding and bounded query/subscription lifetime                                                                              |
| Scient file extension seam | `apps/web/src/scient/fileSurfaces`              | Routes format-specific auxiliary surfaces without teaching the inherited viewer about MATLAB or future runtimes                                     |
| Shared file truth          | workspace read/write RPC                        | Content revision, conditional save, atomic replacement, truncated-save refusal, and permission preservation                                         |

The inherited `FilePreviewPanel` receives one Scient auxiliary-surface mount, one generic Scient
file-freshness hook, and the existing Scient language override. Runtime matching, MATLAB UI,
freshness policy, and future additions stay below `apps/web/src/scient`. This is the intentional T3
conflict-containment boundary.

The generic file-truth changes are protected upstream seams: `WorkspaceFileSystem`, the project
read/write/watch contract, `FileSaveCoordinator`, and the two additive `FilePreviewPanel` bindings.
Every T3 refresh that touches those files must preserve conditional-write/truncation behavior,
exact-file freshness, and both Scient mounts. The static viewer-seam audit enforces those bindings;
MATLAB names, commands, and routing must not move into the inherited panel.

[`scient-analysis-seams.json`](../../scient-analysis-seams.json) is the machine-readable inventory
of Scient-owned roots and shared T3 mount points. `pnpm analysis:seams:check` verifies that every
recorded path and stable anchor still exists. The manifest documents why a shared file changed
without claiming that generic revision, watching, or mini-player behavior must remain fork-only;
those generic changes remain candidates for separate upstream contribution.

## Run flow

1. Opening `.m` source remains independent of MATLAB discovery. Missing or invalid MATLAB never
   blocks viewing or editing.
2. A read returns a SHA-256 content revision. Autosave writes conditionally against the last
   confirmed revision and returns the new revision after atomic replacement.
   A per-resolved-file mutex closes in-process compare/write races without serializing unrelated
   project files.
3. Run is disabled while a save is pending. Start also rereads the file and rejects a stale,
   truncated, non-text, outside-project, unsupported, or already-active source.
4. The registry resolves the requested adapter. The MATLAB adapter discovers an explicit path,
   `PATH`, or conventional installation without launching MATLAB. Discovery means “found,” not
   “ready”: an explicit **Verify** action starts a private 90-second probe and reports release,
   version, architecture, installation root, Java availability, toolbox inventory, duration, and
   actionable sign-in, license, dependency, startup, or timeout states. Results are cached by the
   resolved executable identity and invalidated on configuration or launch failure.
5. Starts enter one FIFO worker queue per runtime. A single local MATLAB process runs at a time;
   queued positions are explicit and a queued run can be cancelled without launching MATLAB.
6. The host creates a private runner, then spawns the exact executable directly, never through a
   shell. MATLAB receives the static `-batch` expression
   `cd(matlabroot);run(getenv('SCIENT_MATLAB_RUNNER'))`; changing to `matlabroot` prevents a
   project-owned `run.m` from shadowing MATLAB's built-in launcher. The private runner reads the
   absolute source path from `SCIENT_MATLAB_ENTRYPOINT`, so spaces, quotes, and shell
   metacharacters do not alter the command or the user file.
7. The server streams bounded stdout/stderr deltas, journals output before publication, persists
   lifecycle summaries atomically, and records the exact runtime profile, source revision, and a
   terminal SHA-256 over canonical captured output (ordered stream identity plus text).
8. A caught MATLAB `MException` is written to a private JSON side channel before the runner
   rethrows it. Scient turns that bounded payload into a structured diagnostic with project-local,
   clickable stack locations; raw stdout/stderr stays separate and expandable.
9. After the user source finishes, the private runner captures at most 50 figures as a PNG preview
   plus native FIG continuation file. Each representation is finalized atomically, hashed, and
   published under the run-owned analysis artifact store. A structured artifact receipt records
   successful collection even when there were no figures and exposes collection failure without
   changing an otherwise successful process result.
10. Stop handles queued/starting/running races and asks the host adapter to terminate the owned
    process tree. App shutdown closes the server execution scope; an unfinished persisted run is
    recovered as `lost` on the next start.
11. An exact-file native watcher treats events only as hints and rereads authoritatively on watcher
    readiness and change. Clean files refresh automatically; dirty buffers require an explicit
    reload or overwrite decision. There is no polling and no project-tree watcher.

## Performance and reliability budgets

- Captured process output is capped at 4 MiB per run. The truncation is explicit in the receipt
  and output, and terminal state is still preserved.
- One transport chunk is at most 256 KiB and respects UTF-8 boundaries.
- Adjacent process output is coalesced for at most 25 ms or 64 events, limiting state copies,
  journal writes, and websocket frames without making interactive output feel delayed.
- The server publication queue is bounded to 256 events. A slow subscriber applies backpressure
  instead of causing unbounded memory growth.
- List and lifecycle messages contain summaries, not accumulated output. Live streams send one
  active snapshot followed by summary updates and output deltas.
- Completed output is evicted from server memory. Full output is loaded from the append-only
  journal only for `analysis.getRun`; client detail atoms expire after one minute idle.
- Canonical run receipts remain on disk. A rebuildable Scient-owned SQLite projection makes
  history lookup independent of directory size, uses opaque keyset cursors, and rebuilds lazily
  after an index-write failure. A durable generation/clean marker lets normal restarts restore
  only nonterminal receipts, so terminal history never requires a directory-wide scan. An
  incomplete or overlapping generation remains dirty until a complete rebuild commits. The
  default history query returns 20 runs; callers may request at most 100; the file panel asks for
  30 and loads older pages explicitly.
- A start lock makes the source check and active-run registration atomic. Only one active run is
  allowed per project file, and the runtime worker enforces deterministic FIFO execution across
  files instead of relying on semaphore scheduling fairness. Each runtime queue is bounded at 100
  waiting runs and rejects further starts with an actionable error.
- Output journal decoding recovers valid records and adds an explicit system warning if records
  are unreadable. A corrupt journal cannot retain a misleading output hash. Interrupted-run
  recovery reconstructs output size before writing the `lost` receipt. It retains a fidelity hash
  only when the recovered journal is complete; corrupt or already truncated output stays unhashed.
- Runtime inspection is cached and invalidated after configuration or a launch failure. Discovery
  performs bounded conventional-directory reads and never scans an entire disk.
- Invalid environment runtime settings are reported as a recoverable setup warning. Passive
  inspection stays non-mutating; explicitly choosing MATLAB atomically replaces the unreadable
  settings. An unreadable run receipt fails visibly rather than silently disappearing from
  history.
- Default figure capture writes and accepts two runner-declared representations per figure (PNG
  and FIG), not five. Undeclared SVG, PDF, and interactive HTML files in the private staging folder
  are ignored until a future explicit producer opt-in exists. The viewer opens the static
  representation by default. Partial capture publishes valid representations and records a
  separate capture warning instead of pretending that the calculation failed or that no figure
  existed.

The canonical `run.json` retains the full verified runtime profile. Paged history rows and live
summary updates omit the repeated toolbox inventory; runtime inspection and `getRun` remain the
authoritative paths for that detail.

Per-run growth is bounded, and this candidate never silently deletes old run directories. The
indexed/paginated projection removes history-query growth from normal interaction, while receipts
and artifact hashes on disk remain canonical. Explicit per-run and project cleanup first confirms
the currently measured retained bytes, transactionally removes only output/artifact payloads, and
keeps a metadata-only receipt with diagnostics, hashes, artifact descriptions, source/runtime
provenance, and removal time. Automatic “keep N and delete the rest” policy remains explicitly out
of scope.

Run metadata and NDJSON output live under the environment's derived
`<state>/analysis` directory. Runtime settings are environment-owned, while run records are keyed
by durable Scient project identity. Standard project source remains the authority; hidden state is
not a replacement for `.m` files.

## PDF and artifact alignment

This slice consumes the same project identity, workspace file truth, server state derivation, and
Scient-owned package pattern as the PDF foundation. It does not create a second PDF source or
viewer. Run outputs use a producer-owned analysis artifact store because they are run results, not
document revisions; their typed asset references still reuse the shared asset and browser
surfaces. PNG is the reliable default view and FIG preserves MATLAB-native continuation. Optional
HTML, SVG, or PDF representations can later be enabled deliberately without changing artifact
identity. Output-file discovery outside the private runner directory remains later work; this
slice never infers artifacts by diffing the user's project tree.

## Platform behavior

- macOS discovery checks `PATH` and `/Applications/MATLAB_R20xx.app/bin/matlab` candidates.
- Windows discovery checks both Program Files roots and uses `matlab.exe`; process-tree behavior
  still requires native Windows acceptance.
- Linux discovery checks `PATH` and `/usr/local/MATLAB/<release>/bin/matlab` candidates.
- A release such as `R2025b` is recorded when it can be derived honestly from the executable path.
  Unknown versions remain `null` until the user explicitly verifies the runtime; opening a file
  never launches MATLAB merely to decorate the UI.
- GUI shell environment differences are handled by conventional discovery and an explicit path
  setting.
  WSL, remote runtimes, and environment modules need later adapters and acceptance fixtures.

## Current boundary

This candidate includes Run file, Stop, deterministic runtime-level FIFO queueing, stdout/stderr,
explicit startup/license/Java/toolbox verification, structured clickable errors, durable indexed
and keyset-paginated terminal history, crash recovery, revision-safe source execution, exact-file
refresh, and bounded PNG/FIG figure capture with explicit artifact-collection status. It does not
yet include Run selection or section, variables, opt-in rich representations, tests, debugging,
`.mlx`/`.mat` viewers, MATLAB MCP, Jupyter attachment, agent initiation, general project-output
discovery, or automatic retention policy. Explicit metadata-preserving cleanup is included.

The shared execution package is intentionally an execution kernel rather than a speculative
all-domain service. An execution-only typesetting contract test proves that a fake
`DocumentBuild` adapter can consume its state machine, process port, cancellation, and simulator
without importing analysis contracts. When the second real consumer exposes genuinely shared
server orchestration, lift that behavior into an `ExecutionCoordinator`; do not make
`DocumentBuild` depend on `AnalysisService`, and do not force either specialized receipt into a
generic task record.

Actor identity is deliberately not represented as a nullable execution-receipt placeholder. Before
agent- or automation-triggered runs land, the accepted Scient operation envelope must supply the
host-resolved actor, project scope, capabilities, authority generation, and operation lineage; the
result receipt then binds to that envelope rather than trusting an analysis-RPC payload.

## Verification contract

The focused suite covers:

- legal and illegal execution state transitions;
- runtime-neutral process and analysis adapter simulators, plus an analysis-free fake typesetting
  consumer;
- MATLAB explicit/PATH/missing discovery, release parsing, private-runner preparation, no-shell
  command preparation, explicit verification status classification, executable-identity caching,
  structured MException collection, bounded default figure formats, partial-capture reporting,
  and rejection of undeclared rich formats;
- real installed MATLAB R2026a startup verification and hidden PNG/FIG figure capture on the
  current Apple Silicon host;
- real host cancellation of a parent and its spawned child process on the current macOS test host;
- revision-conflict refusal, atomic replacement, executable-mode preservation, exact-file watch
  recovery, and explicit dirty-buffer reload/overwrite policy;
- local settings, backwards-compatible receipt decoding, on-demand output, append-only
  persistence, a 10,000-receipt projection rebuild, opaque cursor paging, stale-index repair,
  corruption visibility, and metadata-preserving cleanup recovery;
- summary/snapshot/output-delta client folding;
- source-language correction, inherited-viewer seam containment, uninitialized-project Run state,
  and autosave revision coordination;
- websocket success, stdout/stderr, runtime verification/cache refresh, strict cross-file FIFO,
  queued cancellation without launch, metadata-only list plus on-demand detail, structured
  diagnostics, stale-source refusal, duplicate-run refusal, terminal output hashing,
  bounded/truncated output, startup-race cancellation, retained-storage accounting, stale cleanup
  confirmation, idempotent cleanup, and artifact unavailability after payload removal.

Before review-ready handoff, the repository-wide format, lint, typecheck, test, build, desktop
smoke, brand, diff, and current-main/upstream conflict gates must also pass or be reported exactly.
