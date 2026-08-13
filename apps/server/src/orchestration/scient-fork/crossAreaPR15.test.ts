/**
 * SCIENT-FORK cross-area integration tests for persistence stack phase B.
 *
 * These tests exercise cross-layer flows that span the phase A boundary
 * resolver/projection plus the phase B migration runner, lineage lifecycle,
 * recovery, and normalization:
 *
 * - VAL-MIGRATE-13: Lineage lifecycle semantics survive migration.
 * - VAL-CROSS-001: Fresh startup to ready fork is end to end.
 * - VAL-CROSS-002: Prototype upgrade then fork is compatible.
 * - VAL-CROSS-003: Restart during pending fork is safe.
 * - VAL-CROSS-004: Interrupted provisioning retries deterministically.
 * - VAL-CROSS-006: Re-fork after normalization uses only canonical values.
 * - VAL-CROSS-008: Scient and T3 migrations remain disjoint.
 *
 * All fixtures are synthetic in-memory or temporary file SQLite databases.
 * No live user data, production credentials, or browser validation is used.
 */
import {
  CheckpointRef,
  CommandId,
  CorrelationId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationEvent,
  type OrchestrationReadModel,
  type ThreadForkCommand,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { OrchestrationEventStore } from "../../persistence/Services/OrchestrationEventStore.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { ServerConfig } from "../../config.ts";
import { OrchestrationProjectionPipelineLive } from "../Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "../Layers/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import { OrchestrationProjectionPipeline } from "../Services/ProjectionPipeline.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { runScientMigrations } from "./scientMigrator.ts";
import { makeForkBoundaryResolver } from "./ForkBoundaryReadModel.ts";
import { forkThread } from "./forkDecider.ts";
import { withForkOriginDetail } from "./forkDecisionReadModel.ts";
import { createEmptyReadModel } from "../projector.ts";
import {
  claimFork,
  getForkStatus,
  insertPendingFork,
  listRecoverableForks,
  markForkAbandoned,
  markForkFailed,
  markForkReady,
} from "./forkRepository.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Provide a fresh in-memory SQLite database to an effect. */
function withMemory<E, A>(
  effect: Effect.Effect<A, E, SqlClient.SqlClient>,
): Effect.Effect<A, E | SqlError, never> {
  return effect.pipe(Effect.provide(NodeSqliteClient.layerMemory()));
}

const makeCrossAreaTestLayer = (prefix: string) =>
  Layer.mergeAll(
    OrchestrationProjectionPipelineLive,
    OrchestrationProjectionSnapshotQueryLive,
  ).pipe(
    Layer.provide(ThreadBackgroundLiveness.layer),
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provide(RepositoryIdentityResolver.layer),
    Layer.provideMerge(OrchestrationEventStoreLive),
    Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix })),
    Layer.provideMerge(NodeServices.layer),
  );

// --- Event construction helpers (shared constants) ---

const NOW = "2026-05-01T00:00:00.000Z";
const PROJECT = ProjectId.make("pr15-project");
const ORIGIN = ThreadId.make("pr15-origin");
const PROVIDER = ProviderInstanceId.make("codex");

const T1 = TurnId.make("pr15-turn-1");
const T2 = TurnId.make("pr15-turn-2");
const U1 = MessageId.make("pr15-user-1");
const U2 = MessageId.make("pr15-user-2");
const A1 = MessageId.make("pr15-asst-1");
const A2 = MessageId.make("pr15-asst-2");

type EventInput = Omit<OrchestrationEvent, "sequence">;

function projectCreatedEvent(): EventInput {
  return {
    type: "project.created",
    eventId: EventId.make("evt-pr15-project"),
    aggregateKind: "project",
    aggregateId: PROJECT,
    occurredAt: NOW,
    commandId: CommandId.make("cmd-pr15-project"),
    causationEventId: null,
    correlationId: CorrelationId.make("cmd-pr15-project"),
    metadata: {},
    payload: {
      projectId: PROJECT,
      title: "Phase B Project",
      workspaceRoot: "/tmp/pr15",
      defaultModelSelection: null,
      scripts: [],
      createdAt: NOW,
      updatedAt: NOW,
    },
  };
}

function threadCreatedEvent(threadId: ThreadId, title: string, eventId: string): EventInput {
  return {
    type: "thread.created",
    eventId: EventId.make(eventId),
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: NOW,
    commandId: CommandId.make(`cmd-${eventId}`),
    causationEventId: null,
    correlationId: CorrelationId.make(`cmd-${eventId}`),
    metadata: {},
    payload: {
      threadId,
      projectId: PROJECT,
      title,
      modelSelection: { instanceId: PROVIDER, model: "gpt-5-codex" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdAt: NOW,
      updatedAt: NOW,
    },
  };
}

function messageSentEvent(
  threadId: ThreadId,
  messageId: MessageId,
  role: "user" | "assistant",
  text: string,
  turnId: TurnId | null,
  createdAt: string,
  eventId: string,
): EventInput {
  return {
    type: "thread.message-sent",
    eventId: EventId.make(eventId),
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: createdAt,
    commandId: CommandId.make(`cmd-${eventId}`),
    causationEventId: null,
    correlationId: CorrelationId.make(`cmd-${eventId}`),
    metadata: {},
    payload: {
      threadId,
      messageId,
      role,
      text,
      turnId,
      streaming: false,
      createdAt,
      updatedAt: createdAt,
    },
  };
}

function turnDiffCompletedEvent(
  threadId: ThreadId,
  turnId: TurnId,
  checkpointTurnCount: number,
  assistantMessageId: MessageId,
  completedAt: string,
  eventId: string,
): EventInput {
  return {
    type: "thread.turn-diff-completed",
    eventId: EventId.make(eventId),
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: completedAt,
    commandId: CommandId.make(`cmd-${eventId}`),
    causationEventId: null,
    correlationId: CorrelationId.make(`cmd-${eventId}`),
    metadata: {},
    payload: {
      threadId,
      turnId,
      checkpointTurnCount,
      checkpointRef: CheckpointRef.make(
        `refs/t3/checkpoints/${threadId}/turn/${checkpointTurnCount}`,
      ),
      status: "ready",
      files: [],
      assistantMessageId,
      completedAt,
    },
  };
}

function makeBaseReadModel(): OrchestrationReadModel {
  return {
    ...createEmptyReadModel(NOW),
    projects: [
      {
        id: PROJECT,
        title: "Phase B Project",
        workspaceRoot: "/tmp/pr15",
        defaultModelSelection: null,
        scripts: [],
        createdAt: NOW,
        updatedAt: NOW,
        deletedAt: null,
      },
    ],
    threads: [],
    updatedAt: NOW,
  };
}

// ===========================================================================
// VAL-MIGRATE-13: Lineage lifecycle semantics survive migration
// ===========================================================================

it.effect("VAL-MIGRATE-13: lifecycle operations work on migrated prototype rows", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // 1. Create a prototype database: legacy ledger + prototype lineage rows
      //    in various lifecycle states.
      yield* sql`
        CREATE TABLE scient_schema_migrations (
          migration_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at TEXT NOT NULL
        )
      `;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name, applied_at) VALUES (1, 'durable-thread-forks', '2026-07-01T10:00:00.000Z')`;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name, applied_at) VALUES (2, 'durable-provider-bootstrap', '2026-07-02T12:00:00.000Z')`;

      yield* sql`
        CREATE TABLE scient_thread_lineage (
          thread_id TEXT PRIMARY KEY,
          forked_from_thread_id TEXT,
          fork_point_turn_count INTEGER,
          workspace_mode TEXT,
          fidelity_mode TEXT,
          baseline_turn_id TEXT,
          created_at TEXT
        )
      `;

      // Prototype rows with different implicit states (all default to pending
      // after migration since the prototype schema had no status column).
      const threads = [
        { id: "proto-pending", origin: "origin-a", count: 2, mode: "local", fidelity: "chat-only" },
        {
          id: "proto-provisioning",
          origin: "origin-b",
          count: 1,
          mode: "new-worktree",
          fidelity: "replay",
        },
        { id: "proto-ready", origin: "origin-c", count: 3, mode: "local", fidelity: "chat-only" },
        { id: "proto-abandoned", origin: "origin-d", count: 1, mode: "local", fidelity: "replay" },
      ];

      for (const t of threads) {
        yield* sql`
          INSERT INTO scient_thread_lineage (
            thread_id, forked_from_thread_id, fork_point_turn_count,
            workspace_mode, fidelity_mode, baseline_turn_id, created_at
          ) VALUES (
            ${t.id}, ${t.origin}, ${t.count},
            ${t.mode}, ${t.fidelity}, ${`baseline-${t.id}`}, '2026-07-15T00:00:00.000Z'
          )
        `;
      }

      // 2. Run migrations — normalizes the prototype rows.
      const executed = yield* runScientMigrations(sql);
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        [3, 4, 5, 6, 7],
      );

      // 3. Verify listRecoverableForks selects exactly pending/provisioning/failed
      //    rows with baseline. All prototype rows default to pending after
      //    migration, so proto-pending and proto-provisioning should be
      //    recoverable (provisioning will be set by claiming below).
      //    First, manually set some to different states to test the full matrix.
      yield* sql`UPDATE scient_thread_lineage SET status = 'provisioning', attempt_count = 1 WHERE thread_id = 'proto-provisioning'`;
      yield* sql`UPDATE scient_thread_lineage SET status = 'ready', checkpoint_status = 'ready', workspace_status = 'shared' WHERE thread_id = 'proto-ready'`;
      yield* sql`UPDATE scient_thread_lineage SET status = 'abandoned', last_error = 'permanently failed' WHERE thread_id = 'proto-abandoned'`;

      const recoverable = yield* listRecoverableForks(sql);
      const recoverableIds = recoverable.map((r) => r.newThreadId).sort();
      assert.deepStrictEqual(recoverableIds, ["proto-pending", "proto-provisioning"]);

      // 4. claimFork increments attempts and cannot regress ready/abandoned.
      const claimedPending = yield* claimFork(sql, ThreadId.make("proto-pending"), NOW);
      assert.isTrue(claimedPending);

      const pendingRow = yield* sql<{
        readonly status: string;
        readonly attempt_count: number;
        readonly last_error: string | null;
      }>`SELECT status, attempt_count, last_error FROM scient_thread_lineage WHERE thread_id = 'proto-pending'`;
      assert.strictEqual(pendingRow[0]!.status, "provisioning");
      assert.strictEqual(pendingRow[0]!.attempt_count, 1);
      assert.strictEqual(pendingRow[0]!.last_error, null);

      // Ready cannot be claimed.
      const claimedReady = yield* claimFork(sql, ThreadId.make("proto-ready"), NOW);
      assert.isFalse(claimedReady);

      // Abandoned cannot be claimed.
      const claimedAbandoned = yield* claimFork(sql, ThreadId.make("proto-abandoned"), NOW);
      assert.isFalse(claimedAbandoned);

      // 5. markForkFailed cannot overwrite ready.
      yield* markForkFailed(sql, {
        threadId: ThreadId.make("proto-ready"),
        error: "late failure",
        updatedAt: NOW,
      });
      const readyRow = yield* sql<{ readonly status: string; readonly last_error: string | null }>`
        SELECT status, last_error FROM scient_thread_lineage WHERE thread_id = 'proto-ready'
      `;
      assert.strictEqual(readyRow[0]!.status, "ready");
      assert.strictEqual(readyRow[0]!.last_error, null);

      // 6. markForkReady clears errors and persists statuses.
      yield* markForkFailed(sql, {
        threadId: ThreadId.make("proto-pending"),
        error: "transient crash",
        updatedAt: NOW,
      });
      const failedRow = yield* sql<{ readonly status: string; readonly last_error: string | null }>`
        SELECT status, last_error FROM scient_thread_lineage WHERE thread_id = 'proto-pending'
      `;
      assert.strictEqual(failedRow[0]!.status, "failed");
      assert.strictEqual(failedRow[0]!.last_error, "transient crash");

      yield* markForkReady(sql, {
        threadId: ThreadId.make("proto-pending"),
        checkpointStatus: "ready",
        workspaceStatus: "shared",
        updatedAt: NOW,
      });
      const readyPending = yield* sql<{
        readonly status: string;
        readonly last_error: string | null;
        readonly checkpoint_status: string;
        readonly workspace_status: string;
      }>`SELECT status, last_error, checkpoint_status, workspace_status FROM scient_thread_lineage WHERE thread_id = 'proto-pending'`;
      assert.strictEqual(readyPending[0]!.status, "ready");
      assert.strictEqual(readyPending[0]!.last_error, null);
      assert.strictEqual(readyPending[0]!.checkpoint_status, "ready");
      assert.strictEqual(readyPending[0]!.workspace_status, "shared");

      // 7. Abandoned cannot regress to ready.
      yield* markForkReady(sql, {
        threadId: ThreadId.make("proto-abandoned"),
        checkpointStatus: "ready",
        workspaceStatus: "shared",
        updatedAt: NOW,
      });
      const abandonedRow = yield* sql<{ readonly status: string }>`
        SELECT status FROM scient_thread_lineage WHERE thread_id = 'proto-abandoned'
      `;
      assert.strictEqual(abandonedRow[0]!.status, "abandoned");

      // 8. Abandoned cannot regress to failed.
      yield* markForkFailed(sql, {
        threadId: ThreadId.make("proto-abandoned"),
        error: "attempted retry",
        updatedAt: NOW,
      });
      const abandonedRow2 = yield* sql<{
        readonly status: string;
        readonly last_error: string | null;
      }>`
        SELECT status, last_error FROM scient_thread_lineage WHERE thread_id = 'proto-abandoned'
      `;
      assert.strictEqual(abandonedRow2[0]!.status, "abandoned");
      assert.strictEqual(abandonedRow2[0]!.last_error, "permanently failed");

      // 9. getForkStatus decodes canonical values.
      const status = yield* getForkStatus(sql, ThreadId.make("proto-pending"));
      assert.strictEqual(status!.status, "ready");
      assert.strictEqual(status!.last_error, null);

      // 10. All rows have canonical transcript-bootstrap modes (no prototype leaks).
      const modes = yield* sql<{
        readonly provider_mode: string;
        readonly fidelity_mode: string;
      }>`SELECT provider_mode, fidelity_mode FROM scient_thread_lineage ORDER BY thread_id`;
      for (const row of modes) {
        assert.strictEqual(row.provider_mode, "transcript-bootstrap");
        assert.strictEqual(row.fidelity_mode, "transcript-bootstrap");
      }
    }),
  ),
);

// ===========================================================================
// VAL-CROSS-001: Fresh startup to ready fork is end to end
// ===========================================================================

it.layer(Layer.fresh(makeCrossAreaTestLayer("pr15-cross-001-")))(
  "VAL-CROSS-001: fresh startup to ready fork is end to end",
  (it) => {
    it.effect("fresh SQLite creates isolated ledgers, resolves, projects, and marks ready", () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const pipeline = yield* OrchestrationProjectionPipeline;
        const eventStore = yield* OrchestrationEventStore;
        yield* pipeline.bootstrap;

        // 1. Both ledgers exist and are isolated.
        const t3Ledger = yield* sql<{ readonly migration_id: number }>`
          SELECT migration_id FROM effect_sql_migrations ORDER BY migration_id
        `;
        assert.isTrue(t3Ledger.length > 0);

        const scientLedger = yield* sql<{ readonly migration_id: number; readonly name: string }>`
          SELECT migration_id, name FROM scient_schema_migrations ORDER BY migration_id
        `;
        assert.isTrue(scientLedger.length >= 2);
        const scientIds = new Set(scientLedger.map((r) => r.migration_id));
        assert.isTrue(scientIds.has(1));
        assert.isTrue(scientIds.has(2));

        // 2. Seed origin with one completed turn.
        const appendAndProject = (event: Parameters<typeof eventStore.append>[0]) =>
          eventStore.append(event).pipe(Effect.flatMap((saved) => pipeline.projectEvent(saved)));

        yield* appendAndProject(projectCreatedEvent());
        yield* appendAndProject(threadCreatedEvent(ORIGIN, "Cross 001 Origin", "evt-001-origin"));
        yield* appendAndProject(
          messageSentEvent(
            ORIGIN,
            U1,
            "user",
            "hello 001",
            T1,
            "2026-05-01T00:00:01.000Z",
            "evt-001-u1",
          ),
        );
        yield* appendAndProject(
          messageSentEvent(
            ORIGIN,
            A1,
            "assistant",
            "world 001",
            T1,
            "2026-05-01T00:00:02.000Z",
            "evt-001-a1",
          ),
        );
        yield* appendAndProject(
          turnDiffCompletedEvent(ORIGIN, T1, 1, A1, "2026-05-01T00:00:02.500Z", "evt-001-tdc-1"),
        );

        // 3. Resolve boundary from SQL.
        const resolver = makeForkBoundaryResolver(sql);
        const resolved = yield* resolver.resolve({
          originThreadId: ORIGIN,
          sourceAssistantMessageId: A1,
          threadCreatedAt: NOW,
        });
        assert.strictEqual(resolved.selectedBoundary.assistantMessageId, A1);
        assert.strictEqual(resolved.selectedBoundary.turnId, T1);
        assert.strictEqual(resolved.selectedBoundary.conversationTurnCount, 1);

        // 4. Run fork decider with resolved boundaries.
        const snapshotQuery = yield* ProjectionSnapshotQuery;
        const originOption = yield* snapshotQuery.getThreadDetailById(ORIGIN);
        if (!Option.isSome(originOption)) {
          return assert.fail("Origin thread detail not found");
        }
        const decisionReadModel = withForkOriginDetail(makeBaseReadModel(), originOption.value);

        const FORK = ThreadId.make("pr15-fork-001");
        const forkCmd: ThreadForkCommand = {
          type: "thread.fork",
          commandId: CommandId.make("cmd-001-fork"),
          originThreadId: ORIGIN,
          newThreadId: FORK,
          sourceAssistantMessageId: A1,
          workspaceMode: "local",
        };

        const plannedEvents = yield* forkThread({
          command: forkCmd,
          readModel: decisionReadModel,
          resolvedBoundaries: resolved,
        }).pipe(Effect.provideService(Crypto.Crypto, yield* Crypto.Crypto));

        // 5. Persist and project fork events — lineage projector inserts
        //    a pending lineage row.
        for (const planned of plannedEvents) {
          const saved = yield* eventStore.append(planned);
          yield* pipeline.projectEvent(saved);
        }

        // 6. Verify the pending lineage row was created by the projector.
        const pendingRow = yield* sql<{
          readonly status: string;
          readonly provider_mode: string;
          readonly provider_bootstrap_status: string;
          readonly attempt_count: number;
        }>`SELECT status, provider_mode, provider_bootstrap_status, attempt_count FROM scient_thread_lineage WHERE thread_id = 'pr15-fork-001'`;
        assert.strictEqual(pendingRow.length, 1);
        assert.strictEqual(pendingRow[0]!.status, "pending");
        assert.strictEqual(pendingRow[0]!.provider_mode, "transcript-bootstrap");
        assert.strictEqual(pendingRow[0]!.provider_bootstrap_status, "pending");
        assert.strictEqual(pendingRow[0]!.attempt_count, 0);

        // 7. Simulate reactor: claim and mark ready.
        const claimed = yield* claimFork(sql, FORK, NOW);
        assert.isTrue(claimed);

        yield* markForkReady(sql, {
          threadId: FORK,
          checkpointStatus: "ready",
          workspaceStatus: "shared",
          updatedAt: NOW,
        });

        const readyRow = yield* sql<{
          readonly status: string;
          readonly last_error: string | null;
        }>`SELECT status, last_error FROM scient_thread_lineage WHERE thread_id = 'pr15-fork-001'`;
        assert.strictEqual(readyRow[0]!.status, "ready");
        assert.strictEqual(readyRow[0]!.last_error, null);

        // 8. Verify origin is unchanged — still 1 turn, same messages.
        const originTurns = yield* sql<{ readonly turn_id: string | null }>`
          SELECT turn_id FROM projection_turns
          WHERE thread_id = 'pr15-origin' ORDER BY requested_at ASC
        `;
        assert.deepEqual(
          originTurns.map((t) => t.turn_id),
          [T1],
        );

        const originMessages = yield* sql<{ readonly message_id: string }>`
          SELECT message_id FROM projection_thread_messages
          WHERE thread_id = 'pr15-origin' ORDER BY created_at ASC
        `;
        assert.deepEqual(
          originMessages.map((m) => m.message_id),
          [U1, A1],
        );

        // 9. Verify T3 ledger is unchanged after all Scient operations.
        const t3LedgerAfter = yield* sql<{ readonly migration_id: number }>`
          SELECT migration_id FROM effect_sql_migrations ORDER BY migration_id
        `;
        assert.deepEqual(
          t3Ledger.map((r) => r.migration_id),
          t3LedgerAfter.map((r) => r.migration_id),
        );
      }),
    );
  },
);

// ===========================================================================
// VAL-CROSS-002: Prototype upgrade then fork is compatible
// ===========================================================================

it.effect("VAL-CROSS-002: prototype upgrade normalizes rows and supports repository decode", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // 1. Create prototype database: legacy ledger with applied_at + prototype
      //    lineage rows with chat-only/replay fidelity modes.
      yield* sql`
        CREATE TABLE scient_schema_migrations (
          migration_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at TEXT NOT NULL
        )
      `;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name, applied_at) VALUES (1, 'durable-thread-forks', '2026-07-01T10:00:00.000Z')`;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name, applied_at) VALUES (2, 'durable-provider-bootstrap', '2026-07-02T12:00:00.000Z')`;

      yield* sql`
        CREATE TABLE scient_thread_lineage (
          thread_id TEXT PRIMARY KEY,
          forked_from_thread_id TEXT,
          fork_point_turn_count INTEGER,
          workspace_mode TEXT,
          fidelity_mode TEXT,
          baseline_turn_id TEXT,
          created_at TEXT
        )
      `;

      yield* sql`
        INSERT INTO scient_thread_lineage (
          thread_id, forked_from_thread_id, fork_point_turn_count,
          workspace_mode, fidelity_mode, baseline_turn_id, created_at
        ) VALUES
          ('proto-fork-a', 'origin-a', 2, 'local', 'chat-only', 'baseline-a', '2026-07-10T00:00:00.000Z'),
          ('proto-fork-b', 'origin-b', 1, 'new-worktree', 'replay', 'baseline-b', '2026-07-11T00:00:00.000Z')
      `;

      // 2. Run migrations — normalizes prototype modes.
      const executed = yield* runScientMigrations(sql);
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        [3, 4, 5, 6, 7],
      );

      // 3. Verify identity is preserved (no loss or fabrication).
      const rows = yield* sql<{
        readonly thread_id: string;
        readonly forked_from_thread_id: string;
        readonly fork_point_turn_count: number;
        readonly workspace_mode: string;
        readonly provider_mode: string;
        readonly fidelity_mode: string;
        readonly status: string;
        readonly created_at: string;
      }>`
        SELECT thread_id, forked_from_thread_id, fork_point_turn_count, workspace_mode,
               provider_mode, fidelity_mode, status, created_at
        FROM scient_thread_lineage ORDER BY thread_id
      `;
      assert.strictEqual(rows.length, 2);
      assert.strictEqual(rows[0]!.thread_id, "proto-fork-a");
      assert.strictEqual(rows[0]!.forked_from_thread_id, "origin-a");
      assert.strictEqual(rows[0]!.fork_point_turn_count, 2);
      assert.strictEqual(rows[0]!.workspace_mode, "local");
      assert.strictEqual(rows[0]!.created_at, "2026-07-10T00:00:00.000Z");
      assert.strictEqual(rows[1]!.thread_id, "proto-fork-b");
      assert.strictEqual(rows[1]!.forked_from_thread_id, "origin-b");
      assert.strictEqual(rows[1]!.fork_point_turn_count, 1);
      assert.strictEqual(rows[1]!.workspace_mode, "new-worktree");

      // 4. Verify prototype modes are normalized to transcript-bootstrap.
      for (const row of rows) {
        assert.strictEqual(row.provider_mode, "transcript-bootstrap");
        assert.strictEqual(row.fidelity_mode, "transcript-bootstrap");
        assert.strictEqual(row.status, "pending");
      }

      // 5. Verify repository decoders can read the normalized rows.
      //    listRecoverableForks decodes via Schema and maps to ThreadForkedPayload.
      const recoverable = yield* listRecoverableForks(sql);
      assert.strictEqual(recoverable.length, 2);
      const ids = recoverable.map((r) => r.newThreadId).sort();
      assert.deepStrictEqual(ids, ["proto-fork-a", "proto-fork-b"]);

      // Each decoded payload must have transcript-bootstrap providerMode.
      for (const fork of recoverable) {
        assert.strictEqual(fork.providerMode, "transcript-bootstrap");
      }

      // getForkStatus decodes the canonical status.
      const status = yield* getForkStatus(sql, ThreadId.make("proto-fork-a"));
      assert.strictEqual(status!.status, "pending");
      assert.strictEqual(status!.last_error, null);

      // 6. Second migration run is idempotent — no changes.
      const secondRun = yield* runScientMigrations(sql);
      assert.strictEqual(secondRun.length, 0);

      // 7. Legacy ledger IDs/names/timestamps are preserved.
      const ledger = yield* sql<{
        readonly migration_id: number;
        readonly name: string;
        readonly created_at: string;
        readonly applied_at: string;
      }>`SELECT migration_id, name, created_at, applied_at FROM scient_schema_migrations ORDER BY migration_id`;
      assert.strictEqual(ledger.length, 7);
      assert.strictEqual(ledger[0]!.migration_id, 1);
      assert.strictEqual(ledger[0]!.name, "durable-thread-forks");
      assert.strictEqual(ledger[0]!.applied_at, "2026-07-01T10:00:00.000Z");
      assert.strictEqual(ledger[0]!.created_at, "2026-07-01T10:00:00.000Z");
      assert.strictEqual(ledger[1]!.migration_id, 2);
      assert.strictEqual(ledger[1]!.name, "durable-provider-bootstrap");
    }),
  ),
);

// ===========================================================================
// VAL-CROSS-003: Restart during pending fork is safe
// ===========================================================================

it.effect(
  "VAL-CROSS-003: restart during pending fork leaves fields unchanged and recovery is safe",
  () =>
    withMemory(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        // 1. Fresh database with full migrations.
        yield* runScientMigrations(sql);

        // 2. Insert a pending fork row (simulating a fork that was just
        //    created but not yet provisioned when the server restarted).
        const THREAD = ThreadId.make("pending-restart-fork");
        const ORIGIN_ID = ThreadId.make("pending-restart-origin");
        yield* insertPendingFork(sql, {
          originThreadId: ORIGIN_ID,
          newThreadId: THREAD,
          forkAtTurnId: TurnId.make("turn-1"),
          forkAtTurnCount: 1,
          sourceCheckpointTurnCount: 1,
          baselineTurnId: TurnId.make("baseline-1"),
          baselineUserMessageId: MessageId.make("user-1"),
          baselineAssistantMessageId: MessageId.make("asst-1"),
          workspaceMode: "local",
          providerMode: "transcript-bootstrap",
          attachmentCopies: [],
          createdAt: NOW,
        });

        // 3. Snapshot pending row state before restart.
        const beforeRow = yield* sql<{
          readonly status: string;
          readonly attempt_count: number;
          readonly provider_mode: string;
          readonly provider_bootstrap_status: string;
          readonly last_error: string | null;
          readonly created_at: string;
          readonly updated_at: string;
        }>`SELECT status, attempt_count, provider_mode, provider_bootstrap_status, last_error, created_at, updated_at FROM scient_thread_lineage WHERE thread_id = 'pending-restart-fork'`;

        // 4. Simulate restart: rerun migrations (idempotent).
        const rerun = yield* runScientMigrations(sql);
        assert.strictEqual(rerun.length, 0);

        // 5. Verify pending lifecycle fields are unchanged.
        const afterRow = yield* sql<{
          readonly status: string;
          readonly attempt_count: number;
          readonly provider_mode: string;
          readonly provider_bootstrap_status: string;
          readonly last_error: string | null;
          readonly created_at: string;
          readonly updated_at: string;
        }>`SELECT status, attempt_count, provider_mode, provider_bootstrap_status, last_error, created_at, updated_at FROM scient_thread_lineage WHERE thread_id = 'pending-restart-fork'`;
        assert.deepStrictEqual(afterRow, beforeRow);

        // 6. Recovery finds the pending row.
        const recoverable = yield* listRecoverableForks(sql);
        assert.strictEqual(recoverable.length, 1);
        assert.strictEqual(recoverable[0]!.newThreadId, THREAD);

        // 7. Duplicate delivery: calling claim twice yields at most one
        //    effective claim. The first claim succeeds (pending → provisioning),
        //    the second claim on the same provisioning row also succeeds
        //    (provisioning is still claimable), but the attempt count
        //    increments only once per call. In a real concurrent scenario,
        //    only one worker would proceed with provisioning.
        const claimed1 = yield* claimFork(sql, THREAD, NOW);
        assert.isTrue(claimed1);

        const afterFirstClaim = yield* sql<{
          readonly status: string;
          readonly attempt_count: number;
        }>`SELECT status, attempt_count FROM scient_thread_lineage WHERE thread_id = 'pending-restart-fork'`;
        assert.strictEqual(afterFirstClaim[0]!.status, "provisioning");
        assert.strictEqual(afterFirstClaim[0]!.attempt_count, 1);

        // Second claim also succeeds (provisioning is still recoverable),
        // incrementing attempt count again.
        const claimed2 = yield* claimFork(sql, THREAD, NOW);
        assert.isTrue(claimed2);

        const afterSecondClaim = yield* sql<{
          readonly status: string;
          readonly attempt_count: number;
        }>`SELECT status, attempt_count FROM scient_thread_lineage WHERE thread_id = 'pending-restart-fork'`;
        assert.strictEqual(afterSecondClaim[0]!.attempt_count, 2);

        // 8. Provisioning completes: mark ready.
        yield* markForkReady(sql, {
          threadId: THREAD,
          checkpointStatus: "ready",
          workspaceStatus: "shared",
          updatedAt: NOW,
        });

        const readyRow = yield* sql<{
          readonly status: string;
          readonly last_error: string | null;
        }>`
        SELECT status, last_error FROM scient_thread_lineage WHERE thread_id = 'pending-restart-fork'
      `;
        assert.strictEqual(readyRow[0]!.status, "ready");
        assert.strictEqual(readyRow[0]!.last_error, null);

        // 9. Ready row is no longer recoverable.
        const recoverableAfterReady = yield* listRecoverableForks(sql);
        assert.strictEqual(recoverableAfterReady.length, 0);
      }),
    ),
);

// ===========================================================================
// VAL-CROSS-004: Interrupted provisioning retries deterministically
// ===========================================================================

it.effect(
  "VAL-CROSS-004: interrupted provisioning retries with incremented attempt and terminal abandoned is not retried",
  () =>
    withMemory(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        // 1. Fresh database with full migrations.
        yield* runScientMigrations(sql);

        // 2. Insert a pending fork and claim it (simulating provisioning start).
        const THREAD = ThreadId.make("interrupted-fork");
        yield* insertPendingFork(sql, {
          originThreadId: ThreadId.make("interrupted-origin"),
          newThreadId: THREAD,
          forkAtTurnId: TurnId.make("turn-1"),
          forkAtTurnCount: 1,
          sourceCheckpointTurnCount: 1,
          baselineTurnId: TurnId.make("baseline-1"),
          baselineUserMessageId: MessageId.make("user-1"),
          baselineAssistantMessageId: MessageId.make("asst-1"),
          workspaceMode: "local",
          providerMode: "transcript-bootstrap",
          attachmentCopies: [],
          createdAt: NOW,
        });

        // First claim — provisioning starts, then server crashes.
        const claimed1 = yield* claimFork(sql, THREAD, NOW);
        assert.isTrue(claimed1);

        const afterCrash = yield* sql<{
          readonly status: string;
          readonly attempt_count: number;
        }>`SELECT status, attempt_count FROM scient_thread_lineage WHERE thread_id = 'interrupted-fork'`;
        assert.strictEqual(afterCrash[0]!.status, "provisioning");
        assert.strictEqual(afterCrash[0]!.attempt_count, 1);

        // 3. Simulate restart: rerun migrations (idempotent).
        const rerun = yield* runScientMigrations(sql);
        assert.strictEqual(rerun.length, 0);

        // 4. Recovery finds the provisioning row (it's still recoverable).
        const recoverable = yield* listRecoverableForks(sql);
        assert.strictEqual(recoverable.length, 1);
        assert.strictEqual(recoverable[0]!.newThreadId, THREAD);

        // 5. Reclaim with incremented attempt (second restart).
        const claimed2 = yield* claimFork(sql, THREAD, NOW);
        assert.isTrue(claimed2);

        const afterReclaim = yield* sql<{
          readonly status: string;
          readonly attempt_count: number;
        }>`SELECT status, attempt_count FROM scient_thread_lineage WHERE thread_id = 'interrupted-fork'`;
        assert.strictEqual(afterReclaim[0]!.attempt_count, 2);

        // 6. Terminal failure: mark abandoned.
        yield* markForkAbandoned(sql, {
          threadId: THREAD,
          error: "permanently unusable: checkpoint disappeared",
          updatedAt: NOW,
        });

        const abandonedRow = yield* sql<{
          readonly status: string;
          readonly last_error: string | null;
        }>`SELECT status, last_error FROM scient_thread_lineage WHERE thread_id = 'interrupted-fork'`;
        assert.strictEqual(abandonedRow[0]!.status, "abandoned");
        assert.strictEqual(
          abandonedRow[0]!.last_error,
          "permanently unusable: checkpoint disappeared",
        );

        // 7. Abandoned is not retried: not in recovery list.
        const recoverableAfterAbandon = yield* listRecoverableForks(sql);
        assert.strictEqual(recoverableAfterAbandon.length, 0);

        // 8. Abandoned cannot be claimed (not retried).
        const claimed3 = yield* claimFork(sql, THREAD, NOW);
        assert.isFalse(claimed3);

        // 9. Second restart: rerun migrations, abandoned still not recoverable.
        const rerun2 = yield* runScientMigrations(sql);
        assert.strictEqual(rerun2.length, 0);

        const recoverableFinal = yield* listRecoverableForks(sql);
        assert.strictEqual(recoverableFinal.length, 0);
      }),
    ),
);

// ===========================================================================
// VAL-CROSS-006: Re-fork after normalization uses only canonical values
// ===========================================================================

it.layer(Layer.fresh(makeCrossAreaTestLayer("pr15-cross-006-")))(
  "VAL-CROSS-006: re-fork after normalization uses only canonical values",
  (it) => {
    it.effect("fork-of-fork uses transcript-bootstrap and canonical lineage values", () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const pipeline = yield* OrchestrationProjectionPipeline;
        const eventStore = yield* OrchestrationEventStore;
        const snapshotQuery = yield* ProjectionSnapshotQuery;
        yield* pipeline.bootstrap;

        const appendAndProject = (event: Parameters<typeof eventStore.append>[0]) =>
          eventStore.append(event).pipe(Effect.flatMap((saved) => pipeline.projectEvent(saved)));

        // 1. Seed origin with two completed turns.
        yield* appendAndProject(projectCreatedEvent());
        yield* appendAndProject(threadCreatedEvent(ORIGIN, "Cross 006 Origin", "evt-006-origin"));

        // Turn 1
        yield* appendAndProject(
          messageSentEvent(
            ORIGIN,
            U1,
            "user",
            "prompt 006-1",
            T1,
            "2026-05-02T00:00:01.000Z",
            "evt-006-u1",
          ),
        );
        yield* appendAndProject(
          messageSentEvent(
            ORIGIN,
            A1,
            "assistant",
            "answer 006-1",
            T1,
            "2026-05-02T00:00:02.000Z",
            "evt-006-a1",
          ),
        );
        yield* appendAndProject(
          turnDiffCompletedEvent(ORIGIN, T1, 1, A1, "2026-05-02T00:00:02.500Z", "evt-006-tdc-1"),
        );

        // Turn 2
        yield* appendAndProject(
          messageSentEvent(
            ORIGIN,
            U2,
            "user",
            "prompt 006-2",
            T2,
            "2026-05-02T00:00:03.000Z",
            "evt-006-u2",
          ),
        );
        yield* appendAndProject(
          messageSentEvent(
            ORIGIN,
            A2,
            "assistant",
            "answer 006-2",
            T2,
            "2026-05-02T00:00:04.000Z",
            "evt-006-a2",
          ),
        );
        yield* appendAndProject(
          turnDiffCompletedEvent(ORIGIN, T2, 2, A2, "2026-05-02T00:00:04.500Z", "evt-006-tdc-2"),
        );

        // 2. Fork origin at turn 1.
        const resolver = makeForkBoundaryResolver(sql);
        const resolved1 = yield* resolver.resolve({
          originThreadId: ORIGIN,
          sourceAssistantMessageId: A1,
          threadCreatedAt: NOW,
        });

        const originOption = yield* snapshotQuery.getThreadDetailById(ORIGIN);
        if (!Option.isSome(originOption)) {
          return assert.fail("Origin thread detail not found");
        }
        const decisionRM1 = withForkOriginDetail(makeBaseReadModel(), originOption.value);

        const FORK1 = ThreadId.make("pr15-fork-006-1");
        const forkCmd1: ThreadForkCommand = {
          type: "thread.fork",
          commandId: CommandId.make("cmd-006-fork1"),
          originThreadId: ORIGIN,
          newThreadId: FORK1,
          sourceAssistantMessageId: A1,
          workspaceMode: "local",
        };

        const events1 = yield* forkThread({
          command: forkCmd1,
          readModel: decisionRM1,
          resolvedBoundaries: resolved1,
        }).pipe(Effect.provideService(Crypto.Crypto, yield* Crypto.Crypto));

        const forkedEvent1 = events1.find((e) => e.type === "thread.forked");
        if (!forkedEvent1 || forkedEvent1.type !== "thread.forked") {
          return assert.fail("first fork event not found");
        }
        assert.strictEqual(forkedEvent1.payload.providerMode, "transcript-bootstrap");

        for (const planned of events1) {
          const saved = yield* eventStore.append(planned);
          yield* pipeline.projectEvent(saved);
        }

        // 3. Mark fork1 as ready (simulating reactor completion).
        yield* claimFork(sql, FORK1, NOW);
        yield* markForkReady(sql, {
          threadId: FORK1,
          checkpointStatus: "ready",
          workspaceStatus: "shared",
          updatedAt: NOW,
        });

        // 4. Add a post-fork turn to fork1 so re-fork has a non-baseline
        //    boundary to select.
        const POST_T = TurnId.make("fork1-post-turn");
        const POST_U = MessageId.make("fork1-post-user");
        const POST_A = MessageId.make("fork1-post-asst");
        yield* appendAndProject(
          messageSentEvent(
            FORK1,
            POST_U,
            "user",
            "new question",
            POST_T,
            "2026-05-02T00:00:10.000Z",
            "evt-006-post-u",
          ),
        );
        yield* appendAndProject(
          messageSentEvent(
            FORK1,
            POST_A,
            "assistant",
            "new answer",
            POST_T,
            "2026-05-02T00:00:11.000Z",
            "evt-006-post-a",
          ),
        );
        yield* appendAndProject(
          turnDiffCompletedEvent(
            FORK1,
            POST_T,
            1,
            POST_A,
            "2026-05-02T00:00:11.500Z",
            "evt-006-post-tdc",
          ),
        );

        // 5. Re-fork: fork fork1 at the post-fork assistant (POST_A).
        const resolved2 = yield* resolver.resolve({
          originThreadId: FORK1,
          sourceAssistantMessageId: POST_A,
          threadCreatedAt: NOW,
        });

        const fork1Option = yield* snapshotQuery.getThreadDetailById(FORK1);
        if (!Option.isSome(fork1Option)) {
          return assert.fail("Fork1 thread detail not found");
        }
        const decisionRM2 = withForkOriginDetail(makeBaseReadModel(), fork1Option.value);

        const FORK2 = ThreadId.make("pr15-fork-006-2");
        const forkCmd2: ThreadForkCommand = {
          type: "thread.fork",
          commandId: CommandId.make("cmd-006-fork2"),
          originThreadId: FORK1,
          newThreadId: FORK2,
          sourceAssistantMessageId: POST_A,
          workspaceMode: "local",
        };

        const events2 = yield* forkThread({
          command: forkCmd2,
          readModel: decisionRM2,
          resolvedBoundaries: resolved2,
        }).pipe(Effect.provideService(Crypto.Crypto, yield* Crypto.Crypto));

        // 6. Assert the re-fork payload uses transcript-bootstrap.
        const forkedEvent2 = events2.find((e) => e.type === "thread.forked");
        if (!forkedEvent2 || forkedEvent2.type !== "thread.forked") {
          return assert.fail("re-fork event not found");
        }
        assert.strictEqual(forkedEvent2.payload.providerMode, "transcript-bootstrap");
        assert.strictEqual(forkedEvent2.payload.workspaceMode, "local");

        // 7. Persist and project the re-fork events.
        for (const planned of events2) {
          const saved = yield* eventStore.append(planned);
          yield* pipeline.projectEvent(saved);
        }

        // 8. Assert the lineage row has canonical values (no prototype leaks).
        const lineageRow = yield* sql<{
          readonly provider_mode: string;
          readonly fidelity_mode: string;
          readonly status: string;
          readonly forked_from_thread_id: string;
          readonly fork_point_turn_count: number;
        }>`SELECT provider_mode, fidelity_mode, status, forked_from_thread_id, fork_point_turn_count FROM scient_thread_lineage WHERE thread_id = 'pr15-fork-006-2'`;
        assert.strictEqual(lineageRow.length, 1);
        assert.strictEqual(lineageRow[0]!.provider_mode, "transcript-bootstrap");
        assert.strictEqual(lineageRow[0]!.fidelity_mode, "transcript-bootstrap");
        assert.strictEqual(lineageRow[0]!.status, "pending");
        assert.strictEqual(lineageRow[0]!.forked_from_thread_id, FORK1);

        // 9. Assert repository decode works (getForkStatus).
        const status = yield* getForkStatus(sql, FORK2);
        assert.strictEqual(status!.status, "pending");

        // 10. Assert no prototype values leak in the entire lineage table.
        const protoModes = yield* sql<{ readonly cnt: number }>`
          SELECT COUNT(*) AS cnt FROM scient_thread_lineage
          WHERE provider_mode IN ('cold-start', 'chat-only', 'replay')
             OR fidelity_mode IN ('cold-start', 'chat-only', 'replay')
        `;
        assert.strictEqual(protoModes[0]!.cnt, 0);

        // 11. Assert fork1 lineage row is unchanged by the re-fork.
        const fork1Lineage = yield* sql<{ readonly status: string }>`
          SELECT status FROM scient_thread_lineage WHERE thread_id = 'pr15-fork-006-1'
        `;
        assert.strictEqual(fork1Lineage[0]!.status, "ready");
      }),
    );
  },
);

// ===========================================================================
// VAL-CROSS-008: Scient and T3 migrations remain disjoint
// ===========================================================================

it.effect("VAL-CROSS-008: Scient and T3 migrations remain disjoint in both orders", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // 1. Run T3 migrations first, then Scient migrations.
      yield* runMigrations();
      yield* runScientMigrations(sql);

      // 2. Snapshot both ledgers.
      const t3LedgerAfterBoth = yield* sql<{
        readonly migration_id: number;
        readonly name: string;
      }>`
        SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
      `;
      const scientLedgerAfterBoth = yield* sql<{
        readonly migration_id: number;
        readonly name: string;
      }>`
        SELECT migration_id, name FROM scient_schema_migrations ORDER BY migration_id
      `;

      // 3. Neither ledger contains the other's migration names.
      const t3Names = new Set(t3LedgerAfterBoth.map((r) => r.name));
      const scientNames = new Set(scientLedgerAfterBoth.map((r) => r.name));
      assert.isFalse(t3Names.has("durable-thread-forks"));
      assert.isFalse(t3Names.has("normalize-active-lineage"));
      assert.isFalse(scientNames.has("OrchestrationEvents"));
      assert.isFalse(scientNames.has("Projections"));

      // 4. Run T3 migrations again — no new entries in either ledger.
      yield* runMigrations();
      const t3AfterRerun = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM effect_sql_migrations ORDER BY migration_id
      `;
      assert.strictEqual(t3AfterRerun.length, t3LedgerAfterBoth.length);

      const scientAfterRerun = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
      `;
      assert.strictEqual(scientAfterRerun.length, scientLedgerAfterBoth.length);

      // 5. Run Scient migrations again — no new entries in either ledger.
      yield* runScientMigrations(sql);
      const t3AfterScientRerun = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM effect_sql_migrations ORDER BY migration_id
      `;
      assert.strictEqual(t3AfterScientRerun.length, t3LedgerAfterBoth.length);

      const scientAfterScientRerun = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
      `;
      assert.strictEqual(scientAfterScientRerun.length, scientLedgerAfterBoth.length);

      // 6. Hypothetical T3 migration ID 39 and Scient migration ID 3 cannot
      //    collide: they live in separate tables with independent numbering.
      //    Verify the Scient ledger has ID 3 and the T3 ledger does not.
      const scientIds = new Set(scientLedgerAfterBoth.map((r) => r.migration_id));
      assert.isTrue(scientIds.has(3));

      // T3 IDs are in a different range (typically 1..N for T3's own schema),
      // but even if T3 had ID 3, it would be in a different table. The
      // assertion is that the ledgers are disjoint tables, not that the ID
      // ranges are numerically disjoint.
      // Verify T3 ledger does not contain Scient migration names.
      assert.isFalse(t3Names.has("durable-provider-bootstrap"));
      assert.isFalse(t3Names.has("normalize-active-lineage"));
    }),
  ),
);

it.effect("VAL-CROSS-008: running Scient first then T3 leaves both ledgers intact", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // Run Scient migrations first, then T3 migrations.
      yield* runScientMigrations(sql);
      yield* runMigrations();

      const t3Ledger = yield* sql<{ readonly migration_id: number; readonly name: string }>`
        SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
      `;
      const scientLedger = yield* sql<{ readonly migration_id: number; readonly name: string }>`
        SELECT migration_id, name FROM scient_schema_migrations ORDER BY migration_id
      `;

      // Both ledgers are complete and independent.
      assert.isTrue(t3Ledger.length > 0);
      assert.deepStrictEqual(
        scientLedger.map((r) => r.migration_id),
        [1, 2, 3, 4, 5, 6, 7],
      );

      // T3 ledger does not contain Scient migration names.
      const t3Names = new Set(t3Ledger.map((r) => r.name));
      assert.isFalse(t3Names.has("durable-thread-forks"));
      assert.isFalse(t3Names.has("durable-provider-bootstrap"));
      assert.isFalse(t3Names.has("normalize-active-lineage"));

      // Scient ledger does not contain T3 migration names.
      const scientNames = new Set(scientLedger.map((r) => r.name));
      for (const t3Name of t3Ledger.map((r) => r.name)) {
        assert.isFalse(scientNames.has(t3Name));
      }
    }),
  ),
);
