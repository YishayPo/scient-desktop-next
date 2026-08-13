import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "node:crypto";
import { executionOutputContentParts } from "@scientfactory/execution";
import { initializeScientProject } from "@scientfactory/project-init";
import { HostProcessEnvironment, HostProcessPlatform } from "@t3tools/shared/hostProcess";

import {
  AuthAccessTokenType,
  AnalysisSourceRevision,
  AuthEnvironmentBootstrapTokenType,
  AuthTokenExchangeGrantType,
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  EventId,
  GitCommandError,
  KeybindingRule,
  MessageId,
  ExternalLauncherCommandNotFoundError,
  OrchestrationThreadDetailSnapshot,
  type OrchestrationThreadStreamItem,
  type OrchestrationThreadShell,
  TerminalNotRunningError,
  type OrchestrationCommand,
  type OrchestrationEvent,
  ORCHESTRATION_WS_METHODS,
  type PreviewEvent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ResolvedKeybindingRule,
  ThreadId,
  WS_METHODS,
  WsRpcGroup,
  EditorId,
} from "@t3tools/contracts";
import {
  computeDpopAccessTokenHash,
  computeDpopJwkThumbprint,
  type DpopPublicJwk,
} from "@t3tools/shared/dpop";
import { RELAY_HEALTH_REQUEST_TYP, RELAY_MINT_REQUEST_TYP } from "@t3tools/shared/relayJwt";
import * as RelayClient from "@t3tools/shared/relayClient";
import { assert, it } from "@effect/vitest";
import { assertFailure, assertInclude, assertTrue } from "@effect/vitest/utils";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/unstable/process";
import {
  FetchHttpClient,
  HttpBody,
  HttpClient,
  HttpClientRequest,
  HttpClientResponse,
  HttpRouter,
  HttpServer,
} from "effect/unstable/http";
import { OtlpSerialization, OtlpTracer } from "effect/unstable/observability";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import * as Socket from "effect/unstable/socket/Socket";
import { afterAll, beforeAll, vi } from "vite-plus/test";

const TEST_EPOCH = DateTime.makeUnsafe("1970-01-01T00:00:00.000Z");
const decodeTransferThreadSnapshot = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationThreadDetailSnapshot),
);

const collectQueueUntil = Effect.fn("TransferBudget.collectQueueUntil")(function* <A>(
  queue: Queue.Queue<A>,
  predicate: (value: A) => boolean,
  waitDescription: string,
) {
  return yield* Effect.gen(function* () {
    const values: A[] = [];
    while (true) {
      const value = yield* Queue.take(queue);
      values.push(value);
      if (predicate(value)) return values;
    }
  }).pipe(
    Effect.timeoutOrElse({
      duration: "10 seconds",
      orElse: () => Effect.die(new Error(`Timed out waiting for ${waitDescription}`)),
    }),
  );
});

// The server-router suite exercises the inherited cloud protocol directly.
// Opt this test file into the otherwise-disabled D4 routes and restore the
// caller's environment when the file completes; this test-only switch does
// not make public cloud configuration appear in the route builder.
const previousScientNextCloudRouteTest = process.env.SCIENT_NEXT_CLOUD_ROUTE_TEST;
beforeAll(() => {
  process.env.SCIENT_NEXT_CLOUD_ROUTE_TEST = "true";
});
afterAll(() => {
  if (previousScientNextCloudRouteTest === undefined)
    delete process.env.SCIENT_NEXT_CLOUD_ROUTE_TEST;
  else process.env.SCIENT_NEXT_CLOUD_ROUTE_TEST = previousScientNextCloudRouteTest;
});

import * as BackgroundPolicy from "./background/BackgroundPolicy.ts";
import * as ServerConfig from "./config.ts";
import { makeRoutesLayer } from "./server.ts";
import { isThreadDetailEvent, resolveAvailableEditorsForConfig } from "./ws.ts";
import * as CheckpointDiffQuery from "./checkpointing/CheckpointDiffQuery.ts";
import * as GitManager from "./git/GitManager.ts";
import * as Keybindings from "./keybindings.ts";
import * as ExternalLauncher from "./process/externalLauncher.ts";
import * as OrchestrationEngine from "./orchestration/Services/OrchestrationEngine.ts";
import { OrchestrationListenerCallbackError } from "./orchestration/Errors.ts";
import * as ProjectionSnapshotQuery from "./orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ScientForkReactor from "./orchestration/Services/ScientForkReactor.ts";
import { SqlitePersistenceMemory } from "./persistence/Layers/Sqlite.ts";
import { PersistenceSqlError } from "./persistence/Errors.ts";
import * as ProviderRegistry from "./provider/Services/ProviderRegistry.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "./provider/providerMaintenance.ts";
import * as ServerLifecycleEvents from "./serverLifecycleEvents.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";
import * as ServiceLauncherClient from "./cloud/serviceLauncherClient.ts";
import * as ServerSettings from "./serverSettings.ts";
import * as TerminalManager from "./terminal/Manager.ts";
import * as PreviewManager from "./preview/Manager.ts";
import * as PortScanner from "./preview/PortScanner.ts";
import * as BrowserTraceCollector from "./observability/BrowserTraceCollector.ts";
import * as ProjectFaviconResolver from "./project/ProjectFaviconResolver.ts";
import * as T3ProjectFileLoader from "./project/T3ProjectFileLoader.ts";
import * as ProjectSetupScriptRunner from "./project/ProjectSetupScriptRunner.ts";
import * as RepositoryIdentityResolver from "./project/RepositoryIdentityResolver.ts";
import * as ServerEnvironment from "./environment/ServerEnvironment.ts";
import * as GeneratedDocumentStore from "./scient/documentArtifacts/GeneratedDocumentStore.ts";
import * as WorkspaceEntries from "./workspace/WorkspaceEntries.ts";
import * as WorkspaceFileSystem from "./workspace/WorkspaceFileSystem.ts";
import * as WorkspacePaths from "./workspace/WorkspacePaths.ts";
import * as GitVcsDriver from "./vcs/GitVcsDriver.ts";
import * as VcsDriver from "./vcs/VcsDriver.ts";
import * as VcsStatusBroadcaster from "./vcs/VcsStatusBroadcaster.ts";
import * as VcsDriverRegistry from "./vcs/VcsDriverRegistry.ts";
import * as VcsProvisioningService from "./vcs/VcsProvisioningService.ts";
import * as GitWorkflowService from "./git/GitWorkflowService.ts";
import * as ReviewService from "./review/ReviewService.ts";
import * as SourceControlRepositoryService from "./sourceControl/SourceControlRepositoryService.ts";
import * as ServerSecretStore from "./auth/ServerSecretStore.ts";
import * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import * as CloudManagedEndpointRuntime from "./cloud/ManagedEndpointRuntime.ts";
import * as CloudCliTokenManager from "./cloud/CliTokenManager.ts";
import * as ProcessDiagnostics from "./diagnostics/ProcessDiagnostics.ts";
import * as ProcessResourceMonitor from "./diagnostics/ProcessResourceMonitor.ts";
import * as TraceDiagnostics from "./diagnostics/TraceDiagnostics.ts";
import * as DesktopTelemetryReceiver from "./resourceTelemetry/DesktopTelemetryReceiver.ts";
import * as NativeTelemetryClient from "./resourceTelemetry/NativeTelemetryClient.ts";
import * as ResourceAttribution from "./resourceTelemetry/ResourceAttribution.ts";
import * as ResourceTelemetry from "./resourceTelemetry/ResourceTelemetry.ts";
import * as UsageService from "./usage/UsageService.ts";
import * as Data from "effect/Data";

import { makeOrchestrationIntegrationHarness } from "../integration/OrchestrationEngineHarness.integration.ts";
import {
  countingWsRpcProtocolLayer,
  makeCountingWsRpcClient,
  makeWebSocketTransferRecorder,
  measureHttpGet,
  transferDelta,
} from "../integration/NetworkTransferMeasurement.integration.ts";
import {
  expectedMeasuredAssistantText,
  queueMeasuredTransferTurn,
  seedTransferBudgetHistory,
  TRANSFER_HISTORY_TURN_COUNT,
  TRANSFER_MEASURED_TURN_CREATED_AT,
  TRANSFER_MEASURED_TURN_INDEX,
  TRANSFER_THREAD_ID,
  transferModelSelection,
  waitForTurnQuiesced,
} from "../integration/TransferBudgetScenario.integration.ts";
import {
  formatTransferBudgetReport,
  formatTransferBudgetResult,
  type TransferBudgetRun,
  transferBudgetViolations,
} from "../integration/TransferBudgetReport.integration.ts";

const defaultProjectId = ProjectId.make("project-default");
const defaultThreadId = ThreadId.make("thread-default");
const defaultDesktopBootstrapToken = "test-desktop-bootstrap-token";
const defaultModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5-codex",
} as const;
const testEnvironmentDescriptor = {
  environmentId: EnvironmentId.make("environment-test"),
  label: "Test environment",
  platform: {
    os: "darwin" as const,
    arch: "arm64" as const,
  },
  serverVersion: "0.0.0-test",
  capabilities: {
    repositoryIdentity: true,
  },
};
const makeDefaultOrchestrationReadModel = () => {
  const now = "2026-01-01T00:00:00.000Z";
  return {
    snapshotSequence: 0,
    updatedAt: now,
    projects: [
      {
        id: defaultProjectId,
        title: "Default Project",
        workspaceRoot: "/tmp/default-project",
        defaultModelSelection,
        scripts: [],
        createdAt: now,
        updatedAt: now,
        deletedAt: null,
      },
    ],
    threads: [
      {
        id: defaultThreadId,
        projectId: defaultProjectId,
        title: "Default Thread",
        modelSelection: defaultModelSelection,
        interactionMode: "default" as const,
        runtimeMode: "full-access" as const,
        branch: null,
        worktreePath: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        latestTurn: null,
        messages: [],
        session: null,
        activities: [],
        proposedPlans: [],
        checkpoints: [],
        deletedAt: null,
      },
    ],
  };
};

const makeDefaultOrchestrationThreadShell = (
  overrides: Partial<OrchestrationThreadShell> = {},
): OrchestrationThreadShell => {
  const now = "2026-01-01T00:00:00.000Z";
  return {
    id: defaultThreadId,
    projectId: defaultProjectId,
    title: "Default Thread",
    modelSelection: defaultModelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    latestTurn: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    session: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    ...overrides,
  };
};

const browserOtlpTracingLayer = Layer.mergeAll(
  FetchHttpClient.layer,
  OtlpSerialization.layerJson,
  Layer.succeed(HttpClient.TracerDisabledWhen, () => true),
);

const makeAuthTestLayer = () =>
  EnvironmentAuth.layer.pipe(
    Layer.provide(SqlitePersistenceMemory),
    Layer.provide(ServerSecretStore.layer),
  );

const makeBrowserOtlpPayload = (spanName: string) =>
  Effect.gen(function* () {
    const collector = yield* Effect.acquireRelease(
      Effect.promise(async () => {
        const NodeHttp = await import("node:http");

        return await new Promise<{
          readonly close: () => Promise<void>;
          readonly firstRequest: Promise<{
            readonly body: string;
            readonly contentType: string | null;
          }>;
          readonly url: string;
        }>((resolve, reject) => {
          let resolveFirstRequest:
            | ((request: { readonly body: string; readonly contentType: string | null }) => void)
            | undefined;
          const firstRequest = new Promise<{
            readonly body: string;
            readonly contentType: string | null;
          }>((resolveRequest) => {
            resolveFirstRequest = resolveRequest;
          });

          const server = NodeHttp.createServer((request, response) => {
            const chunks: Buffer[] = [];
            request.on("data", (chunk) => {
              chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
            });
            request.on("end", () => {
              resolveFirstRequest?.({
                body: Buffer.concat(chunks).toString("utf8"),
                contentType: request.headers["content-type"] ?? null,
              });
              resolveFirstRequest = undefined;
              response.statusCode = 204;
              response.end();
            });
          });

          server.on("error", reject);
          server.listen(0, "127.0.0.1", () => {
            const address = server.address();
            if (!address || typeof address === "string") {
              reject(new Error("Expected TCP collector address"));
              return;
            }

            resolve({
              url: `http://127.0.0.1:${address.port}/v1/traces`,
              firstRequest,
              close: () =>
                new Promise<void>((resolveClose, rejectClose) => {
                  server.close((error) => {
                    if (error) {
                      rejectClose(error);
                      return;
                    }
                    resolveClose();
                  });
                }),
            });
          });
        });
      }),
      ({ close }) => Effect.promise(close),
    );

    const runtime = ManagedRuntime.make(
      OtlpTracer.layer({
        url: collector.url,
        exportInterval: "10 millis",
        resource: {
          serviceName: "t3-web",
          attributes: {
            "service.runtime": "t3-web",
            "service.mode": "browser",
            "service.version": "test",
          },
        },
      }).pipe(Layer.provide(browserOtlpTracingLayer)),
    );

    try {
      yield* Effect.promise(() => runtime.runPromise(Effect.void.pipe(Effect.withSpan(spanName))));
    } finally {
      yield* Effect.promise(() => runtime.dispose());
    }

    const request = yield* Effect.raceFirst(
      Effect.promise(() => collector.firstRequest).pipe(Effect.orDie),
      Effect.sleep(Duration.seconds(1)).pipe(
        Effect.andThen(Effect.die(new Error("Timed out waiting for OTLP trace export"))),
      ),
    );
    // @effect-diagnostics-next-line preferSchemaOverJson:off
    return JSON.parse(request.body) as OtlpTracer.TraceData;
  });

const buildAppUnderTest = (options?: {
  config?: Partial<ServerConfig.ServerConfig["Service"]>;
  layers?: {
    keybindings?: Partial<Keybindings.Keybindings["Service"]>;
    providerRegistry?: Partial<ProviderRegistry.ProviderRegistry["Service"]>;
    serverSettings?: Partial<ServerSettings.ServerSettingsService["Service"]>;
    externalLauncher?: Partial<ExternalLauncher.ExternalLauncher["Service"]>;
    vcsDriver?: Partial<VcsDriver.VcsDriver["Service"]>;
    vcsDriverRegistry?: Partial<VcsDriverRegistry.VcsDriverRegistry["Service"]>;
    gitVcsDriver?: Partial<GitVcsDriver.GitVcsDriver["Service"]>;
    gitManager?: Partial<GitManager.GitManager["Service"]>;
    sourceControlRepositoryService?: Partial<
      SourceControlRepositoryService.SourceControlRepositoryService["Service"]
    >;
    reviewService?: Partial<ReviewService.ReviewService["Service"]>;
    vcsStatusBroadcaster?: Partial<VcsStatusBroadcaster.VcsStatusBroadcaster["Service"]>;
    projectSetupScriptRunner?: Partial<
      ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]
    >;
    terminalManager?: Partial<TerminalManager.TerminalManager["Service"]>;
    orchestrationEngine?: Partial<OrchestrationEngine.OrchestrationEngineService["Service"]>;
    projectionSnapshotQuery?: Partial<ProjectionSnapshotQuery.ProjectionSnapshotQuery["Service"]>;
    scientForkReactor?: Partial<ScientForkReactor.ScientForkReactor["Service"]>;
    checkpointDiffQuery?: Partial<CheckpointDiffQuery.CheckpointDiffQuery["Service"]>;
    browserTraceCollector?: Partial<BrowserTraceCollector.BrowserTraceCollector["Service"]>;
    serverLifecycleEvents?: Partial<ServerLifecycleEvents.ServerLifecycleEvents["Service"]>;
    serverRuntimeStartup?: Partial<ServerRuntimeStartup.ServerRuntimeStartup["Service"]>;
    serverEnvironment?: Partial<ServerEnvironment.ServerEnvironment["Service"]>;
    repositoryIdentityResolver?: Partial<
      RepositoryIdentityResolver.RepositoryIdentityResolver["Service"]
    >;
    cloudManagedEndpointRuntime?: Partial<
      CloudManagedEndpointRuntime.CloudManagedEndpointRuntime["Service"]
    >;
    relayClient?: Partial<RelayClient.RelayClient["Service"]>;
    cloudCliTokenManager?: Partial<CloudCliTokenManager.CloudCliTokenManager["Service"]>;
    nativeTelemetryClient?: Partial<NativeTelemetryClient.NativeTelemetryClient["Service"]>;
    desktopTelemetryReceiver?: Partial<
      DesktopTelemetryReceiver.DesktopTelemetryReceiver["Service"]
    >;
  };
}) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const tempBaseDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-router-test-" });
    const baseDir = options?.config?.baseDir ?? tempBaseDir;
    const devUrl = options?.config?.devUrl;
    const derivedPaths = yield* ServerConfig.deriveServerPaths(baseDir, devUrl);
    const config: ServerConfig.ServerConfig["Service"] = {
      logLevel: "Info",
      traceMinLevel: "Info",
      traceTimingEnabled: true,
      traceBatchWindowMs: 200,
      traceMaxBytes: 10 * 1024 * 1024,
      traceMaxFiles: 10,
      otlpTracesUrl: undefined,
      otlpMetricsUrl: undefined,
      otlpExportIntervalMs: 10_000,
      otlpServiceName: "t3-server",
      mode: "desktop",
      port: 0,
      host: "127.0.0.1",
      cwd: process.cwd(),
      baseDir,
      ...derivedPaths,
      staticDir: undefined,
      devUrl,
      devAllowedOrigins: [],
      noBrowser: true,
      startupPresentation: "browser",
      desktopBootstrapToken: defaultDesktopBootstrapToken,
      autoBootstrapProjectFromCwd: false,
      logWebSocketEvents: false,
      tailscaleServeEnabled: false,
      tailscaleServePort: 443,
      ...options?.config,
    };
    const layerConfig = ServerConfig.layer(config);
    const defaultVcsDriver: VcsDriver.VcsDriver["Service"] = {
      capabilities: {
        kind: "git",
        supportsWorktrees: true,
        supportsBookmarks: false,
        supportsAtomicSnapshot: false,
        supportsPushDefaultRemote: true,
        ignoreClassifier: "native",
      },
      execute: () =>
        Effect.succeed({
          exitCode: ChildProcessSpawner.ExitCode(0),
          stdout: "",
          stderr: "",
          stdoutTruncated: false,
          stderrTruncated: false,
        }),
      detectRepository: () => Effect.succeed(null),
      isInsideWorkTree: () => Effect.succeed(false),
      listWorkspaceFiles: () =>
        Effect.succeed({
          paths: [],
          truncated: false,
          freshness: {
            source: "live-local",
            observedAt: TEST_EPOCH,
            expiresAt: Option.none(),
          },
        }),
      listRemotes: () =>
        Effect.succeed({
          remotes: [],
          freshness: {
            source: "live-local",
            observedAt: TEST_EPOCH,
            expiresAt: Option.none(),
          },
        }),
      filterIgnoredPaths: (_cwd, relativePaths) => Effect.succeed(relativePaths),
      initRepository: () => Effect.void,
      ...options?.layers?.vcsDriver,
    };
    const vcsDriverRegistryLayer = Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
      get: () => Effect.succeed(defaultVcsDriver),
      detect: (input) =>
        defaultVcsDriver.detectRepository(input.cwd).pipe(
          Effect.flatMap((repository) =>
            repository
              ? Effect.succeed(repository)
              : defaultVcsDriver.isInsideWorkTree(input.cwd).pipe(
                  Effect.map((isInsideWorkTree) =>
                    isInsideWorkTree
                      ? {
                          kind: "git" as const,
                          rootPath: input.cwd,
                          metadataPath: null,
                          freshness: {
                            source: "live-local" as const,
                            observedAt: TEST_EPOCH,
                            expiresAt: Option.none(),
                          },
                        }
                      : null,
                  ),
                ),
          ),
          Effect.map((repository) =>
            repository
              ? ({
                  kind: repository.kind,
                  repository,
                  driver: defaultVcsDriver,
                } satisfies VcsDriverRegistry.VcsDriverHandle)
              : null,
          ),
        ),
      resolve: (input) =>
        Effect.succeed({
          kind:
            input.requestedKind === "auto" || !input.requestedKind ? "git" : input.requestedKind,
          repository: {
            kind:
              input.requestedKind === "auto" || !input.requestedKind ? "git" : input.requestedKind,
            rootPath: input.cwd,
            metadataPath: null,
            freshness: {
              source: "live-local",
              observedAt: TEST_EPOCH,
              expiresAt: Option.none(),
            },
          },
          driver: defaultVcsDriver,
        }),
      ...options?.layers?.vcsDriverRegistry,
    });
    const gitVcsDriverLayer = Layer.mock(GitVcsDriver.GitVcsDriver)({
      ...options?.layers?.gitVcsDriver,
    });
    const gitManagerLayer = Layer.mock(GitManager.GitManager)({
      ...options?.layers?.gitManager,
    });
    const workspaceEntriesLayer = WorkspaceEntries.layer.pipe(
      Layer.provide(WorkspacePaths.layer),
      Layer.provideMerge(vcsDriverRegistryLayer),
    );
    const workspaceAndProjectServicesLayer = Layer.mergeAll(
      WorkspacePaths.layer,
      workspaceEntriesLayer,
      WorkspaceFileSystem.layer.pipe(
        Layer.provide(WorkspacePaths.layer),
        Layer.provide(workspaceEntriesLayer),
      ),
      ProjectFaviconResolver.layer.pipe(
        Layer.provide(WorkspacePaths.layer),
        Layer.provide(T3ProjectFileLoader.layer),
      ),
    );
    const gitWorkflowLayer = GitWorkflowService.layer.pipe(
      Layer.provideMerge(vcsDriverRegistryLayer),
      Layer.provideMerge(gitVcsDriverLayer),
      Layer.provideMerge(gitManagerLayer),
    );
    const vcsProvisioningLayer = VcsProvisioningService.layer.pipe(
      Layer.provide(vcsDriverRegistryLayer),
    );
    const reviewLayer = options?.layers?.reviewService
      ? Layer.mock(ReviewService.ReviewService)({
          ...options.layers.reviewService,
        })
      : ReviewService.layer.pipe(
          Layer.provideMerge(gitVcsDriverLayer),
          Layer.provide(vcsDriverRegistryLayer),
        );
    const vcsStatusBroadcasterLayer = options?.layers?.vcsStatusBroadcaster
      ? Layer.mock(VcsStatusBroadcaster.VcsStatusBroadcaster)({
          ...options.layers.vcsStatusBroadcaster,
        })
      : VcsStatusBroadcaster.layer.pipe(Layer.provide(gitWorkflowLayer));
    const resourceTelemetryLayer = ResourceTelemetry.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          NativeTelemetryClient.layerTest(options?.layers?.nativeTelemetryClient),
          DesktopTelemetryReceiver.layerTest(options?.layers?.desktopTelemetryReceiver),
          ResourceAttribution.layer,
        ),
      ),
    );
    const serviceLauncherClientLayer = ServiceLauncherClient.layer.pipe(
      Layer.provide(Layer.succeed(HostProcessEnvironment, {})),
    );

    const servedRoutesLayer = HttpRouter.serve(
      makeRoutesLayer.pipe(Layer.provide(serviceLauncherClientLayer)),
      {
        disableListenLog: true,
        disableLogger: true,
      },
    ).pipe(
      Layer.provide(
        Layer.mock(Keybindings.Keybindings)({
          loadConfigState: Effect.succeed({
            keybindings: [],
            issues: [],
          }),
          streamChanges: Stream.empty,
          ...options?.layers?.keybindings,
        }),
      ),
      Layer.provide(
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([]),
          refresh: () => Effect.succeed([]),
          refreshInstance: () => Effect.succeed([]),
          reloadInstance: () => Effect.succeed([]),
          getProviderMaintenanceCapabilitiesForInstance: (_instanceId, provider) =>
            Effect.succeed(
              makeManualOnlyProviderMaintenanceCapabilities({ provider, packageName: null }),
            ),
          getProviderConnectionActionsForInstance: () => Effect.succeed(undefined),
          getProviderManagedRuntimeActionsForInstance: () => Effect.succeed(undefined),
          setProviderManagedRuntimeSummary: () => Effect.succeed([]),
          setProviderMaintenanceActionState: () => Effect.succeed([]),
          setProviderConnectionOperation: () => Effect.succeed([]),
          streamChanges: Stream.empty,
          ...options?.layers?.providerRegistry,
        }),
      ),
      Layer.provide(
        Layer.mock(ServerSettings.ServerSettingsService)({
          start: Effect.void,
          ready: Effect.void,
          getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
          updateSettings: () => Effect.succeed(DEFAULT_SERVER_SETTINGS),
          streamChanges: Stream.empty,
          ...options?.layers?.serverSettings,
        }),
      ),
      Layer.provide(
        Layer.mock(ExternalLauncher.ExternalLauncher)({
          resolveAvailableEditors: () => Effect.succeed([]),
          ...options?.layers?.externalLauncher,
        }),
      ),
      Layer.provide(
        Layer.mock(ProcessDiagnostics.ProcessDiagnostics)({
          read: Effect.succeed({
            serverPid: process.pid,
            readAt: TEST_EPOCH,
            processCount: 0,
            totalRssBytes: 0,
            totalCpuPercent: 0,
            processes: [],
            error: Option.none(),
          }),
          signal: (input) =>
            Effect.succeed({
              pid: input.pid,
              signal: input.signal,
              signaled: true,
              message: Option.none(),
            }),
        }),
      ),
      Layer.provide(
        Layer.mock(ProcessResourceMonitor.ProcessResourceMonitor)({
          readHistory: (input) =>
            Effect.succeed({
              readAt: TEST_EPOCH,
              windowMs: input.windowMs,
              bucketMs: input.bucketMs,
              sampleIntervalMs: 5_000,
              retainedSampleCount: 0,
              totalCpuSecondsApprox: 0,
              buckets: [],
              topProcesses: [],
              error: Option.none(),
            }),
        }),
      ),
      Layer.provide(
        Layer.mock(TraceDiagnostics.TraceDiagnostics)({
          read: () =>
            Effect.succeed({
              traceFilePath: "",
              scannedFilePaths: [],
              readAt: TEST_EPOCH,
              recordCount: 0,
              parseErrorCount: 0,
              firstSpanAt: Option.none(),
              lastSpanAt: Option.none(),
              failureCount: 0,
              interruptionCount: 0,
              slowSpanThresholdMs: 1_000,
              slowSpanCount: 0,
              logLevelCounts: {},
              topSpansByCount: [],
              slowestSpans: [],
              commonFailures: [],
              latestFailures: [],
              latestWarningAndErrorLogs: [],
              partialFailure: Option.none(),
              error: Option.none(),
            }),
        }),
      ),
      Layer.provide(gitManagerLayer),
      Layer.provide(gitVcsDriverLayer),
      Layer.provide(gitWorkflowLayer),
      Layer.provide(reviewLayer),
      Layer.provide(vcsProvisioningLayer),
      Layer.provide(
        Layer.mock(SourceControlRepositoryService.SourceControlRepositoryService)({
          ...options?.layers?.sourceControlRepositoryService,
        }),
      ),
      Layer.provideMerge(vcsStatusBroadcasterLayer),
      Layer.provide(
        Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({
          runForThread: () => Effect.succeed({ status: "no-script" as const }),
          ...options?.layers?.projectSetupScriptRunner,
        }),
      ),
      Layer.provide(
        Layer.mock(TerminalManager.TerminalManager)({
          hasRunningSessionsForThread: () => Effect.succeed(false),
          ...options?.layers?.terminalManager,
        }),
      ),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(PreviewManager.PreviewManager)({
            open: () => Effect.die("PreviewManager not stubbed in this test"),
            navigate: () => Effect.die("PreviewManager not stubbed in this test"),
            resize: () => Effect.die("PreviewManager not stubbed in this test"),
            reportStatus: () => Effect.void,
            refresh: () => Effect.void,
            close: () => Effect.void,
            list: () => Effect.succeed({ sessions: [], serverEpoch: "test-server", revision: 0 }),
            events: Stream.empty,
            subscribeEvents: Effect.flatMap(PubSub.unbounded<PreviewEvent>(), (pubsub) =>
              PubSub.subscribe(pubsub),
            ),
          }),
          Layer.mock(PortScanner.PortDiscovery)({
            scan: () => Effect.succeed([]),
            subscribe: () => Effect.void,
            retain: Effect.void,
            registerTerminalProcesses: () => Effect.void,
            unregisterTerminal: () => Effect.void,
          }),
        ),
      ),
      Layer.provide(
        Layer.mock(OrchestrationEngine.OrchestrationEngineService)({
          readEvents: () => Stream.empty,
          dispatch: () => Effect.succeed({ sequence: 0 }),
          streamDomainEvents: Stream.empty,
          latestSequence: Effect.succeed(0),
          ...options?.layers?.orchestrationEngine,
        }),
      ),
      Layer.provide(
        Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
          getCommandReadModel: () => Effect.succeed(makeDefaultOrchestrationReadModel()),
          getSnapshot: () => Effect.succeed(makeDefaultOrchestrationReadModel()),
          getShellSnapshot: () =>
            Effect.succeed({
              snapshotSequence: 0,
              projects: [],
              threads: [],
              updatedAt: "1970-01-01T00:00:00.000Z",
            }),
          getArchivedShellSnapshot: () =>
            Effect.succeed({
              snapshotSequence: 0,
              projects: [],
              threads: [],
              updatedAt: "1970-01-01T00:00:00.000Z",
            }),
          searchThreads: () => Effect.succeed({ matches: [] }),
          getSnapshotSequence: () => Effect.succeed({ snapshotSequence: 0 }),
          getProjectShellById: () => Effect.succeed(Option.none()),
          getThreadShellById: () => Effect.succeed(Option.none()),
          getThreadDetailById: () => Effect.succeed(Option.none()),
          getThreadDetailSnapshot: () => Effect.succeed(Option.none()),
          getCounts: () => Effect.succeed({ projectCount: 0, threadCount: 0 }),
          getActiveProjectByWorkspaceRoot: () => Effect.succeed(Option.none()),
          getFirstActiveThreadIdByProjectId: () => Effect.succeed(Option.none()),
          getThreadCheckpointContext: () => Effect.succeed(Option.none()),
          ...options?.layers?.projectionSnapshotQuery,
        }),
      ),
      Layer.provide(
        Layer.mock(CheckpointDiffQuery.CheckpointDiffQuery)({
          getTurnDiff: () =>
            Effect.succeed({
              threadId: defaultThreadId,
              fromTurnCount: 0,
              toTurnCount: 0,
              diff: "",
            }),
          getFullThreadDiff: () =>
            Effect.succeed({
              threadId: defaultThreadId,
              fromTurnCount: 0,
              toTurnCount: 0,
              diff: "",
            }),
          ...options?.layers?.checkpointDiffQuery,
        }),
      ),
    );

    const appLayer = servedRoutesLayer.pipe(
      Layer.provide(GeneratedDocumentStore.layer),
      Layer.provide(
        Layer.mock(ScientForkReactor.ScientForkReactor)({
          start: () => Effect.void,
          drain: Effect.void,
          awaitCompletion: () => Effect.void,
          ...options?.layers?.scientForkReactor,
        }),
      ),
      Layer.provide(resourceTelemetryLayer),
      Layer.provide(UsageService.layerTest),
      Layer.provide(
        Layer.mock(BrowserTraceCollector.BrowserTraceCollector)({
          record: () => Effect.void,
          ...options?.layers?.browserTraceCollector,
        }),
      ),
      Layer.provide(
        Layer.mock(ServerLifecycleEvents.ServerLifecycleEvents)({
          publish: (event) => Effect.succeed({ ...(event as any), sequence: 1 }),
          snapshot: Effect.succeed({ sequence: 0, events: [] }),
          stream: Stream.empty,
          ...options?.layers?.serverLifecycleEvents,
        }),
      ),
      Layer.provide(
        Layer.mock(ServerRuntimeStartup.ServerRuntimeStartup)({
          awaitCommandReady: Effect.void,
          markHttpListening: Effect.void,
          enqueueCommand: (effect) => effect,
          ...options?.layers?.serverRuntimeStartup,
        }),
      ),
      Layer.provide(
        Layer.mock(BackgroundPolicy.BackgroundPolicy)({
          reportClientActivity: () => Effect.void,
          removeRpcClient: () => Effect.void,
          reportHostPowerState: () => Effect.void,
          snapshot: Effect.succeed({
            hostPower: {
              source: "unknown",
              idle: "unknown",
              idleSeconds: null,
              locked: "unknown",
              suspended: false,
              onBattery: "unknown",
              lowPowerMode: "unknown",
              thermalState: "unknown",
              stale: true,
              updatedAt: TEST_EPOCH,
            },
            leases: [],
            activeForegroundLeaseCount: 0,
            activeScopeKeys: [],
            shouldRunOpportunisticWork: false,
            updatedAt: TEST_EPOCH,
          }),
          streamChanges: Stream.empty,
          subscribe: Effect.succeed({
            latest: {
              hostPower: {
                source: "unknown",
                idle: "unknown",
                idleSeconds: null,
                locked: "unknown",
                suspended: false,
                onBattery: "unknown",
                lowPowerMode: "unknown",
                thermalState: "unknown",
                stale: true,
                updatedAt: TEST_EPOCH,
              },
              leases: [],
              activeForegroundLeaseCount: 0,
              activeScopeKeys: [],
              shouldRunOpportunisticWork: false,
              updatedAt: TEST_EPOCH,
            },
            changes: Stream.empty,
          }),
          hasDemand: () => Effect.succeed(false),
          shouldRunScopeWork: () => Effect.succeed(false),
          shouldRunOpportunisticWork: Effect.succeed(false),
        }),
      ),
      Layer.provide(
        Layer.mock(ServerEnvironment.ServerEnvironment)({
          getEnvironmentId: Effect.succeed(testEnvironmentDescriptor.environmentId),
          getDescriptor: Effect.succeed(testEnvironmentDescriptor),
          ...options?.layers?.serverEnvironment,
        }),
      ),
      Layer.provide(
        Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
          resolve: () => Effect.succeed(null),
          ...options?.layers?.repositoryIdentityResolver,
        }),
      ),
      Layer.provide(
        Layer.succeed(
          CloudManagedEndpointRuntime.CloudManagedEndpointRuntime,
          CloudManagedEndpointRuntime.CloudManagedEndpointRuntime.of({
            applyConfig: () => Effect.succeed({ status: "disabled" }),
            ...options?.layers?.cloudManagedEndpointRuntime,
          }),
        ),
      ),
      Layer.provide(
        Layer.succeed(
          RelayClient.RelayClient,
          RelayClient.RelayClient.of({
            resolve: Effect.succeed({
              status: "missing",
              version: RelayClient.CLOUDFLARED_VERSION,
            }),
            install: Effect.die("unused relay-client install"),
            installWithProgress: () => Effect.die("unused relay-client install"),
            ...options?.layers?.relayClient,
          }),
        ),
      ),
      Layer.provide(
        Layer.mock(CloudCliTokenManager.CloudCliTokenManager)({
          get: Effect.die(new Error("Unexpected T3 Connect CLI authorization request.")),
          getExisting: Effect.succeed(Option.none()),
          hasCredential: Effect.succeed(false),
          clear: Effect.void,
          ...options?.layers?.cloudCliTokenManager,
        }),
      ),
      Layer.provideMerge(makeAuthTestLayer()),
      Layer.provideMerge(ServerSecretStore.layer),
      Layer.provide(workspaceAndProjectServicesLayer),
      Layer.provideMerge(FetchHttpClient.layer),
      Layer.provide(layerConfig),
    );

    yield* Layer.build(appLayer);
    return config;
  });

const parseSessionCookieFromWsUrl = (
  wsUrl: string,
): { readonly cookie: string | null; readonly url: string } => {
  const next = new URL(wsUrl);
  const cookie = next.hash.startsWith("#cookie=")
    ? decodeURIComponent(next.hash.slice("#cookie=".length))
    : null;
  next.hash = "";
  return {
    cookie,
    url: next.toString(),
  };
};

const wsRpcProtocolLayer = (wsUrl: string) => {
  const { cookie, url } = parseSessionCookieFromWsUrl(wsUrl);
  const webSocketConstructorLayer = Layer.succeed(
    Socket.WebSocketConstructor,
    (socketUrl, protocols) =>
      new NodeSocket.NodeWS.WebSocket(
        socketUrl,
        protocols,
        cookie ? { headers: { cookie } } : undefined,
      ) as unknown as globalThis.WebSocket,
  );

  return RpcClient.layerProtocolSocket().pipe(
    Layer.provide(Socket.layerWebSocket(url).pipe(Layer.provide(webSocketConstructorLayer))),
    Layer.provide(RpcSerialization.layerJson),
  );
};

const makeWsRpcClient = RpcClient.make(WsRpcGroup);
type WsRpcClient =
  typeof makeWsRpcClient extends Effect.Effect<infer Client, any, any> ? Client : never;

const withWsRpcClient = <A, E, R>(
  wsUrl: string,
  f: (client: WsRpcClient) => Effect.Effect<A, E, R>,
) => makeWsRpcClient.pipe(Effect.flatMap(f), Effect.provide(wsRpcProtocolLayer(wsUrl)));

const appendSessionCookieToWsUrl = (url: string, sessionCookieHeader: string) => {
  const isAbsoluteUrl = /^[a-zA-Z][a-zA-Z\d+.-]*:/.test(url);
  const next = new URL(url, "http://localhost");
  next.hash = `cookie=${encodeURIComponent(sessionCookieHeader)}`;
  return isAbsoluteUrl ? next.toString() : `${next.pathname}${next.search}${next.hash}`;
};

const getHttpServerUrl = (pathname = "") =>
  Effect.gen(function* () {
    const server = yield* HttpServer.HttpServer;
    const address = server.address as HttpServer.TcpAddress;
    return `http://127.0.0.1:${address.port}${pathname}`;
  });

const bootstrapBrowserSession = (
  credential = defaultDesktopBootstrapToken,
  options?: {
    readonly headers?: Record<string, string>;
  },
) =>
  Effect.gen(function* () {
    const bootstrapUrl = yield* getHttpServerUrl("/api/auth/browser-session");
    const response = yield* fetchEffect(bootstrapUrl, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...options?.headers,
      },
      body: jsonRequestBody({
        credential,
      }),
    });
    const body = yield* responseJsonEffect<{
      readonly authenticated: boolean;
      readonly sessionMethod: string;
      readonly expiresAt: string;
    }>(response);
    return {
      response,
      body,
      cookie: response.headers["set-cookie"],
    };
  });

const exchangeAccessToken = (
  credential = defaultDesktopBootstrapToken,
  options?: {
    readonly headers?: Record<string, string>;
    readonly scope?: string;
    readonly clientMetadata?: {
      readonly label?: string;
      readonly deviceType?: string;
      readonly os?: string;
    };
  },
) =>
  Effect.gen(function* () {
    const tokenUrl = yield* getHttpServerUrl("/oauth/token");
    const response = yield* fetchEffect(tokenUrl, {
      method: "POST",
      headers: {
        "content-type": "application/x-www-form-urlencoded",
        ...options?.headers,
      },
      body: new URLSearchParams({
        grant_type: AuthTokenExchangeGrantType,
        subject_token: credential,
        subject_token_type: AuthEnvironmentBootstrapTokenType,
        requested_token_type: AuthAccessTokenType,
        scope:
          options?.scope ??
          "orchestration:read orchestration:operate terminal:operate review:write relay:read access:read access:write relay:write",
        ...(options?.clientMetadata?.label ? { client_label: options.clientMetadata.label } : {}),
        ...(options?.clientMetadata?.deviceType
          ? { client_device_type: options.clientMetadata.deviceType }
          : {}),
        ...(options?.clientMetadata?.os ? { client_os: options.clientMetadata.os } : {}),
      }).toString(),
    });
    const body = yield* responseJsonEffect<{
      readonly access_token?: string;
      readonly issued_token_type?: string;
      readonly token_type?: string;
      readonly expires_in?: number;
      readonly scope?: string;
      readonly _tag?: string;
      readonly code?: string;
      readonly reason?: string;
      readonly traceId?: string;
    }>(response);
    return {
      response,
      body,
    };
  });

const makeDpopProof = (input: {
  readonly method: string;
  readonly url: string;
  readonly iat: number;
  readonly accessToken?: string;
  readonly jti?: string;
  readonly privateKey?: NodeCrypto.KeyObject;
  readonly publicJwk?: DpopPublicJwk;
}) => {
  const keyPair =
    input.privateKey && input.publicJwk
      ? { privateKey: input.privateKey, publicJwk: input.publicJwk }
      : (() => {
          const { privateKey, publicKey } = NodeCrypto.generateKeyPairSync("ec", {
            namedCurve: "P-256",
          });
          return { privateKey, publicJwk: publicKey.export({ format: "jwk" }) as DpopPublicJwk };
        })();
  const header = Buffer.from(
    JSON.stringify({
      typ: "dpop+jwt",
      alg: "ES256",
      jwk: keyPair.publicJwk,
    }),
  ).toString("base64url");
  const payload = Buffer.from(
    JSON.stringify({
      htm: input.method,
      htu: input.url,
      jti: input.jti ?? "proof-1",
      iat: input.iat,
      ...(input.accessToken ? { ath: computeDpopAccessTokenHash(input.accessToken) } : {}),
    }),
  ).toString("base64url");
  const signature = NodeCrypto.sign("sha256", Buffer.from(`${header}.${payload}`), {
    key: keyPair.privateKey,
    dsaEncoding: "ieee-p1363",
  }).toString("base64url");
  return {
    proof: `${header}.${payload}.${signature}`,
    thumbprint: computeDpopJwkThumbprint(keyPair.publicJwk),
    privateKey: keyPair.privateKey,
    publicJwk: keyPair.publicJwk,
  };
};

const makeCloudMintCredentialRequest = (input: {
  readonly privateKey: string;
  readonly environmentId: EnvironmentId;
  readonly clientProofKeyThumbprint: string;
  readonly issuer?: string;
  readonly audience?: string;
  readonly subject?: string;
  readonly jti?: string;
  readonly nonce: string;
  readonly issuedAt: string;
  readonly expiresAt: string;
  readonly scope?: ReadonlyArray<"environment:connect">;
}) => {
  const payload = {
    iss: input.issuer ?? "https://relay.example.test",
    aud: input.audience ?? `t3-env:${input.environmentId}`,
    sub: input.subject ?? "user_123",
    jti: input.jti ?? "cloud-mint-jti-1",
    environmentId: input.environmentId,
    clientProofKeyThumbprint: input.clientProofKeyThumbprint,
    cnf: {
      jkt: input.clientProofKeyThumbprint,
    },
    nonce: input.nonce,
    iat: Math.floor(DateTime.makeUnsafe(input.issuedAt).epochMilliseconds / 1_000),
    exp: Math.floor(DateTime.makeUnsafe(input.expiresAt).epochMilliseconds / 1_000),
    scope: input.scope ?? ["environment:connect"],
  } as const;
  const header = Buffer.from(
    JSON.stringify({ alg: "EdDSA", typ: RELAY_MINT_REQUEST_TYP }),
  ).toString("base64url");
  const encodedPayload = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const signingInput = `${header}.${encodedPayload}`;
  return {
    proof: `${signingInput}.${NodeCrypto.sign(null, Buffer.from(signingInput), input.privateKey).toString("base64url")}`,
  };
};

const makeCloudEnvironmentHealthRequest = (input: {
  readonly privateKey: string;
  readonly environmentId: EnvironmentId;
  readonly issuer?: string;
  readonly audience?: string;
  readonly subject?: string;
  readonly jti?: string;
  readonly nonce: string;
  readonly issuedAt: string;
  readonly expiresAt: string;
  readonly scope?: ReadonlyArray<"environment:status">;
}) => {
  const payload = {
    iss: input.issuer ?? "https://relay.example.test",
    aud: input.audience ?? `t3-env:${input.environmentId}`,
    sub: input.subject ?? "user_123",
    jti: input.jti ?? "cloud-health-jti-1",
    environmentId: input.environmentId,
    nonce: input.nonce,
    iat: Math.floor(DateTime.makeUnsafe(input.issuedAt).epochMilliseconds / 1_000),
    exp: Math.floor(DateTime.makeUnsafe(input.expiresAt).epochMilliseconds / 1_000),
    scope: input.scope ?? ["environment:status"],
  } as const;
  const header = Buffer.from(
    JSON.stringify({ alg: "EdDSA", typ: RELAY_HEALTH_REQUEST_TYP }),
  ).toString("base64url");
  const encodedPayload = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const signingInput = `${header}.${encodedPayload}`;
  return {
    proof: `${signingInput}.${NodeCrypto.sign(null, Buffer.from(signingInput), input.privateKey).toString("base64url")}`,
  };
};

const decodeCompactJwtPayload = <A>(token: string): A => {
  const encodedPayload = token.split(".")[1];
  if (!encodedPayload) {
    throw new Error("JWT does not contain a payload.");
  }
  return JSON.parse(Buffer.from(encodedPayload, "base64url").toString("utf8")) as A;
};

class AuthenticationGetterError extends Data.TaggedError("AuthenticationGetterError")<{
  readonly message: string;
}> {}

class TestHttpRequestError extends Data.TaggedError("TestHttpRequestError")<{
  readonly cause: unknown;
}> {}

const testRequestUrl = (input: Parameters<typeof fetch>[0]): string => {
  const value = input.toString();
  if (!/^https?:\/\//i.test(value)) {
    return value;
  }
  const url = new URL(value);
  return `${url.pathname}${url.search}`;
};

const fetchEffect = (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
  const request = HttpClientRequest.make((init?.method ?? "GET") as "GET" | "POST")(
    testRequestUrl(input),
    {
      headers: init?.headers as Record<string, string> | undefined,
    },
  ).pipe(
    typeof init?.body === "string"
      ? HttpClientRequest.bodyText(
          init.body,
          (init.headers as Record<string, string> | undefined)?.["content-type"] ??
            "application/json",
        )
      : (request) => request,
  );
  const effect = HttpClient.execute(request);
  return (
    init?.redirect === "manual"
      ? effect.pipe(Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }))
      : effect
  ).pipe(Effect.mapError((cause) => new TestHttpRequestError({ cause })));
};

const jsonRequestBody = (value: unknown): string => {
  return JSON.stringify(value);
};

const responseJsonEffect = <A>(response: HttpClientResponse.HttpClientResponse) =>
  response.json.pipe(
    Effect.map((json) => json as A),
    Effect.mapError((cause) => new TestHttpRequestError({ cause })),
  );

const responseOk = (response: HttpClientResponse.HttpClientResponse) =>
  response.status >= 200 && response.status < 300;

const getAuthenticatedSessionCookieHeader = (credential = defaultDesktopBootstrapToken) =>
  Effect.gen(function* () {
    const { response, cookie } = yield* bootstrapBrowserSession(credential);
    if (!responseOk(response)) {
      return yield* new AuthenticationGetterError({
        message: `Expected bootstrap session response to succeed, got ${response.status}`,
      });
    }

    if (!cookie) {
      return yield* new AuthenticationGetterError({
        message: "Expected bootstrap session response to set a cookie.",
      });
    }

    return cookie.split(";")[0] ?? cookie;
  });

const getAuthenticatedBearerSessionToken = (credential = defaultDesktopBootstrapToken) =>
  Effect.gen(function* () {
    const { response, body } = yield* exchangeAccessToken(credential);
    if (!responseOk(response)) {
      return yield* new AuthenticationGetterError({
        message: `Expected bearer bootstrap response to succeed, got ${response.status}`,
      });
    }

    if (!body.access_token) {
      return yield* new AuthenticationGetterError({
        message: "Expected token exchange response to include an access token.",
      });
    }

    return body.access_token;
  });

const extractSessionTokenFromSetCookie = (cookieHeader: string): string => {
  const [nameValue] = cookieHeader.split(";", 1);
  const token = nameValue?.split("=", 2)[1];
  if (!token) {
    throw new Error("Expected session cookie header to contain a token value.");
  }
  return token;
};

const splitHeaderTokens = (value: string | null | undefined) =>
  (value ?? "")
    .split(",")
    .map((token) => token.trim())
    .filter((token) => token.length > 0)
    .toSorted();

const assertBrowserApiCorsResponseHeaders = (
  headers: Readonly<Record<string, string | undefined>>,
  options?: {
    readonly origin?: string;
    readonly credentials?: boolean;
  },
) => {
  assert.equal(headers["access-control-allow-origin"], options?.origin ?? "*");
  assert.equal(
    headers["access-control-allow-credentials"],
    options?.credentials ? "true" : undefined,
  );
};

const assertBrowserApiCorsPreflightHeaders = (
  headers: Readonly<Record<string, string | undefined>>,
  options?: {
    readonly origin?: string;
    readonly credentials?: boolean;
  },
) => {
  assertBrowserApiCorsResponseHeaders(headers, options);
  assert.deepEqual(splitHeaderTokens(headers["access-control-allow-methods"] ?? null), [
    "GET",
    "HEAD",
    "OPTIONS",
    "POST",
  ]);
  assert.deepEqual(splitHeaderTokens(headers["access-control-allow-headers"]), [
    "authorization",
    "b3",
    "content-type",
    "dpop",
    "range",
    "traceparent",
  ]);
};
const crossOriginClientOrigin = "http://remote-client.test:3773";

const getWsServerUrl = (
  pathname = "",
  options?: { authenticated?: boolean; credential?: string },
) =>
  Effect.gen(function* () {
    const server = yield* HttpServer.HttpServer;
    const address = server.address as HttpServer.TcpAddress;
    const baseUrl = `ws://127.0.0.1:${address.port}${pathname}`;
    if (options?.authenticated === false) {
      return baseUrl;
    }
    return appendSessionCookieToWsUrl(
      baseUrl,
      yield* getAuthenticatedSessionCookieHeader(options?.credential),
    );
  });

// Mirrors NodeHttpServer.layerTest, which does not expose server options,
// with the production `websocket: { perMessageDeflate: true }` setting.
const NodeHttpServerTestWithWsDeflate = HttpServer.layerTestClient.pipe(
  Layer.provide(
    Layer.fresh(FetchHttpClient.layer).pipe(
      Layer.provide(Layer.succeed(FetchHttpClient.RequestInit)({ keepalive: false })),
    ),
  ),
  Layer.provideMerge(
    Layer.unwrap(
      Effect.map(
        Effect.promise(() => import("node:http")),
        (NodeHttp) =>
          NodeHttpServer.layer(NodeHttp.createServer, {
            port: 0,
            websocket: { perMessageDeflate: true },
          }),
      ),
    ),
  ),
);

it.layer(NodeServices.layer)("server router seam", (it) => {
  it.effect("parks HTTP ingress until command readiness", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const staticDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-router-gate-" });
      yield* fileSystem.writeFileString(path.join(staticDir, "index.html"), "ready");
      const entered = yield* Deferred.make<void>();
      const ready = yield* Deferred.make<void>();
      const completed = yield* Deferred.make<void>();

      yield* buildAppUnderTest({
        config: { staticDir },
        layers: {
          serverRuntimeStartup: {
            awaitCommandReady: Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(ready)),
            ),
          },
        },
      });
      const request = yield* HttpClient.get("/").pipe(
        Effect.tap(() => Deferred.succeed(completed, undefined)),
        Effect.forkChild,
      );
      yield* Deferred.await(entered);
      assert.isFalse(yield* Deferred.isDone(completed));

      yield* Deferred.succeed(ready, undefined);
      assert.equal((yield* Fiber.join(request)).status, 200);
      assert.isTrue(yield* Deferred.isDone(completed));
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("serves static index content for GET / when staticDir is configured", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const staticDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-router-static-" });
      const indexPath = path.join(staticDir, "index.html");
      yield* fileSystem.writeFileString(indexPath, "<html>router-static-ok</html>");

      yield* buildAppUnderTest({ config: { staticDir } });

      const response = yield* HttpClient.get("/");
      assert.equal(response.status, 200);
      assert.include(yield* response.text, "router-static-ok");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("redirects to dev URL when configured", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: { devUrl: new URL("http://127.0.0.1:5173") },
      });

      const url = yield* getHttpServerUrl("/foo/bar?token=test-token");
      const response = yield* fetchEffect(url, { redirect: "manual" });

      assert.equal(response.status, 302);
      assert.equal(response.headers.location, "http://127.0.0.1:5173/foo/bar?token=test-token");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("serves the public environment descriptor without requiring auth", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const url = yield* getHttpServerUrl("/.well-known/t3/environment");
      const response = yield* fetchEffect(url);
      const body = yield* responseJsonEffect<typeof testEnvironmentDescriptor>(response);

      assert.equal(response.status, 200);
      assert.deepEqual(body, testEnvironmentDescriptor);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("compresses large JSON responses through the composed routes", () =>
    Effect.gen(function* () {
      const descriptor = {
        ...testEnvironmentDescriptor,
        label: "Test environment".repeat(100),
      };
      yield* buildAppUnderTest({
        layers: {
          serverEnvironment: {
            getDescriptor: Effect.succeed(descriptor),
          },
        },
      });

      const url = yield* getHttpServerUrl("/.well-known/t3/environment");
      const response = yield* fetchEffect(url, {
        headers: {
          "accept-encoding": "gzip",
        },
      });
      const body = yield* responseJsonEffect<typeof descriptor>(response);

      assert.equal(response.status, 200);
      assert.equal(response.headers["content-encoding"], "gzip");
      assert.equal(response.headers.vary, "Accept-Encoding");
      assert.deepEqual(body, descriptor);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("includes CORS headers on public environment descriptor responses", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const url = yield* getHttpServerUrl("/.well-known/t3/environment");
      const response = yield* fetchEffect(url, {
        headers: {
          origin: crossOriginClientOrigin,
        },
      });
      const body = yield* responseJsonEffect<typeof testEnvironmentDescriptor>(response);

      assert.equal(response.status, 200);
      assertBrowserApiCorsResponseHeaders(response.headers);
      assert.deepEqual(body, testEnvironmentDescriptor);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("reports unauthenticated session state without requiring auth", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const url = yield* getHttpServerUrl("/api/auth/session");
      const response = yield* fetchEffect(url);
      const body = yield* responseJsonEffect<{
        readonly authenticated: boolean;
        readonly auth: {
          readonly policy: string;
          readonly bootstrapMethods: ReadonlyArray<string>;
          readonly sessionMethods: ReadonlyArray<string>;
          readonly sessionCookieName: string;
        };
      }>(response);

      assert.equal(response.status, 200);
      assert.equal(body.authenticated, false);
      assert.equal(body.auth.policy, "desktop-managed-local");
      assert.deepEqual(body.auth.bootstrapMethods, ["desktop-bootstrap"]);
      assert.deepEqual(body.auth.sessionMethods, [
        "browser-session-cookie",
        "bearer-access-token",
        "dpop-access-token",
      ]);
      // Desktop, so port-scoped: instances scan for a free port and share
      // 127.0.0.1, and cookies are not scoped by port.
      assert.isTrue(body.auth.sessionCookieName.startsWith("t3_session_"));
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("bootstraps a browser session and authenticates the session endpoint via cookie", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const {
        response: bootstrapResponse,
        body: bootstrapBody,
        cookie: setCookie,
      } = yield* bootstrapBrowserSession();

      assert.equal(bootstrapResponse.status, 200);
      assert.equal(bootstrapBody.authenticated, true);
      assert.equal(bootstrapBody.sessionMethod, "browser-session-cookie");
      assert.isUndefined((bootstrapBody as { readonly sessionToken?: string }).sessionToken);
      assert.isDefined(setCookie);

      const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
      const sessionResponse = yield* fetchEffect(sessionUrl, {
        headers: {
          cookie: setCookie?.split(";")[0] ?? "",
        },
      });
      const sessionBody = yield* responseJsonEffect<{
        readonly authenticated: boolean;
        readonly sessionMethod?: string;
      }>(sessionResponse);

      assert.equal(sessionResponse.status, 200);
      assert.equal(sessionBody.authenticated, true);
      assert.equal(sessionBody.sessionMethod, "browser-session-cookie");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("exchanges a bootstrap grant for a scoped bearer access token", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const { response: tokenResponse, body: tokenBody } = yield* exchangeAccessToken();

      assert.equal(tokenResponse.status, 200);
      assert.equal(tokenBody.issued_token_type, AuthAccessTokenType);
      assert.equal(tokenBody.token_type, "Bearer");
      assert.equal(
        tokenBody.scope,
        "orchestration:read orchestration:operate terminal:operate review:write relay:read access:read access:write relay:write",
      );
      assert.equal(typeof tokenBody.access_token, "string");

      const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
      const sessionResponse = yield* fetchEffect(sessionUrl, {
        headers: {
          authorization: `Bearer ${tokenBody.access_token ?? ""}`,
        },
      });
      const sessionBody = yield* responseJsonEffect<{
        readonly authenticated: boolean;
        readonly sessionMethod?: string;
        readonly scopes?: ReadonlyArray<string>;
      }>(sessionResponse);

      assert.equal(sessionResponse.status, 200);
      assert.equal(sessionBody.authenticated, true);
      assert.equal(sessionBody.sessionMethod, "bearer-access-token");
      assert.deepEqual(sessionBody.scopes, [
        "orchestration:read",
        "orchestration:operate",
        "terminal:operate",
        "review:write",
        "relay:read",
        "access:read",
        "access:write",
        "relay:write",
      ]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("persists token exchange client display metadata for authorized-client listings", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          host: "0.0.0.0",
        },
      });

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const pairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const pairingBody = (yield* pairingResponse.json) as {
        readonly credential: string;
      };

      const { response } = yield* exchangeAccessToken(pairingBody.credential, {
        headers: {
          "user-agent": "undici",
        },
        scope: "orchestration:read orchestration:operate terminal:operate review:write",
        clientMetadata: {
          label: "Scient Mobile",
          deviceType: "mobile",
          os: "iOS",
        },
      });

      const clientsResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: {
          cookie: ownerCookie,
        },
      });
      const clients = (yield* clientsResponse.json) as ReadonlyArray<{
        readonly current: boolean;
        readonly client: {
          readonly label?: string;
          readonly deviceType: string;
          readonly ipAddress?: string;
          readonly os?: string;
          readonly userAgent?: string;
        };
      }>;
      const mobileClient = clients.find((client) => !client.current);

      assert.equal(pairingResponse.status, 200);
      assert.equal(response.status, 200);
      assert.equal(clientsResponse.status, 200);
      assert.deepInclude(mobileClient?.client, {
        label: "Scient Mobile",
        deviceType: "mobile",
        os: "iOS",
        ipAddress: "127.0.0.1",
        userAgent: "undici",
      });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "exchanges a bootstrap credential for a DPoP-bound access token without bearer downgrade",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
        const credentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
          headers: { cookie: ownerCookie },
          body: yield* HttpBody.json({}),
        });
        const credential = (yield* credentialResponse.json) as { readonly credential: string };
        const tokenUrl = yield* getHttpServerUrl("/oauth/token");
        const now = yield* DateTime.now;
        const tokenProof = makeDpopProof({
          method: "POST",
          url: tokenUrl,
          iat: Math.floor(now.epochMilliseconds / 1_000),
          jti: "token-exchange-proof",
        });
        const tokenResponse = yield* fetchEffect(tokenUrl, {
          method: "POST",
          headers: {
            "content-type": "application/x-www-form-urlencoded",
            dpop: tokenProof.proof,
          },
          body: new URLSearchParams({
            grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
            subject_token: credential.credential,
            subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
            requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
            scope: "orchestration:read orchestration:operate terminal:operate review:write",
          }).toString(),
        });
        const token = yield* responseJsonEffect<{
          readonly access_token: string;
          readonly token_type: string;
        }>(tokenResponse);

        assert.equal(tokenResponse.status, 200);
        assert.equal(tokenResponse.headers["cache-control"], "no-store");
        assert.equal(token.token_type, "DPoP");

        const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
        const bearerResponse = yield* fetchEffect(sessionUrl, {
          headers: { authorization: `Bearer ${token.access_token}` },
        });
        const bearerState = yield* responseJsonEffect<{ readonly authenticated: boolean }>(
          bearerResponse,
        );
        assert.equal(bearerState.authenticated, false);

        const sessionProof = makeDpopProof({
          method: "GET",
          url: sessionUrl,
          iat: Math.floor(now.epochMilliseconds / 1_000),
          jti: "session-proof",
          accessToken: token.access_token,
          privateKey: tokenProof.privateKey,
          publicJwk: tokenProof.publicJwk,
        });
        const dpopResponse = yield* fetchEffect(sessionUrl, {
          headers: {
            authorization: `DPoP ${token.access_token}`,
            dpop: sessionProof.proof,
          },
        });
        const dpopState = yield* responseJsonEffect<{
          readonly authenticated: boolean;
          readonly sessionMethod?: string;
        }>(dpopResponse);
        assert.equal(dpopState.authenticated, true);
        assert.equal(dpopState.sessionMethod, "dpop-access-token");
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects replayed DPoP proofs across token exchanges", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const firstCredentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const firstCredential = (yield* firstCredentialResponse.json) as {
        readonly credential: string;
      };
      const secondCredentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const secondCredential = (yield* secondCredentialResponse.json) as {
        readonly credential: string;
      };
      const tokenUrl = yield* getHttpServerUrl("/oauth/token");
      const now = yield* DateTime.now;
      const dpop = makeDpopProof({
        method: "POST",
        url: tokenUrl,
        iat: Math.floor(now.epochMilliseconds / 1_000),
      });

      const firstBootstrap = yield* exchangeAccessToken(firstCredential.credential, {
        headers: {
          dpop: dpop.proof,
        },
        scope: "orchestration:read orchestration:operate terminal:operate review:write",
      });
      const replayBootstrap = yield* exchangeAccessToken(secondCredential.credential, {
        headers: {
          dpop: dpop.proof,
        },
        scope: "orchestration:read orchestration:operate terminal:operate review:write",
      });

      assert.equal(firstBootstrap.response.status, 200);
      assert.equal(replayBootstrap.response.status, 401);
      assert.equal(replayBootstrap.body._tag, "EnvironmentAuthInvalidError");
      assert.equal(replayBootstrap.body.code, "auth_invalid");
      assert.equal(replayBootstrap.body.reason, "invalid_credential");
      assert.equal(typeof replayBootstrap.body.traceId, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("ignores forwarded host headers when validating token exchange DPoP URLs", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const credentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const credential = (yield* credentialResponse.json) as {
        readonly credential: string;
      };
      const tokenUrl = yield* getHttpServerUrl("/oauth/token");
      const now = yield* DateTime.now;
      const dpop = makeDpopProof({
        method: "POST",
        url: tokenUrl,
        iat: Math.floor(now.epochMilliseconds / 1_000),
      });

      const bootstrap = yield* exchangeAccessToken(credential.credential, {
        headers: {
          dpop: dpop.proof,
          "x-forwarded-host": "environment.example.test",
        },
        scope: "orchestration:read orchestration:operate terminal:operate review:write",
      });

      assert.equal(bootstrap.response.status, 200);
      assert.equal(bootstrap.body.token_type, "DPoP");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects token exchange DPoP proofs bound to spoofed forwarded hosts", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const credentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const credential = (yield* credentialResponse.json) as {
        readonly credential: string;
      };
      const tokenUrl = yield* getHttpServerUrl("/oauth/token");
      const spoofedUrl = new URL(tokenUrl);
      spoofedUrl.hostname = "environment.example.test";
      const now = yield* DateTime.now;
      const dpop = makeDpopProof({
        method: "POST",
        url: spoofedUrl.href,
        iat: Math.floor(now.epochMilliseconds / 1_000),
      });

      const bootstrap = yield* exchangeAccessToken(credential.credential, {
        headers: {
          dpop: dpop.proof,
          "x-forwarded-host": spoofedUrl.host,
        },
        scope: "orchestration:read orchestration:operate terminal:operate review:write",
      });

      assert.equal(bootstrap.response.status, 401);
      assert.equal(bootstrap.body._tag, "EnvironmentAuthInvalidError");
      assert.equal(bootstrap.body.code, "auth_invalid");
      assert.equal(bootstrap.body.reason, "invalid_credential");
      assert.equal(typeof bootstrap.body.traceId, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud link proofs for non-loopback managed endpoint origins", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
      const linkProofResponse = yield* fetchEffect(linkProofUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          challenge: "relay-link-challenge",
          relayIssuer: "https://relay.example.test",
          endpoint: {
            httpBaseUrl: "https://environment.example.test/",
            wsBaseUrl: "wss://environment.example.test/ws",
            providerKind: "manual",
          },
          origin: {
            localHttpHost: "192.168.1.42",
            localHttpPort: 3773,
          },
        }),
      });
      const body = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(linkProofResponse);

      assert.equal(linkProofResponse.status, 400);
      assert.equal(body._tag, "EnvironmentHttpBadRequestError");
      assert.equal(body.message, "Invalid managed endpoint origin.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud link proofs for unsupported endpoint providers", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
      const serverPort = Number(new URL(linkProofUrl).port);
      const linkProofResponse = yield* fetchEffect(linkProofUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          challenge: "relay-link-challenge",
          relayIssuer: "https://relay.example.test",
          endpoint: {
            httpBaseUrl: linkProofUrl.replace("/api/connect/link-proof", ""),
            wsBaseUrl: linkProofUrl
              .replace("http://", "ws://")
              .replace("/api/connect/link-proof", "/ws"),
            // "manual" and "cloudflare_tunnel" are supported; "t3_relay" is not.
            providerKind: "t3_relay",
          },
          origin: {
            localHttpHost: "127.0.0.1",
            localHttpPort: serverPort,
          },
        }),
      });
      const body = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(linkProofResponse);

      assert.equal(linkProofResponse.status, 400);
      assert.equal(body._tag, "EnvironmentHttpBadRequestError");
      assert.equal(body.message, "Invalid managed endpoint origin.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud link proofs requested through a public managed endpoint", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
      const serverPort = Number(new URL(linkProofUrl).port);
      const linkProofResponse = yield* HttpClient.post("/api/connect/link-proof", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
          "content-type": "application/json",
          host: "environment.example.test",
          "x-forwarded-host": "environment.example.test",
          "x-forwarded-proto": "https",
        },
        body: HttpBody.text(
          jsonRequestBody({
            challenge: "relay-link-challenge",
            relayIssuer: "https://relay.example.test",
            endpoint: {
              httpBaseUrl: "https://environment.example.test/",
              wsBaseUrl: "wss://environment.example.test/ws",
              providerKind: "manual",
            },
            origin: {
              localHttpHost: "127.0.0.1",
              localHttpPort: serverPort,
            },
          }),
          "application/json",
        ),
      });
      const body = (yield* linkProofResponse.json) as {
        readonly _tag?: string;
        readonly message?: string;
      };

      assert.equal(linkProofResponse.status, 400);
      assert.equal(body._tag, "EnvironmentHttpBadRequestError");
      assert.equal(body.message, "Invalid managed endpoint origin.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "rejects cloud link proofs when a public request spoofs loopback forwarded headers",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
        const serverPort = Number(new URL(linkProofUrl).port);
        const linkProofResponse = yield* HttpClient.post("/api/connect/link-proof", {
          headers: {
            cookie: yield* getAuthenticatedSessionCookieHeader(),
            "content-type": "application/json",
            host: "environment.example.test",
            "x-forwarded-host": `127.0.0.1:${serverPort}`,
            "x-forwarded-proto": "http",
          },
          body: HttpBody.text(
            jsonRequestBody({
              challenge: "relay-link-challenge",
              relayIssuer: "https://relay.example.test",
              endpoint: {
                httpBaseUrl: "https://environment.example.test/",
                wsBaseUrl: "wss://environment.example.test/ws",
                providerKind: "manual",
              },
              origin: {
                localHttpHost: "127.0.0.1",
                localHttpPort: serverPort,
              },
            }),
            "application/json",
          ),
        });
        const body = (yield* linkProofResponse.json) as {
          readonly _tag?: string;
          readonly message?: string;
        };

        assert.equal(linkProofResponse.status, 400);
        assert.equal(body._tag, "EnvironmentHttpBadRequestError");
        assert.equal(body.message, "Invalid managed endpoint origin.");
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud link proofs with malformed forwarded request hosts", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
      const serverPort = Number(new URL(linkProofUrl).port);
      const linkProofResponse = yield* HttpClient.post("/api/connect/link-proof", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
          "content-type": "application/json",
          host: "bad host",
          "x-forwarded-host": "bad host",
          "x-forwarded-proto": "https",
        },
        body: HttpBody.text(
          jsonRequestBody({
            challenge: "relay-link-challenge",
            relayIssuer: "https://relay.example.test",
            endpoint: {
              httpBaseUrl: "https://environment.example.test/",
              wsBaseUrl: "wss://environment.example.test/ws",
              providerKind: "manual",
            },
            origin: {
              localHttpHost: "127.0.0.1",
              localHttpPort: serverPort,
            },
          }),
          "application/json",
        ),
      });
      const body = (yield* linkProofResponse.json) as {
        readonly _tag?: string;
        readonly message?: string;
      };

      assert.equal(linkProofResponse.status, 400);
      assert.equal(body._tag, "EnvironmentHttpBadRequestError");
      assert.equal(body.message, "Invalid managed endpoint origin.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects local cloud link proofs for a different loopback port", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
      const serverPort = Number(new URL(linkProofUrl).port);
      const linkProofResponse = yield* fetchEffect(linkProofUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          challenge: "relay-link-challenge",
          relayIssuer: "https://relay.example.test",
          endpoint: {
            httpBaseUrl: "https://environment.example.test/",
            wsBaseUrl: "wss://environment.example.test/ws",
            providerKind: "manual",
          },
          origin: {
            localHttpHost: "127.0.0.1",
            localHttpPort: serverPort === 65_535 ? serverPort - 1 : serverPort + 1,
          },
        }),
      });
      const body = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(linkProofResponse);

      assert.equal(linkProofResponse.status, 400);
      assert.equal(body._tag, "EnvironmentHttpBadRequestError");
      assert.equal(body.message, "Invalid managed endpoint origin.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("allows standard clients to read managed relay configuration state", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const credentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: { cookie: ownerCookie },
        body: yield* HttpBody.json({}),
      });
      const credential = (yield* credentialResponse.json) as { readonly credential: string };
      const pairedCookie = yield* getAuthenticatedSessionCookieHeader(credential.credential);
      const linkStateUrl = yield* getHttpServerUrl("/api/connect/link-state");
      const response = yield* fetchEffect(linkStateUrl, {
        headers: { cookie: pairedCookie },
      });
      const body = yield* responseJsonEffect<{
        readonly linked?: boolean;
        readonly publishAgentActivity?: boolean;
      }>(response);

      assert.equal(response.status, 200);
      assert.equal(body.linked, false);
      assert.equal(body.publishAgentActivity, false);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "reports relay client status and streams installation progress over environment RPC",
    () =>
      Effect.gen(function* () {
        const installedRelayClient = {
          status: "available" as const,
          executablePath: "/tmp/t3/tools/cloudflared",
          source: "managed" as const,
          version: RelayClient.CLOUDFLARED_VERSION,
        };
        yield* buildAppUnderTest({
          layers: {
            relayClient: {
              resolve: Effect.succeed({
                status: "missing",
                version: RelayClient.CLOUDFLARED_VERSION,
              }),
              install: Effect.succeed(installedRelayClient),
              installWithProgress: (report) =>
                report({ type: "progress", stage: "checking" }).pipe(
                  Effect.andThen(report({ type: "progress", stage: "downloading" })),
                  Effect.as(installedRelayClient),
                ),
            },
          },
        });

        const wsUrl = yield* getWsServerUrl("/ws");
        const status = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) => client[WS_METHODS.cloudGetRelayClientStatus]({})),
        );
        const installEvents = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.cloudInstallRelayClient]({}).pipe(Stream.runCollect),
          ),
        );

        assert.equal(status.status, "missing");
        assert.deepEqual(Array.from(installEvents), [
          { type: "progress", stage: "checking" },
          { type: "progress", stage: "downloading" },
          { type: "complete", status: installedRelayClient },
        ]);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("requires relay write scope to update agent activity publication", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const preferencesUrl = yield* getHttpServerUrl("/api/connect/preferences");
      const ownerResponse = yield* fetchEffect(preferencesUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({ publishAgentActivity: true }),
      });
      const ownerBody = yield* responseJsonEffect<{
        readonly publishAgentActivity?: boolean;
      }>(ownerResponse);
      assert.equal(ownerResponse.status, 200);
      assert.equal(ownerBody.publishAgentActivity, true);

      const credentialResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: { cookie: ownerCookie },
        body: yield* HttpBody.json({}),
      });
      const credential = (yield* credentialResponse.json) as { readonly credential: string };
      const pairedCookie = yield* getAuthenticatedSessionCookieHeader(credential.credential);
      const pairedResponse = yield* fetchEffect(preferencesUrl, {
        method: "POST",
        headers: {
          cookie: pairedCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({ publishAgentActivity: false }),
      });
      const pairedBody = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly requiredScope?: string;
      }>(pairedResponse);
      assert.equal(pairedResponse.status, 403);
      assert.equal(pairedBody._tag, "EnvironmentScopeRequiredError");
      assert.equal(pairedBody.requiredScope, "relay:write");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects relay config with an invalid cloud mint public key", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: "not-a-public-key",
          endpointRuntime: null,
        }),
      });
      const body = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(relayConfigResponse);

      assert.equal(relayConfigResponse.status, 400);
      assert.equal(body._tag, "EnvironmentHttpBadRequestError");
      assert.equal(body.message, "Cloud mint public key must be a valid Ed25519 public key.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects relay config with insecure relay metadata or empty credentials", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const postRelayConfig = (body: {
        readonly relayUrl: string;
        readonly relayIssuer?: string;
        readonly cloudUserId: string;
        readonly environmentCredential: string;
      }) =>
        fetchEffect(relayConfigUrl, {
          method: "POST",
          headers: {
            cookie: ownerCookie,
            "content-type": "application/json",
          },
          body: jsonRequestBody({
            ...body,
            cloudMintPublicKey: cloudKeyPair.publicKey,
            endpointRuntime: null,
          }),
        });

      const insecureRelayUrl = yield* postRelayConfig({
        relayUrl: "http://relay.example.test",
        cloudUserId: "user_123",
        environmentCredential: "t3env_test_credential",
      });
      const insecureRelayIssuer = yield* postRelayConfig({
        relayUrl: "https://relay.example.test",
        cloudUserId: "user_123",
        relayIssuer: "http://relay.example.test",
        environmentCredential: "t3env_test_credential",
      });
      const nonOriginRelayUrl = yield* postRelayConfig({
        relayUrl: "https://relay.example.test/path",
        cloudUserId: "user_123",
        environmentCredential: "t3env_test_credential",
      });
      const emptyCredential = yield* postRelayConfig({
        relayUrl: "https://relay.example.test",
        cloudUserId: "user_123",
        environmentCredential: "   ",
      });
      const insecureRelayUrlBody = yield* responseJsonEffect<{ readonly message?: string }>(
        insecureRelayUrl,
      );
      const insecureRelayIssuerBody = yield* responseJsonEffect<{ readonly message?: string }>(
        insecureRelayIssuer,
      );
      const nonOriginRelayUrlBody = yield* responseJsonEffect<{ readonly message?: string }>(
        nonOriginRelayUrl,
      );
      const emptyCredentialBody = yield* responseJsonEffect<{ readonly message?: string }>(
        emptyCredential,
      );

      assert.equal(insecureRelayUrl.status, 400);
      assert.equal(insecureRelayUrlBody.message, "Relay URL must be a secure absolute HTTPS URL.");
      assert.equal(insecureRelayIssuer.status, 400);
      assert.equal(
        insecureRelayIssuerBody.message,
        "Relay issuer must be a secure absolute HTTPS URL.",
      );
      assert.equal(nonOriginRelayUrl.status, 400);
      assert.equal(nonOriginRelayUrlBody.message, "Relay URL must be a secure absolute HTTPS URL.");
      assert.equal(emptyCredential.status, 400);
      assert.equal(emptyCredentialBody.message, "Relay environment credential is required.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects relay config replacement from a different cloud account", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const postRelayConfig = (cloudUserId: string, environmentCredential: string) =>
        fetchEffect(relayConfigUrl, {
          method: "POST",
          headers: {
            cookie: ownerCookie,
            "content-type": "application/json",
          },
          body: jsonRequestBody({
            relayUrl: "https://relay.example.test",
            cloudUserId,
            environmentCredential,
            cloudMintPublicKey: cloudKeyPair.publicKey,
            endpointRuntime: null,
          }),
        });

      const firstResponse = yield* postRelayConfig("user_123", "t3env_first_credential");
      const replacementResponse = yield* postRelayConfig("user_456", "t3env_second_credential");
      const replacementBody = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(replacementResponse);

      assert.equal(firstResponse.status, 200);
      assert.equal(replacementResponse.status, 409);
      assert.equal(replacementBody._tag, "EnvironmentHttpConflictError");
      assert.equal(
        replacementBody.message,
        "This environment is already linked to a different cloud account. Unlink it before switching accounts.",
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("reports local cloud link state from persisted relay config", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const linkStateUrl = yield* getHttpServerUrl("/api/connect/link-state");
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");

      const initialResponse = yield* fetchEffect(linkStateUrl, {
        headers: {
          cookie: ownerCookie,
        },
      });
      const initialBody = yield* responseJsonEffect<{
        readonly linked?: boolean;
        readonly cloudUserId?: string | null;
      }>(initialResponse);
      assert.equal(initialResponse.status, 200);
      assert.equal(initialBody.linked, false);
      assert.equal(initialBody.cloudUserId, null);

      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://transport.example.test",
          relayIssuer: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const linkedResponse = yield* fetchEffect(linkStateUrl, {
        headers: {
          cookie: ownerCookie,
        },
      });
      const linkedBody = yield* responseJsonEffect<{
        readonly linked?: boolean;
        readonly cloudUserId?: string | null;
        readonly relayUrl?: string | null;
        readonly relayIssuer?: string | null;
      }>(linkedResponse);

      assert.equal(linkedResponse.status, 200);
      assert.equal(linkedBody.linked, true);
      assert.equal(linkedBody.cloudUserId, "user_123");
      assert.equal(linkedBody.relayUrl, "https://transport.example.test");
      assert.equal(linkedBody.relayIssuer, "https://relay.example.test");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("does not expose internal cloud reconciliation over HTTP", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const reconcileUrl = yield* getHttpServerUrl("/api/connect/reconcile");
      const response = yield* fetchEffect(reconcileUrl, {
        method: "POST",
      });

      assert.equal(response.status, 404);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("unlinks local cloud state and disables the managed endpoint runtime", () =>
    Effect.gen(function* () {
      const appliedRuntimeConfigs: Array<unknown> = [];
      yield* buildAppUnderTest({
        layers: {
          cloudManagedEndpointRuntime: {
            applyConfig: (config) => {
              appliedRuntimeConfigs.push(config);
              if (!config) {
                return Effect.succeed({ status: "disabled" });
              }
              return Effect.succeed({
                status: "running",
                providerKind: "cloudflare_tunnel",
                pid: 123,
                ...(config.tunnelId ? { tunnelId: config.tunnelId } : {}),
                ...(config.tunnelName ? { tunnelName: config.tunnelName } : {}),
              });
            },
          },
        },
      });

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const unlinkUrl = yield* getHttpServerUrl("/api/connect/unlink");
      const linkStateUrl = yield* getHttpServerUrl("/api/connect/link-state");

      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://transport.example.test",
          relayIssuer: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: {
            providerKind: "cloudflare_tunnel",
            connectorToken: "connector-token",
            tunnelId: "tunnel-id",
            tunnelName: "tunnel-name",
          },
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const unlinkResponse = yield* fetchEffect(unlinkUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
        },
      });
      const unlinkBody = yield* responseJsonEffect<{
        readonly ok?: boolean;
        readonly endpointRuntimeStatus?: { readonly status?: string };
      }>(unlinkResponse);
      assert.equal(unlinkResponse.status, 200);
      assert.equal(unlinkBody.ok, true);
      assert.equal(unlinkBody.endpointRuntimeStatus?.status, "disabled");

      const linkStateResponse = yield* fetchEffect(linkStateUrl, {
        headers: {
          cookie: ownerCookie,
        },
      });
      const linkStateBody = yield* responseJsonEffect<{
        readonly linked?: boolean;
        readonly cloudUserId?: string | null;
        readonly relayUrl?: string | null;
        readonly relayIssuer?: string | null;
      }>(linkStateResponse);
      assert.equal(linkStateResponse.status, 200);
      assert.equal(linkStateBody.linked, false);
      assert.equal(linkStateBody.cloudUserId, null);
      assert.equal(linkStateBody.relayUrl, null);
      assert.equal(linkStateBody.relayIssuer, null);
      assert.deepEqual(appliedRuntimeConfigs, [
        {
          providerKind: "cloudflare_tunnel",
          connectorToken: "connector-token",
          tunnelId: "tunnel-id",
          tunnelName: "tunnel-name",
        },
        null,
      ]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects replayed cloud mint requests atomically", () => {
    // This is a cloud protocol regression test; opt the test process into
    // the otherwise-disabled D4 cloud route explicitly.
    const previousCloudEnv = {
      enabled: process.env.SCIENT_NEXT_CLOUD_ENABLED,
      relay: process.env.T3CODE_RELAY_URL,
      clerk: process.env.T3CODE_CLERK_PUBLISHABLE_KEY,
      oauth: process.env.T3CODE_CLERK_CLI_OAUTH_CLIENT_ID,
    };
    const restoreCloudEnv = Effect.sync(() => {
      for (const [name, value] of [
        ["SCIENT_NEXT_CLOUD_ENABLED", previousCloudEnv.enabled],
        ["T3CODE_RELAY_URL", previousCloudEnv.relay],
        ["T3CODE_CLERK_PUBLISHABLE_KEY", previousCloudEnv.clerk],
        ["T3CODE_CLERK_CLI_OAUTH_CLIENT_ID", previousCloudEnv.oauth],
      ] as const) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    });
    return Effect.gen(function* () {
      process.env.SCIENT_NEXT_CLOUD_ENABLED = "true";
      process.env.T3CODE_RELAY_URL = "https://relay.example.test";
      process.env.T3CODE_CLERK_PUBLISHABLE_KEY = "pk_test_example";
      process.env.T3CODE_CLERK_CLI_OAUTH_CLIENT_ID = "oauth_test";
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const request = makeCloudMintCredentialRequest({
        privateKey: cloudKeyPair.privateKey,
        environmentId: testEnvironmentDescriptor.environmentId,
        clientProofKeyThumbprint: "client-proof-key-thumbprint",
        nonce: "cloud-mint-nonce-1",
        issuedAt: DateTime.formatIso(now),
        expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
      });
      const mintUrl = yield* getHttpServerUrl("/api/connect/mint-credential");
      const postMint = () =>
        fetchEffect(mintUrl, {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: jsonRequestBody(request),
        });

      const firstResponse = yield* postMint();
      const replayResponse = yield* postMint();
      const replayBody = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(replayResponse);

      assert.equal(firstResponse.status, 200);
      assert.equal(replayResponse.status, 409);
      assert.equal(replayBody._tag, "EnvironmentHttpConflictError");
      assert.equal(replayBody.message, "Cloud mint request was already consumed.");
    }).pipe(Effect.ensuring(restoreCloudEnv), Effect.provide(NodeHttpServer.layerTest));
  });

  it.effect("serves the documented T3 Connect mint credential endpoint", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const request = makeCloudMintCredentialRequest({
        privateKey: cloudKeyPair.privateKey,
        environmentId: testEnvironmentDescriptor.environmentId,
        clientProofKeyThumbprint: "client-proof-key-thumbprint",
        jti: "cloud-mint-jti-documented-endpoint",
        nonce: "cloud-mint-nonce-documented-endpoint",
        issuedAt: DateTime.formatIso(now),
        expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
      });
      const mintUrl = yield* getHttpServerUrl("/api/t3-connect/mint-credential");
      const response = yield* fetchEffect(mintUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(request),
      });

      assert.equal(response.status, 200);
      const body = yield* responseJsonEffect<{
        readonly credential?: string;
        readonly proof?: string;
      }>(response);
      assert.equal(typeof body.credential, "string");
      assert.equal(typeof body.proof, "string");
      assert.equal(
        decodeCompactJwtPayload<{ readonly requestNonce?: string }>(body.proof!).requestNonce,
        "cloud-mint-nonce-documented-endpoint",
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("serves signed T3 Connect environment health checks", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const request = makeCloudEnvironmentHealthRequest({
        privateKey: cloudKeyPair.privateKey,
        environmentId: testEnvironmentDescriptor.environmentId,
        jti: "cloud-health-jti-documented-endpoint",
        nonce: "cloud-health-nonce-documented-endpoint",
        issuedAt: DateTime.formatIso(now),
        expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
      });
      const healthUrl = yield* getHttpServerUrl("/api/t3-connect/health");
      const response = yield* fetchEffect(healthUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(request),
      });

      assert.equal(response.status, 200);
      const body = yield* responseJsonEffect<{
        readonly status?: string;
        readonly descriptor?: { readonly environmentId?: string };
        readonly proof?: string;
      }>(response);
      assert.equal(body.status, "online");
      assert.equal(body.descriptor?.environmentId, testEnvironmentDescriptor.environmentId);
      assert.equal(typeof body.proof, "string");
      assert.equal(
        decodeCompactJwtPayload<{ readonly requestNonce?: string }>(body.proof!).requestNonce,
        "cloud-health-nonce-documented-endpoint",
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects replayed cloud health requests atomically", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const request = makeCloudEnvironmentHealthRequest({
        privateKey: cloudKeyPair.privateKey,
        environmentId: testEnvironmentDescriptor.environmentId,
        jti: "cloud-health-jti-replay",
        nonce: "cloud-health-nonce-replay",
        issuedAt: DateTime.formatIso(now),
        expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
      });
      const healthUrl = yield* getHttpServerUrl("/api/t3-connect/health");
      const postHealth = () =>
        fetchEffect(healthUrl, {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: jsonRequestBody(request),
        });

      const firstResponse = yield* postHealth();
      const replayResponse = yield* postHealth();
      const replayBody = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly message?: string;
      }>(replayResponse);

      assert.equal(firstResponse.status, 200);
      assert.equal(replayResponse.status, 409);
      assert.equal(replayBody._tag, "EnvironmentHttpConflictError");
      assert.equal(replayBody.message, "Cloud health request was already consumed.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "validates cloud proofs against the configured relay issuer, not the transport URL",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
          privateKeyEncoding: { format: "pem", type: "pkcs8" },
          publicKeyEncoding: { format: "pem", type: "spki" },
        });
        const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
        const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
        const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
          method: "POST",
          headers: {
            cookie: ownerCookie,
            "content-type": "application/json",
          },
          body: jsonRequestBody({
            relayUrl: "https://transport.example.test",
            cloudUserId: "user_123",
            relayIssuer: "https://relay.example.test",
            environmentCredential: "t3env_test_credential",
            cloudMintPublicKey: cloudKeyPair.publicKey,
            endpointRuntime: null,
          }),
        });
        assert.equal(relayConfigResponse.status, 200);

        const now = yield* DateTime.now;
        const mintUrl = yield* getHttpServerUrl("/api/t3-connect/mint-credential");
        const postMint = (request: ReturnType<typeof makeCloudMintCredentialRequest>) =>
          fetchEffect(mintUrl, {
            method: "POST",
            headers: {
              "content-type": "application/json",
            },
            body: jsonRequestBody(request),
          });

        const acceptedResponse = yield* postMint(
          makeCloudMintCredentialRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            clientProofKeyThumbprint: "client-proof-key-thumbprint",
            issuer: "https://relay.example.test",
            jti: "cloud-mint-jti-explicit-relay-issuer",
            nonce: "cloud-mint-nonce-explicit-relay-issuer",
            issuedAt: DateTime.formatIso(now),
            expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
          }),
        );
        const rejectedResponse = yield* postMint(
          makeCloudMintCredentialRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            clientProofKeyThumbprint: "client-proof-key-thumbprint",
            issuer: "https://transport.example.test",
            jti: "cloud-mint-jti-transport-url",
            nonce: "cloud-mint-nonce-transport-url",
            issuedAt: DateTime.formatIso(now),
            expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
          }),
        );

        assert.equal(acceptedResponse.status, 200);
        assert.equal(rejectedResponse.status, 401);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("fails relay config when the managed endpoint connector cannot start", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        layers: {
          cloudManagedEndpointRuntime: {
            applyConfig: () =>
              Effect.succeed({
                status: "failed",
                providerKind: "cloudflare_tunnel",
                reason: "cloudflared missing",
                tunnelId: "tunnel-1",
              }),
          },
        },
      });

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: {
            providerKind: "cloudflare_tunnel",
            connectorToken: "connector-token",
            tunnelId: "tunnel-1",
          },
        }),
      });

      assert.equal(relayConfigResponse.status, 503);
      const relayConfigBody = yield* responseJsonEffect<{
        _tag?: string;
        message?: string;
        endpointRuntimeStatus?: { status?: string; reason?: string };
      }>(relayConfigResponse);
      assert.equal(relayConfigBody._tag, "EnvironmentCloudEndpointUnavailableError");
      assert.equal(relayConfigBody.message, "Managed endpoint runtime could not be started.");
      assert.equal(relayConfigBody.endpointRuntimeStatus?.status, "failed");
      assert.equal(relayConfigBody.endpointRuntimeStatus?.reason, "cloudflared missing");

      const now = yield* DateTime.now;
      const healthRequest = makeCloudEnvironmentHealthRequest({
        privateKey: cloudKeyPair.privateKey,
        environmentId: testEnvironmentDescriptor.environmentId,
        nonce: "cloud-health-after-failed-runtime",
        issuedAt: DateTime.formatIso(now),
        expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
      });
      const healthUrl = yield* getHttpServerUrl("/api/t3-connect/health");
      const healthResponse = yield* fetchEffect(healthUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(healthRequest),
      });
      const healthBody = yield* responseJsonEffect<{
        _tag?: string;
        message?: string;
      }>(healthResponse);
      assert.equal(healthResponse.status, 500);
      assert.equal(healthBody._tag, "EnvironmentHttpInternalServerError");
      assert.equal(
        healthBody.message,
        "Cloud mint public key is not installed for this environment.",
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud mint requests with the wrong issuer or audience", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test/",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const mintUrl = yield* getHttpServerUrl("/api/connect/mint-credential");
      const postMint = (request: ReturnType<typeof makeCloudMintCredentialRequest>) =>
        fetchEffect(mintUrl, {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: jsonRequestBody(request),
        });

      const wrongIssuer = yield* postMint(
        makeCloudMintCredentialRequest({
          privateKey: cloudKeyPair.privateKey,
          environmentId: testEnvironmentDescriptor.environmentId,
          clientProofKeyThumbprint: "client-proof-key-thumbprint",
          issuer: "https://attacker.example.test",
          jti: "cloud-mint-jti-wrong-issuer",
          nonce: "cloud-mint-nonce-wrong-issuer",
          issuedAt: DateTime.formatIso(now),
          expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
        }),
      );
      const wrongAudience = yield* postMint(
        makeCloudMintCredentialRequest({
          privateKey: cloudKeyPair.privateKey,
          environmentId: testEnvironmentDescriptor.environmentId,
          clientProofKeyThumbprint: "client-proof-key-thumbprint",
          audience: "t3-env:other-environment",
          jti: "cloud-mint-jti-wrong-audience",
          nonce: "cloud-mint-nonce-wrong-audience",
          issuedAt: DateTime.formatIso(now),
          expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
        }),
      );

      assert.equal(wrongIssuer.status, 401);
      assert.equal(wrongAudience.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud mint requests for a cloud subject other than the linked user", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test/",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const mintUrl = yield* getHttpServerUrl("/api/t3-connect/mint-credential");
      const response = yield* fetchEffect(mintUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(
          makeCloudMintCredentialRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            clientProofKeyThumbprint: "client-proof-key-thumbprint",
            subject: "user_other",
            jti: "cloud-mint-jti-wrong-subject",
            nonce: "cloud-mint-nonce-wrong-subject",
            issuedAt: DateTime.formatIso(now),
            expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
          }),
        ),
      });

      assert.equal(response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud mint requests without the exact connect scope", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test/",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const mintUrl = yield* getHttpServerUrl("/api/t3-connect/mint-credential");
      const response = yield* fetchEffect(mintUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(
          makeCloudMintCredentialRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            clientProofKeyThumbprint: "client-proof-key-thumbprint",
            jti: "cloud-mint-jti-duplicate-scope",
            nonce: "cloud-mint-nonce-duplicate-scope",
            issuedAt: DateTime.formatIso(now),
            expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
            scope: ["environment:connect", "environment:connect"],
          }),
        ),
      });

      assert.equal(response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud health requests with the wrong issuer or audience", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test/",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const healthUrl = yield* getHttpServerUrl("/api/t3-connect/health");
      const postHealth = (request: ReturnType<typeof makeCloudEnvironmentHealthRequest>) =>
        fetchEffect(healthUrl, {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: jsonRequestBody(request),
        });

      const wrongIssuer = yield* postHealth(
        makeCloudEnvironmentHealthRequest({
          privateKey: cloudKeyPair.privateKey,
          environmentId: testEnvironmentDescriptor.environmentId,
          issuer: "https://attacker.example.test",
          jti: "cloud-health-jti-wrong-issuer",
          nonce: "cloud-health-nonce-wrong-issuer",
          issuedAt: DateTime.formatIso(now),
          expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
        }),
      );
      const wrongAudience = yield* postHealth(
        makeCloudEnvironmentHealthRequest({
          privateKey: cloudKeyPair.privateKey,
          environmentId: testEnvironmentDescriptor.environmentId,
          audience: "t3-env:other-environment",
          jti: "cloud-health-jti-wrong-audience",
          nonce: "cloud-health-nonce-wrong-audience",
          issuedAt: DateTime.formatIso(now),
          expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
        }),
      );

      assert.equal(wrongIssuer.status, 401);
      assert.equal(wrongAudience.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud health requests for a cloud subject other than the linked user", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test/",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const healthUrl = yield* getHttpServerUrl("/api/t3-connect/health");
      const response = yield* fetchEffect(healthUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(
          makeCloudEnvironmentHealthRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            subject: "user_other",
            jti: "cloud-health-jti-wrong-subject",
            nonce: "cloud-health-nonce-wrong-subject",
            issuedAt: DateTime.formatIso(now),
            expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
          }),
        ),
      });

      assert.equal(response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects cloud health requests without the exact status scope", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const cloudKeyPair = NodeCrypto.generateKeyPairSync("ed25519", {
        privateKeyEncoding: { format: "pem", type: "pkcs8" },
        publicKeyEncoding: { format: "pem", type: "spki" },
      });
      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const relayConfigUrl = yield* getHttpServerUrl("/api/connect/relay-config");
      const relayConfigResponse = yield* fetchEffect(relayConfigUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          relayUrl: "https://relay.example.test/",
          cloudUserId: "user_123",
          environmentCredential: "t3env_test_credential",
          cloudMintPublicKey: cloudKeyPair.publicKey,
          endpointRuntime: null,
        }),
      });
      assert.equal(relayConfigResponse.status, 200);

      const now = yield* DateTime.now;
      const healthUrl = yield* getHttpServerUrl("/api/t3-connect/health");
      const response = yield* fetchEffect(healthUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: jsonRequestBody(
          makeCloudEnvironmentHealthRequest({
            privateKey: cloudKeyPair.privateKey,
            environmentId: testEnvironmentDescriptor.environmentId,
            jti: "cloud-health-jti-duplicate-scope",
            nonce: "cloud-health-nonce-duplicate-scope",
            issuedAt: DateTime.formatIso(now),
            expiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
            scope: ["environment:status", "environment:status"],
          }),
        ),
      });

      assert.equal(response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("negotiates permessage-deflate with clients that offer it", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const { cookie, url } = parseSessionCookieFromWsUrl(yield* getWsServerUrl("/ws"));
      const openSocket = (perMessageDeflate: boolean) =>
        Effect.acquireRelease(
          Effect.callback<NodeSocket.NodeWS.WebSocket, Error>((resume) => {
            const socket = new NodeSocket.NodeWS.WebSocket(url, {
              perMessageDeflate,
              ...(cookie ? { headers: { cookie } } : {}),
            });
            socket.on("open", () => resume(Effect.succeed(socket)));
            socket.on("error", (error) => resume(Effect.fail(error)));
          }),
          (socket) => Effect.sync(() => socket.close()),
        );

      const compressed = yield* openSocket(true);
      // The ws client records the negotiated extension only when the server's
      // 101 response accepted the offer.
      assert.include(compressed.extensions, "permessage-deflate");

      const plain = yield* openSocket(false);
      assert.notInclude(plain.extensions, "permessage-deflate");
    }).pipe(Effect.scoped, Effect.provide(NodeHttpServerTestWithWsDeflate)),
  );

  it.effect("issues short-lived websocket tickets for authenticated bearer sessions", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const bearerToken = yield* getAuthenticatedBearerSessionToken();
      const wsTicketUrl = yield* getHttpServerUrl("/api/auth/websocket-ticket");
      const wsTicketResponse = yield* fetchEffect(wsTicketUrl, {
        method: "POST",
        headers: {
          authorization: `Bearer ${bearerToken}`,
        },
      });
      const wsTicketBody = yield* responseJsonEffect<{
        readonly ticket: string;
        readonly expiresAt: string;
      }>(wsTicketResponse);

      assert.equal(wsTicketResponse.status, 200);
      assert.equal(typeof wsTicketBody.ticket, "string");
      assert.isTrue(wsTicketBody.ticket.length > 0);
      assert.equal(typeof wsTicketBody.expiresAt, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("does not allow management-only access tokens to operate the environment", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const { response: exchangeResponse, body: tokenBody } = yield* exchangeAccessToken(
        defaultDesktopBootstrapToken,
        { scope: "access:write" },
      );
      assert.equal(exchangeResponse.status, 200);
      assert.equal(tokenBody.scope, "access:write");
      assert.isDefined(tokenBody.access_token);

      const overbroadPairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          authorization: `Bearer ${tokenBody.access_token ?? ""}`,
        },
        body: yield* HttpBody.json({}),
      });
      const overbroadPairingBody = (yield* overbroadPairingResponse.json) as {
        readonly requiredScope: string;
      };
      const pairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          authorization: `Bearer ${tokenBody.access_token ?? ""}`,
        },
        body: yield* HttpBody.json({ scopes: ["access:write"] }),
      });
      const wsTicketResponse = yield* HttpClient.post("/api/auth/websocket-ticket", {
        headers: {
          authorization: `Bearer ${tokenBody.access_token ?? ""}`,
        },
      });
      const wsTicketBody = (yield* wsTicketResponse.json) as { readonly ticket: string };
      assert.equal(overbroadPairingResponse.status, 403);
      assert.equal(overbroadPairingBody.requiredScope, "orchestration:read");
      assert.equal(pairingResponse.status, 200);
      assert.equal(wsTicketResponse.status, 200);
      const wsUrl = `${yield* getWsServerUrl("/ws", { authenticated: false })}?wsTicket=${encodeURIComponent(wsTicketBody.ticket)}`;
      const rpcError = yield* Effect.flip(
        Effect.scoped(withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverGetConfig]({}))),
      );
      assert.equal(rpcError._tag, "EnvironmentAuthorizationError");
      if (rpcError._tag === "EnvironmentAuthorizationError") {
        assert.equal(rpcError.requiredScope, "orchestration:read");
      }
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("includes CORS headers on remote auth success responses", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const origin = crossOriginClientOrigin;
      const { response: tokenResponse, body: tokenBody } = yield* exchangeAccessToken(
        defaultDesktopBootstrapToken,
        {
          headers: { origin },
        },
      );

      assert.equal(tokenResponse.status, 200);
      assertBrowserApiCorsResponseHeaders(tokenResponse.headers);
      assert.equal(tokenBody.token_type, "Bearer");
      assert.equal(typeof tokenBody.access_token, "string");

      const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
      const sessionResponse = yield* fetchEffect(sessionUrl, {
        headers: {
          authorization: `Bearer ${tokenBody.access_token ?? ""}`,
          origin,
        },
      });
      const sessionBody = yield* responseJsonEffect<{
        readonly authenticated: boolean;
        readonly sessionMethod?: string;
      }>(sessionResponse);

      assert.equal(sessionResponse.status, 200);
      assertBrowserApiCorsResponseHeaders(sessionResponse.headers);
      assert.equal(sessionBody.authenticated, true);
      assert.equal(sessionBody.sessionMethod, "bearer-access-token");

      const wsTicketUrl = yield* getHttpServerUrl("/api/auth/websocket-ticket");
      const wsTicketResponse = yield* fetchEffect(wsTicketUrl, {
        method: "POST",
        headers: {
          authorization: `Bearer ${tokenBody.access_token ?? ""}`,
          origin,
        },
      });
      const wsTicketBody = yield* responseJsonEffect<{
        readonly ticket: string;
      }>(wsTicketResponse);

      assert.equal(wsTicketResponse.status, 200);
      assertBrowserApiCorsResponseHeaders(wsTicketResponse.headers);
      assert.equal(typeof wsTicketBody.ticket, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "responds to remote auth websocket-ticket preflight requests with authorization CORS headers",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const wsTicketUrl = yield* getHttpServerUrl("/api/auth/websocket-ticket");
        const response = yield* fetchEffect(wsTicketUrl, {
          method: "OPTIONS",
          headers: {
            origin: crossOriginClientOrigin,
            "access-control-request-method": "POST",
            "access-control-request-headers": "authorization",
          },
        });

        assert.equal(response.status, 204);
        assertBrowserApiCorsPreflightHeaders(response.headers);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("allows credentialed cloud link proof preflights from the configured dev UI", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: { devUrl: new URL(crossOriginClientOrigin) },
      });

      const linkProofUrl = yield* getHttpServerUrl("/api/connect/link-proof");
      const response = yield* fetchEffect(linkProofUrl, {
        method: "OPTIONS",
        headers: {
          origin: crossOriginClientOrigin,
          "access-control-request-method": "POST",
          "access-control-request-headers": "content-type",
        },
      });

      assert.equal(response.status, 204);
      assertBrowserApiCorsPreflightHeaders(response.headers, {
        origin: crossOriginClientOrigin,
        credentials: true,
      });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("allows configured development origins through ServerConfig", () =>
    Effect.gen(function* () {
      const tailnetOrigin = "https://host.example.ts.net";
      yield* buildAppUnderTest({
        config: {
          devUrl: new URL(crossOriginClientOrigin),
          devAllowedOrigins: [tailnetOrigin],
        },
      });

      const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
      const response = yield* fetchEffect(sessionUrl, {
        method: "OPTIONS",
        headers: {
          origin: tailnetOrigin,
          "access-control-request-method": "GET",
          "access-control-request-headers": "content-type",
        },
      });

      assert.equal(response.status, 204);
      assertBrowserApiCorsPreflightHeaders(response.headers, {
        origin: tailnetOrigin,
        credentials: true,
      });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  for (const desktopOrigin of ["scient://app", "scient-next-dev://app"]) {
    it.effect(`allows credentialed preflights from ${desktopOrigin} in development`, () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest({
          config: { devUrl: new URL(crossOriginClientOrigin) },
        });

        const sessionUrl = yield* getHttpServerUrl("/api/auth/session");
        const response = yield* fetchEffect(sessionUrl, {
          method: "OPTIONS",
          headers: {
            origin: desktopOrigin,
            "access-control-request-method": "GET",
            "access-control-request-headers": "content-type",
          },
        });

        assert.equal(response.status, 204);
        assertBrowserApiCorsPreflightHeaders(response.headers, {
          origin: desktopOrigin,
          credentials: true,
        });
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
    );
  }

  it.effect("includes CORS headers on remote websocket-ticket auth failures", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const wsTicketUrl = yield* getHttpServerUrl("/api/auth/websocket-ticket");
      const response = yield* fetchEffect(wsTicketUrl, {
        method: "POST",
        headers: {
          origin: crossOriginClientOrigin,
        },
      });
      const body = yield* responseJsonEffect<{
        readonly _tag?: string;
        readonly code?: string;
        readonly reason?: string;
        readonly traceId?: string;
      }>(response);

      assert.equal(response.status, 401);
      assertBrowserApiCorsResponseHeaders(response.headers);
      assert.equal(body._tag, "EnvironmentAuthInvalidError");
      assert.equal(body.code, "auth_invalid");
      assert.equal(body.reason, "missing_credential");
      assert.equal(typeof body.traceId, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("issues authenticated one-time pairing credentials for additional clients", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const response = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
        },
        body: yield* HttpBody.json({}),
      });
      const body = (yield* response.json) as {
        readonly credential: string;
        readonly expiresAt: string;
      };

      assert.equal(response.status, 200);
      assert.equal(typeof body.credential, "string");
      assert.isTrue(body.credential.length > 0);
      assert.equal(typeof body.expiresAt, "string");

      const bootstrapResult = yield* bootstrapBrowserSession(body.credential);
      assert.equal(bootstrapResult.response.status, 200);

      const reusedResult = yield* bootstrapBrowserSession(body.credential);
      assert.equal(reusedResult.response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("issues pairing credentials for bearer sessions with access management scope", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const bearerToken = yield* getAuthenticatedBearerSessionToken();
      const response = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          authorization: `Bearer ${bearerToken}`,
        },
        body: yield* HttpBody.json({ label: "Hosted web" }),
      });
      const body = (yield* response.json) as {
        readonly credential: string;
        readonly label?: string;
      };

      assert.equal(response.status, 200);
      assert.isTrue(body.credential.length > 0);
      assert.equal(body.label, "Hosted web");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects pairing credentials with an empty scope grant", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const response = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
        },
        body: yield* HttpBody.json({ scopes: [] }),
      });
      const body = (yield* response.json) as {
        readonly code: string;
        readonly reason: string;
      };

      assert.equal(response.status, 400);
      assert.equal(body.code, "invalid_request");
      assert.equal(body.reason, "invalid_scope");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects unauthenticated pairing credential requests", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const response = yield* HttpClient.post("/api/auth/pairing-token", {
        body: yield* HttpBody.json({}),
      });
      assert.equal(response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("lists and revokes pairing links for access management sessions", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          host: "0.0.0.0",
        },
      });

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const createdResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const createdBody = (yield* createdResponse.json) as {
        readonly id: string;
        readonly credential: string;
      };

      const listResponse = yield* HttpClient.get("/api/auth/pairing-links", {
        headers: {
          cookie: ownerCookie,
        },
      });
      const listedLinks = (yield* listResponse.json) as ReadonlyArray<{
        readonly id: string;
        readonly credential: string;
      }>;

      const revokeResponse = yield* HttpClient.post("/api/auth/pairing-links/revoke", {
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: HttpBody.text(jsonRequestBody({ id: createdBody.id }), "application/json"),
      });
      const revokedBootstrap = yield* bootstrapBrowserSession(createdBody.credential);

      assert.equal(createdResponse.status, 200);
      assert.equal(listResponse.status, 200);
      assert.isTrue(listedLinks.some((entry) => entry.id === createdBody.id));
      assert.equal(revokeResponse.status, 200);
      assert.equal(revokedBootstrap.response.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects pairing credential requests without access management scope", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          host: "0.0.0.0",
        },
      });

      const ownerResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
        },
        body: yield* HttpBody.json({}),
      });
      const ownerBody = (yield* ownerResponse.json) as {
        readonly credential: string;
      };
      assert.equal(ownerResponse.status, 200);

      const pairedSessionCookie = yield* getAuthenticatedSessionCookieHeader(ownerBody.credential);
      const pairedResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: pairedSessionCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const pairedBody = (yield* pairedResponse.json) as {
        readonly _tag: string;
        readonly code: string;
        readonly requiredScope: string;
        readonly traceId: string;
      };

      assert.equal(pairedResponse.status, 403);
      assert.equal(pairedBody._tag, "EnvironmentScopeRequiredError");
      assert.equal(pairedBody.code, "insufficient_scope");
      assert.equal(pairedBody.requiredScope, "access:write");
      assert.equal(typeof pairedBody.traceId, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("lists paired clients and revokes other sessions while keeping the administrator", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          host: "0.0.0.0",
        },
      });

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const pairingTokenUrl = yield* getHttpServerUrl("/api/auth/pairing-token");
      const ownerPairingResponse = yield* fetchEffect(pairingTokenUrl, {
        method: "POST",
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: jsonRequestBody({
          label: "Julius iPhone",
        }),
      });
      const ownerPairingBody = yield* responseJsonEffect<{
        readonly credential: string;
        readonly label?: string;
      }>(ownerPairingResponse);
      assert.equal(ownerPairingResponse.status, 200);
      const pairedSessionBootstrap = yield* bootstrapBrowserSession(ownerPairingBody.credential, {
        headers: {
          "user-agent":
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
        },
      });
      const pairedSessionCookie = pairedSessionBootstrap.cookie?.split(";")[0];
      assert.isDefined(pairedSessionCookie);

      const pairedSessionCookieHeader = pairedSessionCookie ?? "";
      const listBeforeResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: {
          cookie: ownerCookie,
        },
      });
      const clientsBefore = (yield* listBeforeResponse.json) as ReadonlyArray<{
        readonly sessionId: string;
        readonly current: boolean;
        readonly client: {
          readonly label?: string;
          readonly deviceType: string;
          readonly ipAddress?: string;
          readonly os?: string;
          readonly browser?: string;
        };
      }>;
      const pairedClientBefore = clientsBefore.find((entry) => !entry.current);
      const pairedSessionId = clientsBefore.find((entry) => !entry.current)?.sessionId;

      const revokeOthersResponse = yield* HttpClient.post("/api/auth/clients/revoke-others", {
        headers: {
          cookie: ownerCookie,
        },
      });
      const revokeOthersBody = (yield* revokeOthersResponse.json) as {
        readonly revokedCount: number;
      };

      const listAfterResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: {
          cookie: ownerCookie,
        },
      });
      const clientsAfter = (yield* listAfterResponse.json) as ReadonlyArray<{
        readonly sessionId: string;
        readonly current: boolean;
      }>;

      const pairedClientPairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: pairedSessionCookieHeader,
        },
        body: yield* HttpBody.json({}),
      });
      const pairedClientPairingBody = (yield* pairedClientPairingResponse.json) as {
        readonly _tag: string;
        readonly code: string;
        readonly reason: string;
        readonly traceId: string;
      };

      assert.equal(listBeforeResponse.status, 200);
      assert.equal(ownerPairingBody.label, "Julius iPhone");
      assert.lengthOf(clientsBefore, 2);
      assert.isDefined(pairedSessionId);
      assert.isDefined(pairedClientBefore);
      assert.deepInclude(pairedClientBefore?.client, {
        label: "Julius iPhone",
        deviceType: "mobile",
        os: "iOS",
        browser: "Safari",
        ipAddress: "127.0.0.1",
      });
      assert.equal(revokeOthersResponse.status, 200);
      assert.equal(revokeOthersBody.revokedCount, 1);
      assert.equal(listAfterResponse.status, 200);
      assert.lengthOf(clientsAfter, 1);
      assert.equal(clientsAfter[0]?.current, true);
      assert.equal(pairedClientPairingResponse.status, 401);
      assert.equal(pairedClientPairingBody._tag, "EnvironmentAuthInvalidError");
      assert.equal(pairedClientPairingBody.code, "auth_invalid");
      assert.equal(pairedClientPairingBody.reason, "invalid_credential");
      assert.equal(typeof pairedClientPairingBody.traceId, "string");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("separates access inventory reads from credential management writes", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          host: "0.0.0.0",
        },
      });

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const issueScopedSession = Effect.fnUntraced(function* (
        scope: "access:read" | "access:write",
      ) {
        const pairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
          headers: {
            cookie: ownerCookie,
          },
          body: yield* HttpBody.json({ scopes: [scope] }),
        });
        assert.equal(pairingResponse.status, 200);
        const pairingBody = (yield* pairingResponse.json) as {
          readonly credential: string;
        };
        return yield* getAuthenticatedSessionCookieHeader(pairingBody.credential);
      });

      const readCookie = yield* issueScopedSession("access:read");
      const readListResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: {
          cookie: readCookie,
        },
      });
      const readWriteResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: readCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const readWriteBody = (yield* readWriteResponse.json) as {
        readonly requiredScope: string;
      };

      const writeCookie = yield* issueScopedSession("access:write");
      const writeListResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: {
          cookie: writeCookie,
        },
      });
      const writeListBody = (yield* writeListResponse.json) as {
        readonly requiredScope: string;
      };

      assert.equal(readListResponse.status, 200);
      assert.equal(readWriteResponse.status, 403);
      assert.equal(readWriteBody.requiredScope, "access:write");
      assert.equal(writeListResponse.status, 403);
      assert.equal(writeListBody.requiredScope, "access:read");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("revokes an individual paired client session", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          host: "0.0.0.0",
        },
      });

      const ownerCookie = yield* getAuthenticatedSessionCookieHeader();
      const pairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: ownerCookie,
        },
        body: yield* HttpBody.json({}),
      });
      const pairingBody = (yield* pairingResponse.json) as {
        readonly credential: string;
      };
      const pairedSessionCookie = yield* getAuthenticatedSessionCookieHeader(
        pairingBody.credential,
      );

      const clientsResponse = yield* HttpClient.get("/api/auth/clients", {
        headers: {
          cookie: ownerCookie,
        },
      });
      const clients = (yield* clientsResponse.json) as ReadonlyArray<{
        readonly sessionId: string;
        readonly current: boolean;
      }>;
      const pairedSessionId = clients.find((entry) => !entry.current)?.sessionId;
      assert.isDefined(pairedSessionId);

      const revokeResponse = yield* HttpClient.post("/api/auth/clients/revoke", {
        headers: {
          cookie: ownerCookie,
          "content-type": "application/json",
        },
        body: HttpBody.text(jsonRequestBody({ sessionId: pairedSessionId }), "application/json"),
      });
      const pairedClientPairingResponse = yield* HttpClient.post("/api/auth/pairing-token", {
        headers: {
          cookie: pairedSessionCookie,
        },
        body: yield* HttpBody.json({}),
      });

      assert.equal(revokeResponse.status, 200);
      assert.equal(pairedClientPairingResponse.status, 401);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("allows reusing the desktop bootstrap credential", () =>
    Effect.gen(function* () {
      // The desktop-bootstrap grant is delivered over trusted IPC at
      // backend launch and needs to stay claimable after a renderer
      // refresh, so it's intentionally reusable (unlike user-facing
      // one-time pairing credentials).
      yield* buildAppUnderTest();

      const first = yield* bootstrapBrowserSession();
      const second = yield* bootstrapBrowserSession();

      assert.equal(first.response.status, 200);
      assert.equal(second.response.status, 200);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("accepts websocket rpc handshake with a bootstrapped browser session cookie", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const { response: bootstrapResponse, cookie } = yield* bootstrapBrowserSession();

      assert.equal(bootstrapResponse.status, 200);
      assert.isDefined(cookie);

      const wsUrl = appendSessionCookieToWsUrl(
        yield* getWsServerUrl("/ws", { authenticated: false }),
        cookie?.split(";")[0] ?? "",
      );
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverGetConfig]({})),
      );

      assert.equal(response.environment.environmentId, testEnvironmentDescriptor.environmentId);
      assert.equal(response.auth.policy, "desktop-managed-local");
      assert.equal(response.shellResumeCompletionMarker, true);
      assert.equal(response.threadResumeCompletionMarker, true);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("does not block server config when editor discovery never resolves", () =>
    Effect.gen(function* () {
      const discoveryInterrupted = yield* Deferred.make<void>();
      const responseFiber = yield* resolveAvailableEditorsForConfig(
        Effect.never.pipe(
          Effect.onInterrupt(() => Deferred.succeed(discoveryInterrupted, undefined)),
        ),
      ).pipe(Effect.forkChild);

      yield* TestClock.adjust(Duration.seconds(5));

      const availableEditors = yield* Fiber.join(responseFiber);
      yield* Deferred.await(discoveryInterrupted);
      assert.deepEqual(availableEditors, []);
    }),
  );

  it.effect(
    "rejects websocket rpc handshake when a session token is only provided via query string",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const { cookie } = yield* bootstrapBrowserSession();
        assert.isDefined(cookie);
        const sessionToken = extractSessionTokenFromSetCookie(cookie ?? "");
        const wsUrl = `${yield* getWsServerUrl("/ws", { authenticated: false })}?token=${encodeURIComponent(sessionToken)}`;

        const error = yield* Effect.flip(
          Effect.scoped(withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverGetConfig]({}))),
        );

        assert.equal(error._tag, "RpcClientError");
        assertInclude(String(error), "SocketOpenError");
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "accepts websocket rpc handshake with a dedicated websocket ticket in the query string",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const bearerToken = yield* getAuthenticatedBearerSessionToken();
        const wsTicketUrl = yield* getHttpServerUrl("/api/auth/websocket-ticket");
        const wsTicketResponse = yield* fetchEffect(wsTicketUrl, {
          method: "POST",
          headers: {
            authorization: `Bearer ${bearerToken}`,
          },
        });
        const wsTicketBody = yield* responseJsonEffect<{
          readonly ticket: string;
        }>(wsTicketResponse);
        const wsUrl = `${yield* getWsServerUrl("/ws", { authenticated: false })}?wsTicket=${encodeURIComponent(wsTicketBody.ticket)}`;

        const response = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverGetConfig]({})),
        );

        assert.equal(response.environment.environmentId, testEnvironmentDescriptor.environmentId);
        assert.equal(response.auth.policy, "desktop-managed-local");
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("proxies browser OTLP trace exports through the server", () =>
    Effect.gen(function* () {
      const upstreamRequests: Array<{
        readonly body: string;
        readonly contentType: string | null;
      }> = [];
      const localTraceRecords: Array<unknown> = [];
      const payload = {
        resourceSpans: [
          {
            resource: {
              attributes: [
                {
                  key: "service.name",
                  value: { stringValue: "t3-web" },
                },
              ],
            },
            scopeSpans: [
              {
                scope: {
                  name: "effect",
                  version: "4.0.0-beta.43",
                },
                spans: [
                  {
                    traceId: "11111111111111111111111111111111",
                    spanId: "2222222222222222",
                    parentSpanId: "3333333333333333",
                    name: "RpcClient.server.getSettings",
                    kind: 3,
                    startTimeUnixNano: "1000000",
                    endTimeUnixNano: "2000000",
                    attributes: [
                      {
                        key: "rpc.method",
                        value: { stringValue: "server.getSettings" },
                      },
                    ],
                    events: [
                      {
                        name: "http.request",
                        timeUnixNano: "1500000",
                        attributes: [
                          {
                            key: "http.status_code",
                            value: { intValue: "200" },
                          },
                        ],
                      },
                    ],
                    links: [],
                    status: {
                      code: "STATUS_CODE_OK",
                    },
                    flags: 1,
                  },
                ],
              },
            ],
          },
        ],
      };

      const collector = yield* Effect.acquireRelease(
        Effect.promise(async () => {
          const NodeHttp = await import("node:http");

          return await new Promise<{
            readonly close: () => Promise<void>;
            readonly url: string;
          }>((resolve, reject) => {
            const server = NodeHttp.createServer((request, response) => {
              const chunks: Buffer[] = [];
              request.on("data", (chunk) => {
                chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
              });
              request.on("end", () => {
                upstreamRequests.push({
                  body: Buffer.concat(chunks).toString("utf8"),
                  contentType: request.headers["content-type"] ?? null,
                });
                response.statusCode = 204;
                response.end();
              });
            });

            server.on("error", reject);
            server.listen(0, "127.0.0.1", () => {
              const address = server.address();
              if (!address || typeof address === "string") {
                reject(new Error("Expected TCP collector address"));
                return;
              }

              resolve({
                url: `http://127.0.0.1:${address.port}/v1/traces`,
                close: () =>
                  new Promise<void>((resolveClose, rejectClose) => {
                    server.close((error) => {
                      if (error) {
                        rejectClose(error);
                        return;
                      }
                      resolveClose();
                    });
                  }),
              });
            });
          });
        }),
        ({ close }) => Effect.promise(close),
      );

      yield* buildAppUnderTest({
        config: {
          otlpTracesUrl: collector.url,
        },
        layers: {
          browserTraceCollector: {
            record: (records) =>
              Effect.sync(() => {
                localTraceRecords.push(...records);
              }),
          },
        },
      });

      const response = yield* HttpClient.post("/api/observability/v1/traces", {
        headers: {
          cookie: yield* getAuthenticatedSessionCookieHeader(),
          "content-type": "application/json",
          origin: "http://localhost:5733",
        },
        // @effect-diagnostics-next-line preferSchemaOverJson:off
        body: HttpBody.text(JSON.stringify(payload), "application/json"),
      });

      assert.equal(response.status, 204);
      assert.equal(response.headers["access-control-allow-origin"], "*");
      assert.deepEqual(localTraceRecords, [
        {
          type: "otlp-span",
          name: "RpcClient.server.getSettings",
          traceId: "11111111111111111111111111111111",
          spanId: "2222222222222222",
          parentSpanId: "3333333333333333",
          sampled: true,
          kind: "client",
          startTimeUnixNano: "1000000",
          endTimeUnixNano: "2000000",
          durationMs: 1,
          attributes: {
            "rpc.method": "server.getSettings",
          },
          resourceAttributes: {
            "service.name": "t3-web",
          },
          scope: {
            name: "effect",
            version: "4.0.0-beta.43",
            attributes: {},
          },
          events: [
            {
              name: "http.request",
              timeUnixNano: "1500000",
              attributes: {
                "http.status_code": "200",
              },
            },
          ],
          links: [],
          status: {
            code: "STATUS_CODE_OK",
          },
        },
      ]);
      assert.deepEqual(upstreamRequests, [
        {
          body: jsonRequestBody(payload),
          contentType: "application/json",
        },
      ]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("responds to browser OTLP trace preflight requests with CORS headers", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const response = yield* HttpClient.options("/api/observability/v1/traces", {
        headers: {
          origin: "http://localhost:5733",
          "access-control-request-method": "POST",
          "access-control-request-headers": "content-type",
        },
      });

      assert.equal(response.status, 204);
      assert.equal(response.headers["access-control-allow-origin"], "*");
      assert.deepEqual(splitHeaderTokens(response.headers["access-control-allow-methods"]), [
        "GET",
        "HEAD",
        "OPTIONS",
        "POST",
      ]);
      assert.deepEqual(splitHeaderTokens(response.headers["access-control-allow-headers"]), [
        "authorization",
        "b3",
        "content-type",
        "dpop",
        "range",
        "traceparent",
      ]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "stores browser OTLP trace exports locally when no upstream collector is configured",
    () =>
      Effect.gen(function* () {
        const localTraceRecords: Array<unknown> = [];
        const payload = yield* makeBrowserOtlpPayload("client.test");
        const resourceSpan = payload.resourceSpans[0];
        const scopeSpan = resourceSpan?.scopeSpans[0];
        const span = scopeSpan?.spans[0];

        assert.notEqual(resourceSpan, undefined);
        assert.notEqual(scopeSpan, undefined);
        assert.notEqual(span, undefined);
        if (!resourceSpan || !scopeSpan || !span) {
          return;
        }

        yield* buildAppUnderTest({
          layers: {
            browserTraceCollector: {
              record: (records) =>
                Effect.sync(() => {
                  localTraceRecords.push(...records);
                }),
            },
          },
        });

        const response = yield* HttpClient.post("/api/observability/v1/traces", {
          headers: {
            cookie: yield* getAuthenticatedSessionCookieHeader(),
            "content-type": "application/json",
          },
          // @effect-diagnostics-next-line preferSchemaOverJson:off
          body: HttpBody.text(JSON.stringify(payload), "application/json"),
        });

        assert.equal(response.status, 204);
        assert.equal(localTraceRecords.length, 1);
        const record = localTraceRecords[0] as {
          readonly type: string;
          readonly name: string;
          readonly traceId: string;
          readonly spanId: string;
          readonly kind: string;
          readonly attributes: Readonly<Record<string, unknown>>;
          readonly events: ReadonlyArray<unknown>;
          readonly links: ReadonlyArray<unknown>;
          readonly scope: {
            readonly name?: string;
            readonly attributes: Readonly<Record<string, unknown>>;
          };
          readonly resourceAttributes: Readonly<Record<string, unknown>>;
          readonly status?: {
            readonly code?: string;
          };
        };

        assert.equal(record.type, "otlp-span");
        assert.equal(record.name, span.name);
        assert.equal(record.traceId, span.traceId);
        assert.equal(record.spanId, span.spanId);
        assert.equal(record.kind, "internal");
        assert.deepEqual(record.attributes, {});
        assert.deepEqual(record.events, []);
        assert.deepEqual(record.links, []);
        assert.equal(record.scope.name, scopeSpan.scope.name);
        assert.deepEqual(record.scope.attributes, {});
        assert.equal(record.resourceAttributes["service.name"], "t3-web");
        assert.equal(record.status?.code, String(span.status.code));
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc server.upsertKeybinding", () =>
    Effect.gen(function* () {
      const rule: KeybindingRule = {
        command: "terminal.toggle",
        key: "ctrl+k",
      };
      const resolved: ResolvedKeybindingRule = {
        command: "terminal.toggle",
        shortcut: {
          key: "k",
          metaKey: false,
          ctrlKey: true,
          shiftKey: false,
          altKey: false,
          modKey: true,
        },
      };

      yield* buildAppUnderTest({
        layers: {
          keybindings: {
            upsertKeybindingRule: () => Effect.succeed([resolved]),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverUpsertKeybinding](rule)),
      );

      assert.deepEqual(response.issues, []);
      assert.deepEqual(response.keybindings, [resolved]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc server.removeKeybinding", () =>
    Effect.gen(function* () {
      const rule: KeybindingRule = {
        command: "terminal.toggle",
        key: "ctrl+k",
      };
      const resolved: ResolvedKeybindingRule = {
        command: "terminal.toggle",
        shortcut: {
          key: "j",
          metaKey: false,
          ctrlKey: false,
          shiftKey: false,
          altKey: false,
          modKey: true,
        },
      };

      yield* buildAppUnderTest({
        layers: {
          keybindings: {
            removeKeybindingRule: () => Effect.succeed([resolved]),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.serverRemoveKeybinding](rule)),
      );

      assert.deepEqual(response.issues, []);
      assert.deepEqual(response.keybindings, [resolved]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("shares one preview automation broker across websocket sessions", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* buildAppUnderTest();

        const wsUrl = yield* getWsServerUrl("/ws");
        const firstConnected = yield* Deferred.make<string>();
        const firstClosed = yield* Deferred.make<void>();
        const host = {
          clientId: "shared-preview-host",
          environmentId: testEnvironmentDescriptor.environmentId,
        } as const;

        yield* withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.previewAutomationConnect](host).pipe(
            Stream.tap((event) =>
              event.type === "connected"
                ? Deferred.succeed(firstConnected, event.connectionId)
                : Effect.void,
            ),
            Stream.runDrain,
            Effect.ensuring(Deferred.succeed(firstClosed, undefined)),
          ),
        ).pipe(Effect.forkScoped);

        const firstConnectionId = yield* Deferred.await(firstConnected);
        const replacementEvent = yield* withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.previewAutomationConnect](host).pipe(Stream.runHead),
        ).pipe(Effect.map(Option.getOrThrow));
        const firstStreamClosed = yield* Deferred.await(firstClosed).pipe(
          Effect.timeoutOption("2 seconds"),
        );

        assert.equal(replacementEvent.type, "connected");
        assert.notEqual(replacementEvent.connectionId, firstConnectionId);
        assert.isTrue(Option.isSome(firstStreamClosed));
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects websocket rpc handshake when session authentication is missing", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ws-auth-required-" });
      yield* fs.writeFileString(
        path.join(workspaceDir, "needle-file.ts"),
        "export const needle = 1;",
      );

      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws", { authenticated: false });
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.projectsSearchEntries]({
            cwd: workspaceDir,
            query: "needle",
            limit: 10,
          }),
        ).pipe(Effect.result),
      );

      assertTrue(result._tag === "Failure");
      const failureMessage = String(result.failure);
      assertTrue(
        failureMessage.includes("SocketOpenError") || failureMessage.includes("SocketCloseError"),
      );
      assertTrue(
        failureMessage.includes("Unauthorized") ||
          failureMessage.includes("An error occurred during Open"),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc subscribeServerConfig streams snapshot then update", () =>
    Effect.gen(function* () {
      const providers = [
        {
          instanceId: ProviderInstanceId.make("codex"),
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          installed: true,
          version: "1.0.0",
          status: "ready" as const,
          auth: { status: "authenticated" as const },
          checkedAt: "2026-04-11T00:00:00.000Z",
          models: [],
          slashCommands: [],
          skills: [],
        },
      ] as const;
      const changeEvent = {
        keybindings: [],
        issues: [],
      } as const;

      yield* buildAppUnderTest({
        config: {
          otlpTracesUrl: "http://localhost:4318/v1/traces",
          otlpMetricsUrl: "http://localhost:4318/v1/metrics",
        },
        layers: {
          keybindings: {
            loadConfigState: Effect.succeed({
              keybindings: [],
              issues: [],
            }),
            streamChanges: Stream.succeed(changeEvent),
          },
          providerRegistry: {
            getProviders: Effect.succeed(providers),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const events = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.subscribeServerConfig]({}).pipe(Stream.take(2), Stream.runCollect),
        ),
      );

      const [first, second] = Array.from(events);
      assert.equal(first?.type, "snapshot");
      if (first?.type === "snapshot") {
        assert.equal(first.version, 1);
        assert.deepEqual(first.config.keybindings, []);
        assert.deepEqual(first.config.issues, []);
        assert.deepEqual(first.config.providers, providers);
        assert.equal(first.config.observability.logsDirectoryPath.endsWith("/logs"), true);
        assert.equal(first.config.observability.localTracingEnabled, true);
        assert.equal(first.config.observability.otlpTracesUrl, "http://localhost:4318/v1/traces");
        assert.equal(first.config.observability.otlpTracesEnabled, true);
        assert.equal(first.config.observability.otlpMetricsUrl, "http://localhost:4318/v1/metrics");
        assert.equal(first.config.observability.otlpMetricsEnabled, true);
        assert.deepEqual(first.config.settings, DEFAULT_SERVER_SETTINGS);
      }
      assert.deepEqual(second, {
        version: 1,
        type: "keybindingsUpdated",
        payload: { keybindings: [], issues: [] },
      });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket resource telemetry through the subscription", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws");
      const snapshot = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.subscribeResourceTelemetry]({}).pipe(Stream.runHead),
        ),
      );

      assertTrue(Option.isSome(snapshot));
      assert.equal(snapshot.value.processes.length, 0);
      assert.equal(snapshot.value.groups.backend.processCount, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc subscribeServerConfig emits provider status updates", () =>
    Effect.gen(function* () {
      const nextProviders = [
        {
          instanceId: ProviderInstanceId.make("codex"),
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          installed: true,
          version: "1.0.0",
          status: "ready" as const,
          auth: { status: "authenticated" as const },
          checkedAt: "2026-04-11T00:00:00.000Z",
          models: [],
          slashCommands: [],
          skills: [],
        },
      ] as const;

      yield* buildAppUnderTest({
        layers: {
          keybindings: {
            loadConfigState: Effect.succeed({
              keybindings: [],
              issues: [],
            }),
            streamChanges: Stream.empty,
          },
          providerRegistry: {
            getProviders: Effect.succeed([]),
            streamChanges: Stream.succeed(nextProviders),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const events = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.subscribeServerConfig]({}).pipe(Stream.take(2), Stream.runCollect),
        ),
      );

      const [first, second] = Array.from(events);
      assert.equal(first?.type, "snapshot");
      if (first?.type === "snapshot") {
        assert.deepEqual(first.config.providers, []);
      }
      assert.deepEqual(second, {
        version: 1,
        type: "providerStatuses",
        payload: { providers: nextProviders },
      });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "routes websocket rpc subscribeServerLifecycle replays snapshot and streams updates",
    () =>
      Effect.gen(function* () {
        const lifecycleEvents = [
          {
            version: 1 as const,
            sequence: 1,
            type: "welcome" as const,
            payload: {
              environment: testEnvironmentDescriptor,
              cwd: "/tmp/project",
              projectName: "project",
            },
          },
        ] as const;
        const liveEvents = Stream.make({
          version: 1 as const,
          sequence: 2,
          type: "ready" as const,
          payload: { at: "2026-01-01T00:00:00.000Z", environment: testEnvironmentDescriptor },
        });

        yield* buildAppUnderTest({
          layers: {
            serverLifecycleEvents: {
              snapshot: Effect.succeed({
                sequence: 1,
                events: lifecycleEvents,
              }),
              stream: liveEvents,
            },
          },
        });

        const wsUrl = yield* getWsServerUrl("/ws");
        const events = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.subscribeServerLifecycle]({}).pipe(Stream.take(2), Stream.runCollect),
          ),
        );

        const [first, second] = Array.from(events);
        assert.equal(first?.type, "welcome");
        assert.equal(first?.sequence, 1);
        assert.equal(second?.type, "ready");
        assert.equal(second?.sequence, 2);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc projects.searchEntries", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ws-project-search-" });
      yield* fs.writeFileString(
        path.join(workspaceDir, "needle-file.ts"),
        "export const needle = 1;",
      );

      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.projectsSearchEntries]({
            cwd: workspaceDir,
            query: "needle",
            limit: 10,
          }),
        ),
      );

      assert.isAtLeast(response.entries.length, 1);
      assert.isTrue(response.entries.some((entry) => entry.path === "needle-file.ts"));
      assert.equal(response.truncated, false);
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("routes websocket rpc projects.listEntries and projects.readFile", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ws-project-files-" });
      yield* fs.makeDirectory(path.join(workspaceDir, "src"), { recursive: true });
      yield* fs.writeFileString(
        path.join(workspaceDir, "src", "index.ts"),
        "export const answer = 42;\n",
      );

      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.all({
            listing: client[WS_METHODS.projectsListEntries]({ cwd: workspaceDir }),
            file: client[WS_METHODS.projectsReadFile]({
              cwd: workspaceDir,
              relativePath: "src/index.ts",
            }),
          }),
        ),
      );

      assert.isTrue(response.listing.entries.some((entry) => entry.path === "src/index.ts"));
      assert.deepEqual(response.file, {
        relativePath: "src/index.ts",
        contents: "export const answer = 42;\n",
        byteLength: 26,
        truncated: false,
        revision: `sha256:${NodeCrypto.createHash("sha256")
          .update("export const answer = 42;\n")
          .digest("hex")}`,
      });
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("routes websocket rpc projects.searchEntries excludes gitignored files", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-ws-project-search-gitignored-",
      });
      yield* fs.writeFileString(path.join(workspaceDir, ".gitignore"), ".venv/\n");
      yield* fs.makeDirectory(path.join(workspaceDir, ".venv", "lib"), { recursive: true });
      yield* fs.writeFileString(
        path.join(workspaceDir, ".venv", "lib", "ignored-search-target.ts"),
        "export const ignored = true;",
      );
      yield* fs.makeDirectory(path.join(workspaceDir, "src"), { recursive: true });
      yield* fs.writeFileString(
        path.join(workspaceDir, "src", "tracked.ts"),
        "export const ok = 1;",
      );

      yield* buildAppUnderTest({
        layers: {
          vcsDriver: {
            isInsideWorkTree: () => Effect.succeed(true),
            listWorkspaceFiles: () =>
              Effect.succeed({
                paths: ["src/tracked.ts"],
                truncated: false,
                freshness: {
                  source: "live-local",
                  observedAt: TEST_EPOCH,
                  expiresAt: Option.none(),
                },
              }),
            filterIgnoredPaths: (_cwd, relativePaths) =>
              Effect.succeed(
                relativePaths.filter((relativePath) => !relativePath.startsWith(".venv/")),
              ),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.projectsSearchEntries]({
            cwd: workspaceDir,
            query: "ignored-search-target",
            limit: 10,
          }),
        ),
      );

      assert.equal(response.entries.length, 0);
      assert.equal(response.truncated, false);
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("preserves structured workspace rpc failures", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-ws-workspace-errors-",
      });
      const outsideDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-ws-workspace-errors-outside-",
      });
      const outsideFile = path.join(outsideDir, "outside.txt");
      yield* fs.writeFileString(outsideFile, "outside\n");
      yield* fs.symlink(outsideFile, path.join(workspaceDir, "linked-outside.txt"));
      const resolvedOutsideFile = yield* fs.realPath(outsideFile);

      yield* buildAppUnderTest();

      const invalidWorkspace = path.join(workspaceDir, "missing-workspace");
      const missingBrowseParent = path.join(workspaceDir, "missing-browse");
      const sensitiveQuery = "authorization: Bearer secret-token";
      const wsUrl = yield* getWsServerUrl("/ws");
      const results = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.all({
            search: client[WS_METHODS.projectsSearchEntries]({
              cwd: invalidWorkspace,
              query: sensitiveQuery,
              limit: 10,
            }).pipe(Effect.result),
            list: client[WS_METHODS.projectsListEntries]({ cwd: invalidWorkspace }).pipe(
              Effect.result,
            ),
            read: client[WS_METHODS.projectsReadFile]({
              cwd: workspaceDir,
              relativePath: "linked-outside.txt",
            }).pipe(Effect.result),
            browse: client[WS_METHODS.filesystemBrowse]({
              cwd: workspaceDir,
              partialPath: "./missing-browse/child",
            }).pipe(Effect.result),
          }),
        ),
      );

      if (
        results.search._tag !== "Failure" ||
        results.search.failure._tag !== "ProjectSearchEntriesError"
      ) {
        assert.fail("Expected a ProjectSearchEntriesError");
      }
      const searchError = results.search.failure;
      assert.equal(
        searchError.message,
        `Failed to search workspace entries in '${invalidWorkspace}'.`,
      );
      assert.equal(searchError.cwd, invalidWorkspace);
      assert.equal(searchError.queryLength, sensitiveQuery.length);
      assert.notProperty(searchError, "query");
      assert.notInclude(searchError.message, "Bearer");
      assert.notInclude(searchError.message, "secret-token");
      assert.equal(searchError.limit, 10);
      assert.equal(searchError.failure, "workspace_root_not_found");
      assert.equal(searchError.normalizedCwd, invalidWorkspace);
      assert.isDefined(searchError.cause);

      if (
        results.list._tag !== "Failure" ||
        results.list.failure._tag !== "ProjectListEntriesError"
      ) {
        assert.fail("Expected a ProjectListEntriesError");
      }
      const listError = results.list.failure;
      assert.equal(listError.message, `Failed to list workspace entries in '${invalidWorkspace}'.`);
      assert.equal(listError.cwd, invalidWorkspace);
      assert.equal(listError.failure, "workspace_root_not_found");
      assert.equal(listError.normalizedCwd, invalidWorkspace);
      assert.isDefined(listError.cause);

      if (results.read._tag !== "Failure" || results.read.failure._tag !== "ProjectReadFileError") {
        assert.fail("Expected a ProjectReadFileError");
      }
      const readError = results.read.failure;
      assert.equal(
        readError.message,
        `Failed to read workspace file 'linked-outside.txt' in '${workspaceDir}'.`,
      );
      assert.equal(readError.cwd, workspaceDir);
      assert.equal(readError.relativePath, "linked-outside.txt");
      assert.equal(readError.failure, "resolved_path_outside_root");
      assert.equal(readError.resolvedPath, resolvedOutsideFile);
      assert.isDefined(readError.cause);

      if (
        results.browse._tag !== "Failure" ||
        results.browse.failure._tag !== "FilesystemBrowseError"
      ) {
        assert.fail("Expected a FilesystemBrowseError");
      }
      const browseError = results.browse.failure;
      assert.equal(
        browseError.message,
        `Failed to browse filesystem path './missing-browse/child' from '${workspaceDir}'.`,
      );
      assert.equal(browseError.cwd, workspaceDir);
      assert.equal(browseError.partialPath, "./missing-browse/child");
      assert.equal(browseError.failure, "read_directory_failed");
      assert.equal(browseError.parentPath, missingBrowseParent);
      assert.isDefined(browseError.cause);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("reports workspace root stat failures without relabeling them as missing", () =>
    Effect.gen(function* () {
      if ((yield* HostProcessPlatform) === "win32") return;

      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const blockedRoot = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-ws-workspace-stat-error-",
      });
      const workspaceRoot = path.join(blockedRoot, "workspace");
      yield* fs.makeDirectory(workspaceRoot);
      yield* fs.chmod(blockedRoot, 0o000);

      const result = yield* Effect.gen(function* () {
        yield* buildAppUnderTest();
        const wsUrl = yield* getWsServerUrl("/ws");
        return yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.projectsListEntries]({ cwd: workspaceRoot }).pipe(Effect.result),
          ),
        );
      }).pipe(Effect.ensuring(fs.chmod(blockedRoot, 0o700).pipe(Effect.ignore)));

      if (result._tag !== "Failure" || result.failure._tag !== "ProjectListEntriesError") {
        assert.fail("Expected a ProjectListEntriesError");
      }
      const error = result.failure;
      assert.equal(error.failure, "workspace_root_stat_failed");
      assert.equal(error.normalizedCwd, workspaceRoot);
      assert.equal(error.detail, "validate-existing");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc projects.writeFile", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ws-project-write-" });

      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.projectsWriteFile]({
            cwd: workspaceDir,
            relativePath: "nested/created.txt",
            contents: "written-by-rpc",
          }),
        ),
      );

      assert.equal(response.relativePath, "nested/created.txt");
      const persisted = yield* fs.readFileString(path.join(workspaceDir, "nested", "created.txt"));
      assert.equal(persisted, "written-by-rpc");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("streams selected workspace file changes over websocket rpc", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-ws-project-watch-",
      });
      const sourcePath = path.join(workspaceDir, "analysis.m");
      yield* fs.writeFileString(sourcePath, "answer = 1;\n");

      yield* buildAppUnderTest();
      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const event = yield* client[WS_METHODS.projectsSubscribeFileChanges]({
              cwd: workspaceDir,
              relativePath: "analysis.m",
            }).pipe(
              Stream.tap((event) =>
                event._tag === "watch-ready"
                  ? fs.writeFileString(sourcePath, "answer = 2;\n")
                  : Effect.void,
              ),
              Stream.filter((event) => event._tag === "file-changed"),
              Stream.runHead,
            );
            const refreshed = yield* client[WS_METHODS.projectsReadFile]({
              cwd: workspaceDir,
              relativePath: "analysis.m",
            });
            return { event, refreshed };
          }),
        ),
      );

      assert.deepEqual(
        result.event,
        Option.some({ _tag: "file-changed", relativePath: "analysis.m" }),
      );
      assert.equal(result.refreshed.contents, "answer = 2;\n");
      assert.match(result.refreshed.revision, /^sha256:[0-9a-f]{64}$/u);
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("runs a project-owned MATLAB file through the analysis RPC and streams output", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-rpc-project-",
      });
      const runtimeDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-rpc-runtime-",
      });
      const matlabExecutable = path.join(runtimeDir, "matlab");
      const sourcePath = path.join(workspaceDir, "analysis.m");
      yield* Effect.promise(() =>
        initializeScientProject({ root: workspaceDir, title: "MATLAB RPC Test" }),
      );
      yield* fs.writeFileString(sourcePath, "disp('scient-analysis-ok');\n");
      yield* fs.writeFileString(
        matlabExecutable,
        [
          "#!/usr/bin/env node",
          "const fs = require('node:fs');",
          "const source = process.env.SCIENT_MATLAB_ENTRYPOINT;",
          "if (!source || !fs.readFileSync(source, 'utf8').includes('scient-analysis-ok')) process.exit(2);",
          "const artifacts = process.env.SCIENT_MATLAB_ARTIFACT_DIR;",
          "if (!artifacts || !process.env.SCIENT_MATLAB_RUNNER) process.exit(3);",
          "fs.mkdirSync(artifacts, { recursive: true });",
          "fs.writeFileSync(`${artifacts}/figure-001.png`, Buffer.from([137, 80, 78, 71]));",
          "fs.writeFileSync(`${artifacts}/figure-001.fig`, 'scient-analysis-figure');",
          "console.log('stdout:scient-analysis-ok');",
          "console.error('stderr:diagnostic');",
          "",
        ].join("\n"),
      );
      yield* fs.chmod(matlabExecutable, 0o755);

      yield* buildAppUnderTest();
      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const inspection = yield* client[WS_METHODS.analysisConfigureRuntime]({
              cwd: workspaceDir,
              runtimeKind: "matlab",
              executablePath: matlabExecutable,
            });
            assert.equal(inspection.runtimes[0]?.availability, "available");
            const source = yield* client[WS_METHODS.projectsReadFile]({
              cwd: workspaceDir,
              relativePath: "analysis.m",
            });
            const started = yield* client[WS_METHODS.analysisStartRun]({
              cwd: workspaceDir,
              relativePath: "analysis.m",
              sourceRevision: AnalysisSourceRevision.make(source.revision),
              runtimeId: inspection.runtimes[0]!.id,
            });
            const events = yield* client[WS_METHODS.subscribeAnalysisRuns]({
              cwd: workspaceDir,
              relativePath: "analysis.m",
            }).pipe(
              Stream.filterMap((event) =>
                event._tag === "run-updated" && event.run.receipt.runId === started.receipt.runId
                  ? Result.succeed(event.run)
                  : Result.failVoid,
              ),
              Stream.takeUntil((run) =>
                ["succeeded", "failed", "cancelled", "lost"].includes(run.receipt.status),
              ),
              Stream.runCollect,
              Effect.timeoutOrElse({
                duration: "10 seconds",
                orElse: () => Effect.die(new Error("Timed out waiting for MATLAB run output.")),
              }),
            );
            const terminal = events.at(-1)!;
            const listed = yield* client[WS_METHODS.analysisListRuns]({
              cwd: workspaceDir,
              relativePath: "analysis.m",
            });
            assert.isFalse("output" in listed.runs[0]!.receipt);
            assert.equal(terminal.receipt.status, "succeeded");
            const terminalRun = yield* client[WS_METHODS.analysisGetRun]({
              cwd: workspaceDir,
              runId: started.receipt.runId,
            });
            const figure = terminalRun.artifacts[0]?.representations.find(
              (representation) =>
                representation.mediaType === "application/vnd.mathworks.matlab.figure",
            );
            assert.isDefined(figure);
            const asset = yield* client[WS_METHODS.assetsCreateUrl]({
              resource: {
                _tag: "analysis-artifact",
                projectId: terminalRun.projectId,
                runId: terminalRun.receipt.runId,
                artifactId: terminalRun.artifacts[0]!.artifactId,
                representationId: figure!.representationId,
              },
            });
            return { terminalRun, asset };
          }),
        ),
      );
      const { terminalRun, asset } = result;

      assert.equal(terminalRun.receipt.status, "succeeded");
      assert.equal(terminalRun.receipt.exitCode, 0);
      const outputHash = NodeCrypto.createHash("sha256");
      for (const part of executionOutputContentParts(terminalRun.receipt.output)) {
        outputHash.update(part, "utf8");
      }
      assert.equal(terminalRun.receipt.outputContentHash, `sha256:${outputHash.digest("hex")}`);
      const output = terminalRun.receipt.output.map((chunk) => chunk.text).join("");
      assert.include(output, "stdout:scient-analysis-ok");
      assert.include(output, "stderr:diagnostic");
      assert.deepEqual(
        terminalRun.artifacts[0]?.representations.map((representation) => representation.mediaType),
        ["application/vnd.mathworks.matlab.figure", "image/png"],
      );
      const assetResponse = yield* fetchEffect(yield* getHttpServerUrl(asset.relativeUrl));
      assert.equal(assetResponse.status, 200);
      assert.equal(yield* assetResponse.text, "scient-analysis-figure");
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("cleans retained MATLAB data without deleting its history or provenance", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-cleanup-project-",
      });
      const runtimeDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-cleanup-runtime-",
      });
      const matlabExecutable = path.join(runtimeDir, "matlab");
      yield* Effect.promise(() => initializeScientProject({ root: workspaceDir }));
      yield* fs.writeFileString(path.join(workspaceDir, "cleanup.m"), "disp('retain receipt');\n");
      yield* fs.writeFileString(
        matlabExecutable,
        [
          "#!/usr/bin/env node",
          "const fs = require('node:fs');",
          "const artifacts = process.env.SCIENT_MATLAB_ARTIFACT_DIR;",
          "fs.mkdirSync(artifacts, { recursive: true });",
          "fs.writeFileSync(`${artifacts}/figure-001.png`, Buffer.from([137, 80, 78, 71, 13, 10]));",
          "console.log('cleanup-preserves-provenance');",
          "",
        ].join("\n"),
      );
      yield* fs.chmod(matlabExecutable, 0o755);

      yield* buildAppUnderTest();
      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const inspection = yield* client[WS_METHODS.analysisConfigureRuntime]({
              cwd: workspaceDir,
              runtimeKind: "matlab",
              executablePath: matlabExecutable,
            });
            const source = yield* client[WS_METHODS.projectsReadFile]({
              cwd: workspaceDir,
              relativePath: "cleanup.m",
            });
            const started = yield* client[WS_METHODS.analysisStartRun]({
              cwd: workspaceDir,
              relativePath: "cleanup.m",
              sourceRevision: AnalysisSourceRevision.make(source.revision),
              runtimeId: inspection.runtimes[0]!.id,
            });
            yield* client[WS_METHODS.subscribeAnalysisRuns]({
              cwd: workspaceDir,
              relativePath: "cleanup.m",
            }).pipe(
              Stream.filter(
                (event) =>
                  event._tag !== "run-output" &&
                  event.run.receipt.runId === started.receipt.runId &&
                  event.run.receipt.status === "succeeded",
              ),
              Stream.runHead,
              Effect.timeoutOrElse({
                duration: "10 seconds",
                orElse: () => Effect.die(new Error("Timed out waiting for cleanup fixture.")),
              }),
            );

            const beforeRun = yield* client[WS_METHODS.analysisGetRun]({
              cwd: workspaceDir,
              runId: started.receipt.runId,
            });
            const beforeStorage = yield* client[WS_METHODS.analysisStorageSummary]({
              cwd: workspaceDir,
            });
            const staleProjectCleanup = yield* client[WS_METHODS.analysisCleanupProject]({
              cwd: workspaceDir,
              expectedRetainedBytes: beforeStorage.totalBytes + 1,
            }).pipe(Effect.result);
            const firstCleanup = yield* client[WS_METHODS.analysisCleanupRun]({
              cwd: workspaceDir,
              runId: started.receipt.runId,
            });
            const afterRun = yield* client[WS_METHODS.analysisGetRun]({
              cwd: workspaceDir,
              runId: started.receipt.runId,
            });
            const listed = yield* client[WS_METHODS.analysisListRuns]({
              cwd: workspaceDir,
              relativePath: "cleanup.m",
            });
            const secondCleanup = yield* client[WS_METHODS.analysisCleanupRun]({
              cwd: workspaceDir,
              runId: started.receipt.runId,
            });
            const removedArtifact = yield* client[WS_METHODS.assetsCreateUrl]({
              resource: {
                _tag: "analysis-artifact",
                projectId: afterRun.projectId,
                runId: afterRun.receipt.runId,
                artifactId: afterRun.artifacts[0]!.artifactId,
                representationId: afterRun.artifacts[0]!.representations[0]!.representationId,
              },
            }).pipe(Effect.result);
            return {
              beforeRun,
              beforeStorage,
              staleProjectCleanup,
              firstCleanup,
              afterRun,
              listed,
              secondCleanup,
              removedArtifact,
            };
          }),
        ),
      );

      assert.isAbove(result.beforeStorage.totalBytes, 0);
      assert.equal(result.beforeStorage.retainedRunCount, 1);
      assert.equal(result.staleProjectCleanup._tag, "Failure");
      if (result.staleProjectCleanup._tag === "Failure") {
        assert.equal(result.staleProjectCleanup.failure._tag, "AnalysisOperationError");
        if (result.staleProjectCleanup.failure._tag === "AnalysisOperationError") {
          assert.equal(result.staleProjectCleanup.failure.reason, "operation-failed");
        }
      }
      assert.equal(result.firstCleanup.cleanedRunCount, 1);
      assert.equal(result.firstCleanup.freedBytes, result.beforeStorage.totalBytes);
      assert.deepEqual(result.firstCleanup.storage, {
        retainedRunCount: 0,
        metadataOnlyRunCount: 1,
        outputBytes: 0,
        artifactBytes: 0,
        totalBytes: 0,
      });
      assert.equal(result.afterRun.localStorage.status, "metadata-only");
      assert.deepEqual(result.afterRun.receipt.output, []);
      assert.equal(
        result.afterRun.receipt.outputByteLength,
        result.beforeRun.receipt.outputByteLength,
      );
      assert.equal(
        result.afterRun.receipt.outputContentHash,
        result.beforeRun.receipt.outputContentHash,
      );
      assert.deepEqual(result.afterRun.artifacts, result.beforeRun.artifacts);
      assert.equal(result.listed.runs[0]?.localStorage.status, "metadata-only");
      assert.equal(result.secondCleanup.cleanedRunCount, 0);
      assert.equal(result.secondCleanup.freedBytes, 0);
      assert.equal(result.removedArtifact._tag, "Failure");
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect(
    "verifies MATLAB explicitly and reuses the executable-identity cache until refresh",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const workspaceDir = yield* fs.makeTempDirectoryScoped({
          prefix: "scient-matlab-verify-project-",
        });
        const runtimeDir = yield* fs.makeTempDirectoryScoped({
          prefix: "scient-matlab-verify-runtime-",
        });
        const matlabExecutable = path.join(runtimeDir, "matlab");
        const launchesPath = path.join(runtimeDir, "launches.txt");
        yield* Effect.promise(() => initializeScientProject({ root: workspaceDir }));
        yield* fs.writeFileString(
          matlabExecutable,
          [
            "#!/usr/bin/env node",
            "const fs = require('node:fs');",
            "const path = require('node:path');",
            "fs.appendFileSync(path.join(path.dirname(__filename), 'launches.txt'), 'verify\\n');",
            "fs.writeFileSync(process.env.SCIENT_MATLAB_VERIFY_RESULT, JSON.stringify({",
            "  release: 'R2026a', version: '26.1', architecture: 'maca64',",
            "  installationRoot: '/Applications/MATLAB_R2026a.app',",
            "  javaAvailable: true, javaVersion: 'Java 21',",
            "  toolboxes: [{ name: 'MATLAB', version: '26.1' }]",
            "}));",
            "",
          ].join("\n"),
        );
        yield* fs.chmod(matlabExecutable, 0o755);

        yield* buildAppUnderTest();
        const wsUrl = yield* getWsServerUrl("/ws");
        const verifications = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            Effect.gen(function* () {
              const configured = yield* client[WS_METHODS.analysisConfigureRuntime]({
                cwd: workspaceDir,
                runtimeKind: "matlab",
                executablePath: matlabExecutable,
              });
              const runtimeId = configured.runtimes[0]!.id;
              const first = yield* client[WS_METHODS.analysisVerifyRuntime]({
                cwd: workspaceDir,
                runtimeId,
              });
              const cached = yield* client[WS_METHODS.analysisVerifyRuntime]({
                cwd: workspaceDir,
                runtimeId,
              });
              const refreshed = yield* client[WS_METHODS.analysisVerifyRuntime]({
                cwd: workspaceDir,
                runtimeId,
                refresh: true,
              });
              return { first, cached, refreshed };
            }),
          ),
        );

        assert.equal(verifications.first.verification?.status, "ready");
        assert.equal(verifications.first.verification?.release, "R2026a");
        assert.equal(verifications.first.verification?.javaAvailable, true);
        assert.deepEqual(verifications.first.verification?.toolboxes, [
          { name: "MATLAB", version: "26.1" },
        ]);
        assert.deepEqual(verifications.cached.verification, verifications.first.verification);
        assert.equal(verifications.refreshed.verification?.status, "ready");
        assert.equal((yield* fs.readFileString(launchesPath)).trim().split("\n").length, 2);
      }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect(
    "runs one MATLAB process at a time in FIFO order and skips a cancelled queued run",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const workspaceDir = yield* fs.makeTempDirectoryScoped({
          prefix: "scient-matlab-queue-project-",
        });
        const runtimeDir = yield* fs.makeTempDirectoryScoped({
          prefix: "scient-matlab-queue-runtime-",
        });
        const matlabExecutable = path.join(runtimeDir, "matlab");
        const orderPath = path.join(runtimeDir, "order.txt");
        yield* Effect.promise(() => initializeScientProject({ root: workspaceDir }));
        for (const fileName of ["first.m", "cancelled.m", "third.m"]) {
          yield* fs.writeFileString(path.join(workspaceDir, fileName), `disp('${fileName}');\n`);
        }
        yield* fs.writeFileString(
          matlabExecutable,
          [
            "#!/usr/bin/env node",
            "const fs = require('node:fs');",
            "const path = require('node:path');",
            "const runtimeDir = path.dirname(__filename);",
            "const active = path.join(runtimeDir, 'active');",
            "const order = path.join(runtimeDir, 'order.txt');",
            "const name = path.basename(process.env.SCIENT_MATLAB_ENTRYPOINT);",
            "if (fs.existsSync(active)) { fs.appendFileSync(order, `overlap:${name}\\n`); process.exit(9); }",
            "fs.writeFileSync(active, name);",
            "fs.appendFileSync(order, `start:${name}\\n`);",
            "setTimeout(() => {",
            "  fs.rmSync(active, { force: true });",
            "  fs.appendFileSync(order, `end:${name}\\n`);",
            "}, 250);",
            "",
          ].join("\n"),
        );
        yield* fs.chmod(matlabExecutable, 0o755);

        yield* buildAppUnderTest();
        const wsUrl = yield* getWsServerUrl("/ws");
        const result = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            Effect.gen(function* () {
              const inspection = yield* client[WS_METHODS.analysisConfigureRuntime]({
                cwd: workspaceDir,
                runtimeKind: "matlab",
                executablePath: matlabExecutable,
              });
              const runtimeId = inspection.runtimes[0]!.id;
              const revisions = new Map<string, AnalysisSourceRevision>();
              for (const fileName of ["first.m", "cancelled.m", "third.m"]) {
                const source = yield* client[WS_METHODS.projectsReadFile]({
                  cwd: workspaceDir,
                  relativePath: fileName,
                });
                revisions.set(fileName, AnalysisSourceRevision.make(source.revision));
              }
              const first = yield* client[WS_METHODS.analysisStartRun]({
                cwd: workspaceDir,
                relativePath: "first.m",
                sourceRevision: revisions.get("first.m")!,
                runtimeId,
              });
              const cancelled = yield* client[WS_METHODS.analysisStartRun]({
                cwd: workspaceDir,
                relativePath: "cancelled.m",
                sourceRevision: revisions.get("cancelled.m")!,
                runtimeId,
              });
              const third = yield* client[WS_METHODS.analysisStartRun]({
                cwd: workspaceDir,
                relativePath: "third.m",
                sourceRevision: revisions.get("third.m")!,
                runtimeId,
              });
              assert.isAtLeast(cancelled.queuePosition ?? 0, 1);
              assert.isAtLeast(third.queuePosition ?? 0, 2);
              const cancelledReceipt = yield* client[WS_METHODS.analysisCancelRun]({
                cwd: workspaceDir,
                runId: cancelled.receipt.runId,
              });
              assert.equal(cancelledReceipt.receipt.status, "cancelled");
              yield* Effect.sleep("1 second");
              return {
                first: yield* client[WS_METHODS.analysisGetRun]({
                  cwd: workspaceDir,
                  runId: first.receipt.runId,
                }),
                cancelled: yield* client[WS_METHODS.analysisGetRun]({
                  cwd: workspaceDir,
                  runId: cancelled.receipt.runId,
                }),
                third: yield* client[WS_METHODS.analysisGetRun]({
                  cwd: workspaceDir,
                  runId: third.receipt.runId,
                }),
              };
            }),
          ),
        );

        assert.equal(result.first.receipt.status, "succeeded");
        assert.equal(result.cancelled.receipt.status, "cancelled");
        assert.equal(result.third.receipt.status, "succeeded");
        assert.deepEqual((yield* fs.readFileString(orderPath)).trim().split("\n"), [
          "start:first.m",
          "end:first.m",
          "start:third.m",
          "end:third.m",
        ]);
      }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("keeps captured MATLAB figures when the source later fails", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-failed-artifact-project-",
      });
      const runtimeDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-failed-artifact-runtime-",
      });
      const matlabExecutable = path.join(runtimeDir, "matlab");
      yield* Effect.promise(() => initializeScientProject({ root: workspaceDir }));
      yield* fs.writeFileString(path.join(workspaceDir, "partial.m"), "error('after plot');\n");
      yield* fs.writeFileString(
        matlabExecutable,
        [
          "#!/usr/bin/env node",
          "const fs = require('node:fs');",
          "const artifacts = process.env.SCIENT_MATLAB_ARTIFACT_DIR;",
          "fs.mkdirSync(artifacts, { recursive: true });",
          "fs.writeFileSync(`${artifacts}/figure-001.png`, Buffer.from([137, 80, 78, 71]));",
          "fs.writeFileSync(`${artifacts}/scient-capture-failed.txt`, 'partial capture');",
          "fs.writeFileSync(process.env.SCIENT_MATLAB_DIAGNOSTIC_PATH, JSON.stringify({ identifier: 'Scient:AfterPlot', message: 'source failed after drawing', stack: [{ file: process.env.SCIENT_MATLAB_ENTRYPOINT, name: 'partial', line: 1 }] }));",
          "console.error('source failed after drawing');",
          "process.exit(7);",
          "",
        ].join("\n"),
      );
      yield* fs.chmod(matlabExecutable, 0o755);

      yield* buildAppUnderTest();
      const wsUrl = yield* getWsServerUrl("/ws");
      const completed = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const inspection = yield* client[WS_METHODS.analysisConfigureRuntime]({
              cwd: workspaceDir,
              runtimeKind: "matlab",
              executablePath: matlabExecutable,
            });
            const source = yield* client[WS_METHODS.projectsReadFile]({
              cwd: workspaceDir,
              relativePath: "partial.m",
            });
            const started = yield* client[WS_METHODS.analysisStartRun]({
              cwd: workspaceDir,
              relativePath: "partial.m",
              sourceRevision: AnalysisSourceRevision.make(source.revision),
              runtimeId: inspection.runtimes[0]!.id,
            });
            yield* client[WS_METHODS.subscribeAnalysisRuns]({
              cwd: workspaceDir,
              relativePath: "partial.m",
            }).pipe(
              Stream.filter(
                (event) =>
                  event._tag === "run-updated" &&
                  event.run.receipt.runId === started.receipt.runId &&
                  event.run.receipt.status === "failed",
              ),
              Stream.runHead,
              Effect.timeoutOrElse({
                duration: "10 seconds",
                orElse: () => Effect.die(new Error("Timed out waiting for failed MATLAB run.")),
              }),
            );
            return yield* client[WS_METHODS.analysisGetRun]({
              cwd: workspaceDir,
              runId: started.receipt.runId,
            });
          }),
        ),
      );

      assert.equal(completed.receipt.status, "failed");
      assert.equal(completed.receipt.exitCode, 7);
      assert.equal(completed.artifacts.length, 1);
      assert.equal(completed.artifacts[0]?.representations[0]?.mediaType, "image/png");
      assert.equal(completed.artifactReceipt.status, "failed");
      assert.include(completed.artifactReceipt.failureMessage ?? "", "could not capture every");
      assert.equal(completed.diagnostics.length, 1);
      assert.equal(completed.diagnostics[0]?.severity, "error");
      assert.equal(completed.diagnostics[0]?.code, "Scient:AfterPlot");
      assert.equal(completed.diagnostics[0]?.message, "source failed after drawing");
      assert.equal(completed.diagnostics[0]?.relativePath, "partial.m");
      assert.equal(completed.diagnostics[0]?.line, 1);
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("records artifact publication failure without falsifying a successful MATLAB run", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-artifact-failure-project-",
      });
      const runtimeDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-artifact-failure-runtime-",
      });
      const matlabExecutable = path.join(runtimeDir, "matlab");
      yield* Effect.promise(() => initializeScientProject({ root: workspaceDir }));
      yield* fs.writeFileString(path.join(workspaceDir, "analysis.m"), "answer = 42;\n");
      yield* fs.writeFileString(
        matlabExecutable,
        [
          "#!/usr/bin/env node",
          "const fs = require('node:fs');",
          "const artifacts = process.env.SCIENT_MATLAB_ARTIFACT_DIR;",
          "fs.mkdirSync(artifacts, { recursive: true });",
          "fs.mkdirSync(`${artifacts}/figure-001.png`);",
          "process.exit(0);",
          "",
        ].join("\n"),
      );
      yield* fs.chmod(matlabExecutable, 0o755);

      yield* buildAppUnderTest();
      const wsUrl = yield* getWsServerUrl("/ws");
      const completed = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const inspection = yield* client[WS_METHODS.analysisConfigureRuntime]({
              cwd: workspaceDir,
              runtimeKind: "matlab",
              executablePath: matlabExecutable,
            });
            const source = yield* client[WS_METHODS.projectsReadFile]({
              cwd: workspaceDir,
              relativePath: "analysis.m",
            });
            const started = yield* client[WS_METHODS.analysisStartRun]({
              cwd: workspaceDir,
              relativePath: "analysis.m",
              sourceRevision: AnalysisSourceRevision.make(source.revision),
              runtimeId: inspection.runtimes[0]!.id,
            });
            assert.deepEqual(started.artifactReceipt, {
              status: "pending",
              failureMessage: null,
            });
            yield* client[WS_METHODS.subscribeAnalysisRuns]({
              cwd: workspaceDir,
              relativePath: "analysis.m",
            }).pipe(
              Stream.filter(
                (event) =>
                  event._tag !== "run-output" &&
                  event.run.receipt.runId === started.receipt.runId &&
                  event.run.receipt.status === "succeeded",
              ),
              Stream.runHead,
              Effect.timeoutOrElse({
                duration: "10 seconds",
                orElse: () => Effect.die(new Error("Timed out waiting for MATLAB run.")),
              }),
            );
            return yield* client[WS_METHODS.analysisGetRun]({
              cwd: workspaceDir,
              runId: started.receipt.runId,
            });
          }),
        ),
      );

      assert.equal(completed.receipt.status, "succeeded");
      assert.equal(completed.receipt.failureMessage, null);
      assert.deepEqual(completed.artifacts, []);
      assert.equal(completed.artifactReceipt.status, "failed");
      assert.include(completed.artifactReceipt.failureMessage ?? "", "could not collect");
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("refuses to run a MATLAB source revision that changed after it was opened", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-stale-source-",
      });
      const runtimeDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-stale-runtime-",
      });
      const matlabExecutable = path.join(runtimeDir, "matlab");
      yield* Effect.promise(() => initializeScientProject({ root: workspaceDir }));
      yield* fs.writeFileString(path.join(workspaceDir, "analysis.m"), "answer = 1;\n");
      yield* fs.writeFileString(matlabExecutable, "#!/usr/bin/env node\nprocess.exit(0);\n");
      yield* fs.chmod(matlabExecutable, 0o755);

      yield* buildAppUnderTest();
      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const inspection = yield* client[WS_METHODS.analysisConfigureRuntime]({
              cwd: workspaceDir,
              runtimeKind: "matlab",
              executablePath: matlabExecutable,
            });
            const opened = yield* client[WS_METHODS.projectsReadFile]({
              cwd: workspaceDir,
              relativePath: "analysis.m",
            });
            yield* fs.writeFileString(path.join(workspaceDir, "analysis.m"), "answer = 2;\n");
            return yield* client[WS_METHODS.analysisStartRun]({
              cwd: workspaceDir,
              relativePath: "analysis.m",
              sourceRevision: AnalysisSourceRevision.make(opened.revision),
              runtimeId: inspection.runtimes[0]!.id,
            }).pipe(Effect.result);
          }),
        ),
      );

      assert.isTrue(result._tag === "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure._tag, "AnalysisOperationError");
        if (result.failure._tag === "AnalysisOperationError") {
          assert.equal(result.failure.reason, "source-changed");
        }
      }
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("bounds persisted and streamed MATLAB output without losing terminal state", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-bounded-output-project-",
      });
      const runtimeDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-bounded-output-runtime-",
      });
      const matlabExecutable = path.join(runtimeDir, "matlab");
      yield* Effect.promise(() => initializeScientProject({ root: workspaceDir }));
      yield* fs.writeFileString(path.join(workspaceDir, "verbose.m"), "disp('verbose');\n");
      yield* fs.writeFileString(
        matlabExecutable,
        "#!/usr/bin/env node\nprocess.stdout.write('x'.repeat(5 * 1024 * 1024));\n",
      );
      yield* fs.chmod(matlabExecutable, 0o755);

      yield* buildAppUnderTest();
      const wsUrl = yield* getWsServerUrl("/ws");
      const completed = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const inspection = yield* client[WS_METHODS.analysisConfigureRuntime]({
              cwd: workspaceDir,
              runtimeKind: "matlab",
              executablePath: matlabExecutable,
            });
            const source = yield* client[WS_METHODS.projectsReadFile]({
              cwd: workspaceDir,
              relativePath: "verbose.m",
            });
            const started = yield* client[WS_METHODS.analysisStartRun]({
              cwd: workspaceDir,
              relativePath: "verbose.m",
              sourceRevision: AnalysisSourceRevision.make(source.revision),
              runtimeId: inspection.runtimes[0]!.id,
            });
            yield* client[WS_METHODS.subscribeAnalysisRuns]({
              cwd: workspaceDir,
              relativePath: "verbose.m",
            }).pipe(
              Stream.filter(
                (event) =>
                  event._tag === "run-updated" &&
                  event.run.receipt.runId === started.receipt.runId &&
                  ["succeeded", "failed", "cancelled", "lost"].includes(event.run.receipt.status),
              ),
              Stream.runHead,
              Effect.timeoutOrElse({
                duration: "10 seconds",
                orElse: () => Effect.die(new Error("Timed out waiting for bounded MATLAB output.")),
              }),
            );
            return yield* client[WS_METHODS.analysisGetRun]({
              cwd: workspaceDir,
              runId: started.receipt.runId,
            });
          }),
        ),
      );

      assert.equal(completed.receipt.status, "succeeded");
      assert.equal(completed.receipt.outputByteLength, 4 * 1024 * 1024);
      assert.isTrue(completed.receipt.outputTruncated);
      assert.include(
        completed.receipt.output.map((chunk) => chunk.text).join(""),
        "Output was truncated at the 4194304-byte limit.",
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("cancels a MATLAB run even when Stop races process startup", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-cancel-project-",
      });
      const runtimeDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-matlab-cancel-runtime-",
      });
      const matlabExecutable = path.join(runtimeDir, "matlab");
      yield* Effect.promise(() => initializeScientProject({ root: workspaceDir }));
      yield* fs.writeFileString(path.join(workspaceDir, "slow.m"), "pause(60);\n");
      yield* fs.writeFileString(
        matlabExecutable,
        "#!/usr/bin/env node\nconsole.log('runtime-started');\nsetInterval(() => {}, 1000);\n",
      );
      yield* fs.chmod(matlabExecutable, 0o755);

      yield* buildAppUnderTest();
      const wsUrl = yield* getWsServerUrl("/ws");
      const terminalRun = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const inspection = yield* client[WS_METHODS.analysisConfigureRuntime]({
              cwd: workspaceDir,
              runtimeKind: "matlab",
              executablePath: matlabExecutable,
            });
            const source = yield* client[WS_METHODS.projectsReadFile]({
              cwd: workspaceDir,
              relativePath: "slow.m",
            });
            const started = yield* client[WS_METHODS.analysisStartRun]({
              cwd: workspaceDir,
              relativePath: "slow.m",
              sourceRevision: AnalysisSourceRevision.make(source.revision),
              runtimeId: inspection.runtimes[0]!.id,
            });
            const duplicate = yield* client[WS_METHODS.analysisStartRun]({
              cwd: workspaceDir,
              relativePath: "slow.m",
              sourceRevision: AnalysisSourceRevision.make(source.revision),
              runtimeId: inspection.runtimes[0]!.id,
            }).pipe(Effect.result);
            assert.equal(duplicate._tag, "Failure");
            if (duplicate._tag === "Failure") {
              assert.equal(duplicate.failure._tag, "AnalysisOperationError");
              if (duplicate.failure._tag === "AnalysisOperationError") {
                assert.equal(duplicate.failure.reason, "run-already-active");
              }
            }
            yield* client[WS_METHODS.analysisCancelRun]({
              cwd: workspaceDir,
              runId: started.receipt.runId,
            });
            const events = yield* client[WS_METHODS.subscribeAnalysisRuns]({
              cwd: workspaceDir,
              relativePath: "slow.m",
            }).pipe(
              Stream.filterMap((event) =>
                event._tag !== "run-output" && event.run.receipt.runId === started.receipt.runId
                  ? Result.succeed(event.run)
                  : Result.failVoid,
              ),
              Stream.takeUntil((run) =>
                ["succeeded", "failed", "cancelled", "lost"].includes(run.receipt.status),
              ),
              Stream.runCollect,
              Effect.timeoutOrElse({
                duration: "10 seconds",
                orElse: () => Effect.die(new Error("Timed out waiting for MATLAB cancellation.")),
              }),
            );
            return events.at(-1)!;
          }),
        ),
      );

      assert.equal(terminalRun.receipt.status, "cancelled");
      assert.isTrue(terminalRun.receipt.cancellationRequested);
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("creates a missing workspace root during websocket project.create dispatch", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const parentDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ws-project-create-" });
      const missingWorkspaceRoot = path.join(parentDir, "nested", "new-project");

      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "project.create",
            commandId: CommandId.make("cmd-project-create-missing-root"),
            projectId: ProjectId.make("project-create-missing-root"),
            title: "New Project",
            workspaceRoot: missingWorkspaceRoot,
            createWorkspaceRootIfMissing: true,
            defaultModelSelection: {
              instanceId: ProviderInstanceId.make("codex"),
              model: "gpt-5-codex",
            },
            createdAt: "2026-01-01T00:00:00.000Z",
          }),
        ),
      );
      const stat = yield* fs.stat(missingWorkspaceRoot);

      assert.isAtLeast(response.sequence, 0);
      assert.equal(stat.type, "Directory");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("acknowledges websocket thread.fork only after provisioning completes", () =>
    Effect.gen(function* () {
      const provisioningStarted = yield* Deferred.make<void>();
      const allowProvisioningToComplete = yield* Deferred.make<void>();
      const rpcCompleted = yield* Deferred.make<void>();
      const effects: string[] = [];
      const forkThreadId = ThreadId.make("thread-fork-ready-gate");

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            dispatch: (command) =>
              Effect.sync(() => {
                effects.push(`dispatch:${command.type}`);
                return { sequence: 41 };
              }),
          },
          scientForkReactor: {
            awaitCompletion: (threadId) =>
              Effect.gen(function* () {
                effects.push(`await:${threadId}`);
                yield* Deferred.succeed(provisioningStarted, undefined);
                yield* Deferred.await(allowProvisioningToComplete);
                effects.push(`ready:${threadId}`);
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          Effect.gen(function* () {
            const rpcFiber = yield* client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
              type: "thread.fork",
              commandId: CommandId.make("cmd-thread-fork-ready-gate"),
              originThreadId: ThreadId.make("thread-fork-ready-gate-origin"),
              newThreadId: forkThreadId,
              sourceAssistantMessageId: MessageId.make("assistant-fork-ready-gate"),
              workspaceMode: "local",
            }).pipe(
              Effect.tap(() => Deferred.succeed(rpcCompleted, undefined)),
              Effect.forkChild,
            );

            yield* Deferred.await(provisioningStarted);
            assert.isTrue(Option.isNone(yield* Deferred.poll(rpcCompleted)));
            yield* Deferred.succeed(allowProvisioningToComplete, undefined);
            return yield* Fiber.join(rpcFiber);
          }),
        ),
      );

      assert.equal(response.sequence, 41);
      assert.deepEqual(effects, [
        "dispatch:thread.fork",
        `await:${forkThreadId}`,
        `ready:${forkThreadId}`,
      ]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("returns websocket thread.fork provisioning failures to the caller", () =>
    Effect.gen(function* () {
      const forkThreadId = ThreadId.make("thread-fork-failed-gate");

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            dispatch: () => Effect.succeed({ sequence: 42 }),
          },
          scientForkReactor: {
            awaitCompletion: () =>
              Effect.fail(
                new ScientForkReactor.ScientForkCompletionError({
                  threadId: forkThreadId,
                  detail: "Fork workspace provisioning failed.",
                }),
              ),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.fork",
            commandId: CommandId.make("cmd-thread-fork-failed-gate"),
            originThreadId: ThreadId.make("thread-fork-failed-gate-origin"),
            newThreadId: forkThreadId,
            sourceAssistantMessageId: MessageId.make("assistant-fork-failed-gate"),
            workspaceMode: "new-worktree",
          }),
        ).pipe(Effect.result),
      );

      assertTrue(result._tag === "Failure");
      assertTrue(result.failure._tag === "OrchestrationDispatchCommandError");
      assert.include(result.failure.message, "Fork workspace provisioning failed.");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc projects.writeFile errors", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const workspaceDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ws-project-write-" });

      yield* buildAppUnderTest();

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.projectsWriteFile]({
            cwd: workspaceDir,
            relativePath: "../escape.txt",
            contents: "nope",
          }),
        ).pipe(Effect.result),
      );

      if (result._tag !== "Failure" || result.failure._tag !== "ProjectWriteFileError") {
        assert.fail("Expected a ProjectWriteFileError");
      }
      const writeError = result.failure;
      assert.equal(
        writeError.message,
        `Failed to write workspace file '../escape.txt' in '${workspaceDir}'.`,
      );
      assert.equal(writeError.cwd, workspaceDir);
      assert.equal(writeError.relativePath, "../escape.txt");
      assert.equal(writeError.failure, "workspace_path_outside_root");
      assert.isDefined(writeError.cause);
      assert.notProperty(writeError, "contents");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc shell.openInEditor", () =>
    Effect.gen(function* () {
      let openedInput: { cwd: string; editor: EditorId } | null = null;
      yield* buildAppUnderTest({
        layers: {
          externalLauncher: {
            launchEditor: (input) =>
              Effect.sync(() => {
                openedInput = input;
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.shellOpenInEditor]({
            cwd: "/tmp/project",
            editor: "cursor",
          }),
        ),
      );

      assert.deepEqual(openedInput, { cwd: "/tmp/project", editor: "cursor" });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc shell.openInEditor errors", () =>
    Effect.gen(function* () {
      const externalLauncherError = new ExternalLauncherCommandNotFoundError({
        editor: "cursor",
        command: "cursor",
      });
      yield* buildAppUnderTest({
        layers: {
          externalLauncher: {
            launchEditor: () => Effect.fail(externalLauncherError),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.shellOpenInEditor]({
            cwd: "/tmp/project",
            editor: "cursor",
          }),
        ).pipe(Effect.result),
      );

      assertFailure(result, externalLauncherError);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc git methods", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        config: {
          cwd: "/tmp/repo",
        },
        layers: {
          vcsDriver: {
            isInsideWorkTree: () => Effect.succeed(true),
          },
          gitManager: {
            invalidateLocalStatus: () => Effect.void,
            invalidateRemoteStatus: () => Effect.void,
            invalidateStatus: () => Effect.void,
            localStatus: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: true,
                refName: "main",
                hasWorkingTreeChanges: false,
                workingTree: { files: [], insertions: 0, deletions: 0 },
              }),
            remoteStatus: () =>
              Effect.succeed({
                hasUpstream: true,
                aheadCount: 0,
                behindCount: 0,
                pr: null,
              }),
            status: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: true,
                refName: "main",
                hasWorkingTreeChanges: false,
                workingTree: { files: [], insertions: 0, deletions: 0 },
                hasUpstream: true,
                aheadCount: 0,
                behindCount: 0,
                pr: null,
              }),
            runStackedAction: (input, options) =>
              Effect.gen(function* () {
                const result = {
                  action: "commit" as const,
                  branch: { status: "skipped_not_requested" as const },
                  commit: {
                    status: "created" as const,
                    commitSha: "abc123",
                    subject: "feat: demo",
                  },
                  push: { status: "skipped_not_requested" as const },
                  pr: { status: "skipped_not_requested" as const },
                  toast: {
                    title: "Committed abc123",
                    description: "feat: demo",
                    cta: {
                      kind: "run_action" as const,
                      label: "Push",
                      action: {
                        kind: "push" as const,
                      },
                    },
                  },
                };

                yield* (
                  options?.progressReporter?.publish({
                    actionId: options.actionId ?? input.actionId,
                    cwd: input.cwd,
                    action: input.action,
                    kind: "phase_started",
                    phase: "commit",
                    label: "Committing...",
                  }) ?? Effect.void
                );

                yield* (
                  options?.progressReporter?.publish({
                    actionId: options.actionId ?? input.actionId,
                    cwd: input.cwd,
                    action: input.action,
                    kind: "action_finished",
                    result,
                  }) ?? Effect.void
                );

                return result;
              }),
            resolvePullRequest: () =>
              Effect.succeed({
                pullRequest: {
                  number: 1,
                  title: "Demo PR",
                  url: "https://example.com/pr/1",
                  baseBranch: "main",
                  headBranch: "feature/demo",
                  state: "open",
                },
              }),
            preparePullRequestThread: () =>
              Effect.succeed({
                pullRequest: {
                  number: 1,
                  title: "Demo PR",
                  url: "https://example.com/pr/1",
                  baseBranch: "main",
                  headBranch: "feature/demo",
                  state: "open",
                },
                branch: "feature/demo",
                worktreePath: null,
                isOnPullRequestHead: true,
              }),
          },
          gitVcsDriver: {
            pullCurrentBranch: () =>
              Effect.succeed({
                status: "pulled",
                refName: "main",
                upstreamRef: "origin/main",
              }),
            listRefs: () =>
              Effect.succeed({
                refs: [
                  {
                    name: "main",
                    current: true,
                    isDefault: true,
                    worktreePath: null,
                  },
                ],
                isRepo: true,
                hasPrimaryRemote: true,
                nextCursor: null,
                totalCount: 1,
              }),
            createWorktree: () =>
              Effect.succeed({
                worktree: { path: "/tmp/wt", refName: "feature/demo" },
              }),
            removeWorktree: () => Effect.void,
            createRef: (input) => Effect.succeed({ refName: input.refName }),
            switchRef: (input) => Effect.succeed({ refName: input.refName }),
          },
          vcsStatusBroadcaster: {
            refreshStatus: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: true,
                refName: "main",
                hasWorkingTreeChanges: false,
                workingTree: { files: [], insertions: 0, deletions: 0 },
                hasUpstream: true,
                aheadCount: 0,
                behindCount: 0,
                pr: null,
              }),
          },
          reviewService: {
            getDiffPreview: (input) =>
              Effect.succeed({
                cwd: input.cwd,
                generatedAt: DateTime.nowUnsafe(),
                sources: [
                  {
                    id: "working-tree",
                    kind: "working-tree",
                    title: "Dirty worktree",
                    baseRef: "HEAD",
                    headRef: null,
                    diff: "dirty-diff",
                    diffHash: "hash-dirty",
                    truncated: false,
                  },
                  {
                    id: "branch-range",
                    kind: "branch-range",
                    title: "Against main",
                    baseRef: "main",
                    headRef: "feature/demo",
                    diff: "base-diff",
                    diffHash: "hash-base",
                    truncated: false,
                  },
                ],
              }),
            getDiffFileContents: () =>
              Effect.succeed({
                oldContents: "before\n",
                newContents: "after\n",
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");

      const pull = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.vcsPull]({ cwd: "/tmp/repo" })),
      );
      assert.equal(pull.status, "pulled");

      const refreshedStatus = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.vcsRefreshStatus]({ cwd: "/tmp/repo" }),
        ),
      );
      assert.equal(refreshedStatus.isRepo, true);

      const stackedEvents = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.gitRunStackedAction]({
            actionId: "action-1",
            cwd: "/tmp/repo",
            action: "commit",
          }).pipe(
            Stream.runCollect,
            Effect.map((events) => Array.from(events)),
          ),
        ),
      );
      const lastStackedEvent = stackedEvents.at(-1);
      assert.equal(lastStackedEvent?.kind, "action_finished");
      if (lastStackedEvent?.kind === "action_finished") {
        assert.equal(lastStackedEvent.result.action, "commit");
      }

      const resolvedPr = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.gitResolvePullRequest]({
            cwd: "/tmp/repo",
            reference: "1",
          }),
        ),
      );
      assert.equal(resolvedPr.pullRequest.number, 1);

      const prepared = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.gitPreparePullRequestThread]({
            cwd: "/tmp/repo",
            reference: "1",
            mode: "local",
          }),
        ),
      );
      assert.equal(prepared.branch, "feature/demo");

      const refs = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.vcsListRefs]({ cwd: "/tmp/repo" })),
      );
      assert.equal(refs.refs[0]?.name, "main");

      const worktree = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.vcsCreateWorktree]({
            cwd: "/tmp/repo",
            refName: "main",
            path: null,
          }),
        ),
      );
      assert.equal(worktree.worktree.refName, "feature/demo");

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.vcsRemoveWorktree]({
            cwd: "/tmp/repo",
            path: "/tmp/wt",
          }),
        ),
      );

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.vcsCreateRef]({
            cwd: "/tmp/repo",
            refName: "feature/new",
          }),
        ),
      );

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.vcsSwitchRef]({
            cwd: "/tmp/repo",
            refName: "main",
          }),
        ),
      );

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.vcsInit]({
            cwd: "/tmp/repo",
          }),
        ),
      );

      const diffPreview = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.reviewGetDiffPreview]({ cwd: "/tmp/repo" }),
        ),
      );
      assert.equal(diffPreview.sources[0]?.diff, "dirty-diff");

      const diffFileContents = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.reviewGetDiffFileContents]({
            cwd: "/tmp/repo",
            sourceKind: "working-tree",
            changeType: "change",
            baseRef: "HEAD",
            headRef: null,
            oldPath: "README.md",
            newPath: "README.md",
          }),
        ),
      );
      assert.equal(diffFileContents.oldContents, "before\n");
      assert.equal(diffFileContents.newContents, "after\n");
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc git.pull errors", () =>
    Effect.gen(function* () {
      const gitError = new GitCommandError({
        operation: "pull",
        command: "git pull --ff-only",
        cwd: "/tmp/repo",
        detail: "upstream missing",
      });
      let invalidationCalls = 0;
      let statusCalls = 0;
      yield* buildAppUnderTest({
        layers: {
          gitVcsDriver: {
            pullCurrentBranch: () => Effect.fail(gitError),
          },
          gitManager: {
            invalidateLocalStatus: () =>
              Effect.sync(() => {
                invalidationCalls += 1;
              }),
            invalidateRemoteStatus: () =>
              Effect.sync(() => {
                invalidationCalls += 1;
              }),
            invalidateStatus: () =>
              Effect.sync(() => {
                invalidationCalls += 1;
              }),
            localStatus: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: true,
                refName: "main",
                hasWorkingTreeChanges: true,
                workingTree: { files: [], insertions: 0, deletions: 0 },
              }),
            remoteStatus: () =>
              Effect.sync(() => {
                statusCalls += 1;
                return {
                  hasUpstream: true,
                  aheadCount: 0,
                  behindCount: 0,
                  pr: null,
                };
              }),
            status: () =>
              Effect.sync(() => {
                statusCalls += 1;
                return {
                  isRepo: true,
                  hasPrimaryRemote: true,
                  isDefaultRef: true,
                  refName: "main",
                  hasWorkingTreeChanges: true,
                  workingTree: { files: [], insertions: 0, deletions: 0 },
                  hasUpstream: true,
                  aheadCount: 0,
                  behindCount: 0,
                  pr: null,
                };
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.vcsPull]({ cwd: "/tmp/repo" })).pipe(
          Effect.result,
        ),
      );

      assertFailure(result, gitError);
      assert.equal(invalidationCalls, 0);
      assert.equal(statusCalls, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc git.runStackedAction errors after refreshing git status", () =>
    Effect.gen(function* () {
      const gitError = new GitCommandError({
        operation: "commit",
        command: "git commit",
        cwd: "/tmp/repo",
        detail: "nothing to commit",
      });
      let invalidationCalls = 0;
      let statusCalls = 0;
      yield* buildAppUnderTest({
        layers: {
          gitManager: {
            invalidateLocalStatus: () =>
              Effect.sync(() => {
                invalidationCalls += 1;
              }),
            invalidateRemoteStatus: () =>
              Effect.sync(() => {
                invalidationCalls += 1;
              }),
            invalidateStatus: () =>
              Effect.sync(() => {
                invalidationCalls += 1;
              }),
            localStatus: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: false,
                refName: "feature/demo",
                hasWorkingTreeChanges: true,
                workingTree: { files: [], insertions: 0, deletions: 0 },
              }),
            remoteStatus: () =>
              Effect.sync(() => {
                statusCalls += 1;
                return {
                  hasUpstream: true,
                  aheadCount: 0,
                  behindCount: 0,
                  pr: null,
                };
              }),
            status: () =>
              Effect.sync(() => {
                statusCalls += 1;
                return {
                  isRepo: true,
                  hasPrimaryRemote: true,
                  isDefaultRef: false,
                  refName: "feature/demo",
                  hasWorkingTreeChanges: true,
                  workingTree: { files: [], insertions: 0, deletions: 0 },
                  hasUpstream: true,
                  aheadCount: 0,
                  behindCount: 0,
                  pr: null,
                };
              }),
            runStackedAction: () => Effect.fail(gitError),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.gitRunStackedAction]({
            actionId: "action-1",
            cwd: "/tmp/repo",
            action: "commit",
          }).pipe(Stream.runCollect, Effect.result),
        ),
      );

      assertFailure(result, gitError);
      assert.equal(invalidationCalls, 0);
      assert.equal(statusCalls, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("completes websocket rpc git.pull before background git status refresh finishes", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        layers: {
          gitVcsDriver: {
            pullCurrentBranch: () =>
              Effect.succeed({
                status: "pulled" as const,
                refName: "main",
                upstreamRef: "origin/main",
              }),
          },
          gitManager: {
            invalidateLocalStatus: () => Effect.void,
            invalidateRemoteStatus: () => Effect.void,
            invalidateStatus: () => Effect.void,
            localStatus: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: true,
                refName: "main",
                hasWorkingTreeChanges: false,
                workingTree: { files: [], insertions: 0, deletions: 0 },
              }),
            remoteStatus: () =>
              Effect.sleep(Duration.seconds(2)).pipe(
                Effect.as({
                  hasUpstream: true,
                  aheadCount: 0,
                  behindCount: 0,
                  pr: null,
                }),
              ),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const startedAt = yield* Clock.currentTimeMillis;
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) => client[WS_METHODS.vcsPull]({ cwd: "/tmp/repo" })),
      );
      const elapsedMs = (yield* Clock.currentTimeMillis) - startedAt;

      assert.equal(result.status, "pulled");
      assertTrue(elapsedMs < 1_000);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "completes websocket rpc git.runStackedAction before background git status refresh finishes",
    () =>
      Effect.gen(function* () {
        yield* buildAppUnderTest({
          layers: {
            vcsDriver: {
              isInsideWorkTree: () => Effect.succeed(true),
            },
            gitManager: {
              invalidateLocalStatus: () => Effect.void,
              invalidateRemoteStatus: () => Effect.void,
              invalidateStatus: () => Effect.void,
              localStatus: () =>
                Effect.succeed({
                  isRepo: true,
                  hasPrimaryRemote: true,
                  isDefaultRef: false,
                  refName: "feature/demo",
                  hasWorkingTreeChanges: false,
                  workingTree: { files: [], insertions: 0, deletions: 0 },
                }),
              remoteStatus: () =>
                Effect.sleep(Duration.seconds(2)).pipe(
                  Effect.as({
                    hasUpstream: true,
                    aheadCount: 0,
                    behindCount: 0,
                    pr: null,
                  }),
                ),
              runStackedAction: () =>
                Effect.succeed({
                  action: "commit" as const,
                  branch: { status: "skipped_not_requested" as const },
                  commit: {
                    status: "created" as const,
                    commitSha: "abc123",
                    subject: "feat: demo",
                  },
                  push: { status: "skipped_not_requested" as const },
                  pr: { status: "skipped_not_requested" as const },
                  toast: {
                    title: "Committed abc123",
                    description: "feat: demo",
                    cta: {
                      kind: "run_action" as const,
                      label: "Push",
                      action: {
                        kind: "push" as const,
                      },
                    },
                  },
                }),
            },
          },
        });

        const wsUrl = yield* getWsServerUrl("/ws");
        const startedAt = yield* Clock.currentTimeMillis;
        yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.gitRunStackedAction]({
              actionId: "action-1",
              cwd: "/tmp/repo",
              action: "commit",
            }).pipe(Stream.runCollect),
          ),
        );
        const elapsedMs = (yield* Clock.currentTimeMillis) - startedAt;

        assertTrue(elapsedMs < 1_000);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "starts a background local git status refresh after a successful git.runStackedAction",
    () =>
      Effect.gen(function* () {
        const localRefreshStarted = yield* Deferred.make<void>();

        yield* buildAppUnderTest({
          layers: {
            vcsDriver: {
              isInsideWorkTree: () => Effect.succeed(true),
            },
            gitManager: {
              invalidateLocalStatus: () => Effect.void,
              invalidateRemoteStatus: () => Effect.void,
              invalidateStatus: () => Effect.void,
              localStatus: () =>
                Deferred.succeed(localRefreshStarted, undefined).pipe(
                  Effect.ignore,
                  Effect.andThen(
                    Effect.succeed({
                      isRepo: true,
                      hasPrimaryRemote: true,
                      isDefaultRef: false,
                      refName: "feature/demo",
                      hasWorkingTreeChanges: false,
                      workingTree: { files: [], insertions: 0, deletions: 0 },
                    }),
                  ),
                ),
              remoteStatus: () =>
                Effect.sleep(Duration.seconds(2)).pipe(
                  Effect.as({
                    hasUpstream: true,
                    aheadCount: 0,
                    behindCount: 0,
                    pr: null,
                  }),
                ),
              runStackedAction: () =>
                Effect.succeed({
                  action: "commit" as const,
                  branch: { status: "skipped_not_requested" as const },
                  commit: {
                    status: "created" as const,
                    commitSha: "abc123",
                    subject: "feat: demo",
                  },
                  push: { status: "skipped_not_requested" as const },
                  pr: { status: "skipped_not_requested" as const },
                  toast: {
                    title: "Committed abc123",
                    description: "feat: demo",
                    cta: {
                      kind: "run_action" as const,
                      label: "Push",
                      action: {
                        kind: "push" as const,
                      },
                    },
                  },
                }),
            },
          },
        });

        const wsUrl = yield* getWsServerUrl("/ws");
        yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[WS_METHODS.gitRunStackedAction]({
              actionId: "action-1",
              cwd: "/tmp/repo",
              action: "commit",
            }).pipe(Stream.runCollect),
          ),
        );

        yield* Deferred.await(localRefreshStarted);
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc orchestration methods", () =>
    Effect.gen(function* () {
      const now = "2026-01-01T00:00:00.000Z";
      const snapshot = {
        snapshotSequence: 1,
        updatedAt: now,
        projects: [
          {
            id: ProjectId.make("project-a"),
            title: "Project A",
            workspaceRoot: "/tmp/project-a",
            defaultModelSelection,
            scripts: [],
            createdAt: now,
            updatedAt: now,
            deletedAt: null,
          },
        ],
        threads: [
          {
            id: ThreadId.make("thread-1"),
            projectId: ProjectId.make("project-a"),
            title: "Thread A",
            modelSelection: defaultModelSelection,
            interactionMode: "default" as const,
            runtimeMode: "full-access" as const,
            branch: null,
            worktreePath: null,
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            latestTurn: null,
            messages: [],
            session: null,
            activities: [],
            proposedPlans: [],
            checkpoints: [],
            deletedAt: null,
          },
        ],
      };

      yield* buildAppUnderTest({
        layers: {
          projectionSnapshotQuery: {
            getSnapshot: () => Effect.succeed(snapshot),
            searchThreads: () =>
              Effect.succeed({
                matches: [
                  {
                    threadId: ThreadId.make("thread-1"),
                    projectId: ProjectId.make("project-a"),
                    source: "assistant",
                    snippet: "Search reached the final response.",
                    messageCreatedAt: now,
                  },
                ],
              }),
          },
          orchestrationEngine: {
            dispatch: () => Effect.succeed({ sequence: 7 }),
            readEvents: () => Stream.empty,
          },
          checkpointDiffQuery: {
            getTurnDiff: () =>
              Effect.succeed({
                threadId: ThreadId.make("thread-1"),
                fromTurnCount: 0,
                toTurnCount: 1,
                diff: "turn-diff",
              }),
            getFullThreadDiff: () =>
              Effect.succeed({
                threadId: ThreadId.make("thread-1"),
                fromTurnCount: 0,
                toTurnCount: 1,
                diff: "full-diff",
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const dispatchResult = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.session.stop",
            commandId: CommandId.make("cmd-1"),
            threadId: ThreadId.make("thread-1"),
            createdAt: now,
          }),
        ),
      );
      assert.equal(dispatchResult.sequence, 7);

      const turnDiffResult = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.getTurnDiff]({
            threadId: ThreadId.make("thread-1"),
            fromTurnCount: 0,
            toTurnCount: 1,
          }),
        ),
      );
      assert.equal(turnDiffResult.diff, "turn-diff");

      const fullDiffResult = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.getFullThreadDiff]({
            threadId: ThreadId.make("thread-1"),
            toTurnCount: 1,
          }),
        ),
      );
      assert.equal(fullDiffResult.diff, "full-diff");

      const searchResult = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.searchThreads]({
            query: "final response",
          }),
        ),
      );
      assert.deepEqual(searchResult.matches, [
        {
          threadId: ThreadId.make("thread-1"),
          projectId: ProjectId.make("project-a"),
          source: "assistant",
          snippet: "Search reached the final response.",
          messageCreatedAt: now,
        },
      ]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc orchestration shell snapshot errors", () =>
    Effect.gen(function* () {
      const projectionError = new PersistenceSqlError({
        operation: "ProjectionSnapshotQuery.getShellSnapshot:test",
        detail: "failed to read projection shell snapshot",
      });
      yield* buildAppUnderTest({
        layers: {
          projectionSnapshotQuery: {
            getShellSnapshot: () => Effect.fail(projectionError),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeShell]({}).pipe(Stream.runCollect),
        ).pipe(Effect.result),
      );

      assertTrue(result._tag === "Failure");
      assertTrue(result.failure._tag === "OrchestrationGetSnapshotError");
      assertTrue(result.failure.cause instanceof Error);
      assert.include(result.failure.cause.message, projectionError.message);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("marks an empty shell catch-up replay as synchronized when requested", () =>
    Effect.gen(function* () {
      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            readEvents: () => Stream.empty,
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const firstItem = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeShell]({
            afterSequence: 0,
            requestCompletionMarker: true,
          }).pipe(Stream.runHead),
        ),
      );

      assert.deepEqual(Option.getOrThrow(firstItem), { kind: "synchronized" });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("marks a socket thread snapshot as synchronized when requested", () =>
    Effect.gen(function* () {
      const thread = makeDefaultOrchestrationReadModel().threads[0]!;
      yield* buildAppUnderTest({
        layers: {
          projectionSnapshotQuery: {
            getThreadDetailSnapshot: () =>
              Effect.succeed(Option.some({ snapshotSequence: 1, thread })),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const items = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeThread]({
            threadId: defaultThreadId,
            requestCompletionMarker: true,
          }).pipe(Stream.take(2), Stream.runCollect),
        ),
      );

      assert.equal(items[0]?.kind, "snapshot");
      assert.deepEqual(items[1], { kind: "synchronized" });
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("buffers shell events published while the fallback snapshot loads", () =>
    Effect.gen(function* () {
      const liveEvents = yield* PubSub.unbounded<OrchestrationEvent>();
      const deletedEvent = {
        sequence: 2,
        eventId: EventId.make("event-shell-thread-deleted"),
        aggregateKind: "thread",
        aggregateId: defaultThreadId,
        occurredAt: "2026-01-01T00:00:01.000Z",
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "thread.deleted",
        payload: {
          threadId: defaultThreadId,
          deletedAt: "2026-01-01T00:00:01.000Z",
        },
      } satisfies Extract<OrchestrationEvent, { type: "thread.deleted" }>;

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            streamDomainEvents: Stream.fromPubSub(liveEvents),
          },
          projectionSnapshotQuery: {
            getShellSnapshot: () =>
              Effect.gen(function* () {
                yield* PubSub.publish(liveEvents, deletedEvent);
                return {
                  snapshotSequence: 1,
                  projects: [],
                  threads: [makeDefaultOrchestrationThreadShell()],
                  updatedAt: "2026-01-01T00:00:00.000Z",
                };
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const items = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeShell]({
            requestCompletionMarker: true,
          }).pipe(Stream.take(3), Stream.runCollect),
        ),
      ).pipe(Effect.timeout("2 seconds"));

      assert.equal(items[0]?.kind, "snapshot");
      assert.equal(items[1]?.kind, "thread-removed");
      assert.deepEqual(items[2], { kind: "synchronized" });
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("buffers thread events published while the initial snapshot loads", () =>
    Effect.gen(function* () {
      const thread = makeDefaultOrchestrationReadModel().threads[0]!;
      const liveEvents = yield* PubSub.unbounded<OrchestrationEvent>();
      const messageEvent = {
        sequence: 2,
        eventId: EventId.make("event-message"),
        aggregateKind: "thread",
        aggregateId: defaultThreadId,
        occurredAt: "2026-01-01T00:00:01.000Z",
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "thread.message-sent",
        payload: {
          threadId: defaultThreadId,
          messageId: MessageId.make("message-1"),
          role: "user",
          text: "First message",
          turnId: null,
          streaming: false,
          createdAt: "2026-01-01T00:00:01.000Z",
          updatedAt: "2026-01-01T00:00:01.000Z",
        },
      } satisfies Extract<OrchestrationEvent, { type: "thread.message-sent" }>;

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            streamDomainEvents: Stream.fromPubSub(liveEvents),
          },
          projectionSnapshotQuery: {
            getThreadDetailSnapshot: () =>
              Effect.gen(function* () {
                yield* Effect.sleep("25 millis");
                yield* PubSub.publish(liveEvents, messageEvent);
                return Option.some({ snapshotSequence: 1, thread });
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const items = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeThread]({
            threadId: defaultThreadId,
          }).pipe(Stream.take(2), Stream.runCollect),
        ),
      ).pipe(Effect.timeout("2 seconds"));

      assert.equal(items[0]?.kind, "snapshot");
      assert.equal(items[1]?.kind, "event");
      assert.equal(items[1]?.kind === "event" ? items[1].event.sequence : null, 2);
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("subscribeThread sends a fresh snapshot instead of replaying a large gap", () =>
    Effect.gen(function* () {
      let readEventsCalls = 0;
      const thread = makeDefaultOrchestrationReadModel().threads[0]!;

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            // Head is far ahead of the client's afterSequence (gap > 1000).
            latestSequence: Effect.succeed(100_000),
            readEvents: () =>
              Stream.sync(() => {
                readEventsCalls += 1;
                return {} as OrchestrationEvent;
              }),
          },
          projectionSnapshotQuery: {
            getThreadDetailSnapshot: () =>
              Effect.succeed(Option.some({ snapshotSequence: 100_000, thread })),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const items = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeThread]({
            threadId: defaultThreadId,
            afterSequence: 5,
            requestCompletionMarker: true,
          }).pipe(Stream.take(2), Stream.runCollect),
        ),
      );

      const [first, second] = Array.from(items);
      // Large gap => fresh thread snapshot, and the global replay never starts.
      assert.equal(first?.kind, "snapshot");
      if (first?.kind === "snapshot") {
        assert.equal(first.snapshot.thread.id, defaultThreadId);
        assert.equal(first.snapshot.snapshotSequence, 100_000);
      }
      assert.equal(second?.kind, "synchronized");
      assert.equal(readEventsCalls, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeThread replaces a cursor ahead of the authoritative head", () =>
    Effect.gen(function* () {
      let readEventsCalls = 0;
      const thread = makeDefaultOrchestrationReadModel().threads[0]!;

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            latestSequence: Effect.succeed(5),
            readEvents: () =>
              Stream.sync(() => {
                readEventsCalls += 1;
                return {} as OrchestrationEvent;
              }),
          },
          projectionSnapshotQuery: {
            getThreadDetailSnapshot: () =>
              Effect.succeed(Option.some({ snapshotSequence: 5, thread })),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const first = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeThread]({
            threadId: defaultThreadId,
            afterSequence: 10,
          }).pipe(Stream.runHead),
        ),
      );

      assert.equal(Option.getOrThrow(first).kind, "snapshot");
      assert.equal(readEventsCalls, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeThread bounds catch-up replay to the captured head", () =>
    Effect.gen(function* () {
      let replayLimit: number | undefined;
      const now = "2026-01-01T00:00:00.000Z";
      const messageEvent = {
        sequence: 3,
        eventId: EventId.make("event-replay-message"),
        aggregateKind: "thread",
        aggregateId: defaultThreadId,
        occurredAt: now,
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "thread.message-sent",
        payload: {
          threadId: defaultThreadId,
          messageId: MessageId.make("message-replay"),
          role: "user",
          text: "Replayed message",
          turnId: null,
          streaming: false,
          createdAt: now,
          updatedAt: now,
        },
      } satisfies Extract<OrchestrationEvent, { type: "thread.message-sent" }>;

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            latestSequence: Effect.succeed(50),
            readEvents: (_afterSequence, limit) => {
              replayLimit = limit;
              return Stream.make(messageEvent);
            },
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const items = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeThread]({
            threadId: defaultThreadId,
            afterSequence: 0,
            requestCompletionMarker: true,
          }).pipe(Stream.take(2), Stream.runCollect),
        ),
      );

      const [first, second] = Array.from(items);
      assert.equal(first?.kind, "event");
      assert.equal(first?.kind === "event" ? first.event.sequence : null, 3);
      assert.equal(second?.kind, "synchronized");
      // The replay is bounded to the head captured before the read, not
      // Number.MAX_SAFE_INTEGER.
      assert.equal(replayLimit, 50);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeShell sends a fresh snapshot instead of replaying a large gap", () =>
    Effect.gen(function* () {
      let readEventsCalls = 0;
      const snapshotThreadId = ThreadId.make("thread-from-snapshot");
      const now = "2026-01-01T00:00:00.000Z";

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            // Head is far ahead of the client's afterSequence (gap > 1000).
            latestSequence: Effect.succeed(100_000),
            readEvents: () =>
              Stream.sync(() => {
                readEventsCalls += 1;
                return {
                  sequence: 1,
                  eventId: EventId.make("event-should-not-be-read"),
                  aggregateKind: "thread",
                  aggregateId: snapshotThreadId,
                  occurredAt: now,
                  commandId: null,
                  causationEventId: null,
                  correlationId: null,
                  metadata: {},
                  type: "thread.created",
                  payload: {} as never,
                } satisfies OrchestrationEvent;
              }),
          },
          projectionSnapshotQuery: {
            getShellSnapshot: () =>
              Effect.succeed({
                snapshotSequence: 100_000,
                projects: [],
                threads: [makeDefaultOrchestrationThreadShell({ id: snapshotThreadId })],
                updatedAt: now,
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const items = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeShell]({
            afterSequence: 5,
            requestCompletionMarker: true,
          }).pipe(Stream.take(2), Stream.runCollect),
        ),
      );

      const [first, second] = Array.from(items);
      // Large gap => fresh snapshot, and the unbounded replay is never started.
      assert.equal(first?.kind, "snapshot");
      if (first?.kind === "snapshot") {
        assert.equal(first.snapshot.threads[0]?.id, snapshotThreadId);
      }
      assert.equal(second?.kind, "synchronized");
      assert.equal(readEventsCalls, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeShell replaces a cursor ahead of the authoritative head", () =>
    Effect.gen(function* () {
      let readEventsCalls = 0;

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            latestSequence: Effect.succeed(5),
            readEvents: () =>
              Stream.sync(() => {
                readEventsCalls += 1;
                return {} as OrchestrationEvent;
              }),
          },
          projectionSnapshotQuery: {
            getShellSnapshot: () =>
              Effect.succeed({
                snapshotSequence: 5,
                projects: [],
                threads: [],
                updatedAt: "2026-01-01T00:00:00.000Z",
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const first = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeShell]({ afterSequence: 10 }).pipe(
            Stream.runHead,
          ),
        ),
      );

      assert.equal(Option.getOrThrow(first).kind, "snapshot");
      assert.equal(readEventsCalls, 0);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeShell coalesces a per-thread burst without stalling other threads", () =>
    Effect.gen(function* () {
      const busyThreadId = ThreadId.make("thread-busy");
      const newThreadId = ThreadId.make("thread-new");
      const now = "2026-01-01T00:00:00.000Z";
      const shellFetches: Array<string> = [];
      let replayLimit: number | undefined;

      const messageEvent = (sequence: number): OrchestrationEvent =>
        ({
          sequence,
          eventId: EventId.make(`event-${sequence}`),
          aggregateKind: "thread",
          aggregateId: busyThreadId,
          occurredAt: now,
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          type: "thread.message-sent",
          payload: {} as never,
        }) satisfies OrchestrationEvent;

      const createdEvent: OrchestrationEvent = {
        sequence: 50,
        eventId: EventId.make("event-created"),
        aggregateKind: "thread",
        aggregateId: newThreadId,
        occurredAt: now,
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "thread.created",
        payload: {} as never,
      };

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            latestSequence: Effect.succeed(50),
            // A burst of message-sent deltas for the busy thread, plus one
            // thread.created for a different thread, all within one batch.
            readEvents: (_afterSequence, limit) => {
              replayLimit = limit;
              return Stream.fromIterable([
                ...Array.from({ length: 20 }, (_unused, index) => messageEvent(index + 1)),
                createdEvent,
              ]);
            },
          },
          projectionSnapshotQuery: {
            getThreadShellById: (threadId) =>
              Effect.sync(() => {
                shellFetches.push(threadId);
                return Option.some(makeDefaultOrchestrationThreadShell({ id: threadId }));
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const items = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeShell]({
            afterSequence: 0,
            requestCompletionMarker: true,
          }).pipe(Stream.take(3), Stream.runCollect),
        ),
      );

      const collected = Array.from(items);
      const upsertedIds = collected.flatMap((item) =>
        item.kind === "thread-upserted" ? [item.thread.id] : [],
      );
      // Both threads surface, and the busy thread's 20-event burst collapses to
      // a single shell refetch (not 20). The new thread is not stuck behind it.
      assert.include(upsertedIds, busyThreadId);
      assert.include(upsertedIds, newThreadId);
      assert.equal(collected[2]?.kind, "synchronized");
      assert.equal(shellFetches.filter((id) => id === busyThreadId).length, 1);
      assert.equal(replayLimit, 50);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeShell coalesces live bursts after the synchronization marker", () =>
    Effect.gen(function* () {
      const busyThreadId = ThreadId.make("thread-live-busy");
      const newThreadId = ThreadId.make("thread-live-new");
      const now = "2026-01-01T00:00:00.000Z";
      const liveEvents = yield* PubSub.unbounded<OrchestrationEvent>();
      const synchronized = yield* Deferred.make<void>();
      const shellFetches: Array<string> = [];
      const observedLiveThreadIds = new Set<string>();

      const messageEvent = (sequence: number): OrchestrationEvent =>
        ({
          sequence,
          eventId: EventId.make(`event-live-${sequence}`),
          aggregateKind: "thread",
          aggregateId: busyThreadId,
          occurredAt: now,
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          type: "thread.message-sent",
          payload: {} as never,
        }) satisfies OrchestrationEvent;

      const createdEvent: OrchestrationEvent = {
        sequence: 50,
        eventId: EventId.make("event-live-created"),
        aggregateKind: "thread",
        aggregateId: newThreadId,
        occurredAt: now,
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "thread.created",
        payload: {} as never,
      };

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            streamDomainEvents: Stream.fromPubSub(liveEvents),
          },
          projectionSnapshotQuery: {
            getThreadShellById: (threadId) =>
              Effect.sync(() => {
                shellFetches.push(threadId);
                return Option.some(makeDefaultOrchestrationThreadShell({ id: threadId }));
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const items = yield* Effect.scoped(
        Effect.gen(function* () {
          const itemsFiber = yield* withWsRpcClient(wsUrl, (client) =>
            client[ORCHESTRATION_WS_METHODS.subscribeShell]({
              requestCompletionMarker: true,
            }).pipe(
              Stream.tap((item) =>
                item.kind === "synchronized"
                  ? Deferred.succeed(synchronized, undefined).pipe(Effect.ignore)
                  : Effect.void,
              ),
              Stream.takeUntil((item) => {
                if (item.kind === "thread-upserted") {
                  observedLiveThreadIds.add(item.thread.id);
                }
                return (
                  observedLiveThreadIds.has(busyThreadId) && observedLiveThreadIds.has(newThreadId)
                );
              }),
              Stream.runCollect,
            ),
          ).pipe(Effect.forkScoped);

          yield* Deferred.await(synchronized);
          for (const event of [
            ...Array.from({ length: 20 }, (_unused, index) => messageEvent(index + 1)),
            createdEvent,
          ]) {
            yield* PubSub.publish(liveEvents, event);
          }

          return yield* Fiber.join(itemsFiber);
        }),
      ).pipe(Effect.timeout("2 seconds"));

      assert.equal(items[0]?.kind, "snapshot");
      assert.equal(items[1]?.kind, "synchronized");
      const liveUpsertedIds = Array.from(items)
        .slice(2)
        .flatMap((item) => (item.kind === "thread-upserted" ? [item.thread.id] : []));
      assert.include(liveUpsertedIds, busyThreadId);
      assert.include(liveUpsertedIds, newThreadId);
      assert.isBelow(shellFetches.filter((id) => id === busyThreadId).length, 20);
    }).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
  );

  it.effect("subscribeShell coalescing still emits a removal for a deleted thread", () =>
    Effect.gen(function* () {
      const goneThreadId = ThreadId.make("thread-gone");
      const now = "2026-01-01T00:00:00.000Z";

      const makeThreadEvent = (
        sequence: number,
        type: "thread.deleted" | "thread.message-sent",
      ): OrchestrationEvent =>
        ({
          sequence,
          eventId: EventId.make(`event-${sequence}`),
          aggregateKind: "thread",
          aggregateId: goneThreadId,
          occurredAt: now,
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          type,
          payload: type === "thread.deleted" ? { threadId: goneThreadId, deletedAt: now } : {},
        }) as OrchestrationEvent;

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            latestSequence: Effect.succeed(2),
            // A thread.deleted followed, within the same coalescing window, by a
            // later refetchable event for the same thread. The later event wins
            // coalescing; its shell refetch returns none (the row is gone), which
            // must still surface a removal rather than be swallowed.
            readEvents: () =>
              Stream.fromIterable([
                makeThreadEvent(1, "thread.deleted"),
                makeThreadEvent(2, "thread.message-sent"),
              ]),
          },
          projectionSnapshotQuery: {
            getThreadShellById: () => Effect.succeed(Option.none()),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const items = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeShell]({ afterSequence: 0 }).pipe(
            Stream.take(1),
            Stream.runCollect,
          ),
        ),
      );

      const [first] = Array.from(items);
      assert.equal(first?.kind, "thread-removed");
      assert.equal(first?.kind === "thread-removed" ? first.threadId : null, goneThreadId);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeShell retries a transient shell projection refetch failure", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-transient-refetch");
      const now = "2026-01-01T00:00:00.000Z";
      let attempts = 0;

      const event: OrchestrationEvent = {
        sequence: 1,
        eventId: EventId.make("event-transient-refetch"),
        aggregateKind: "thread",
        aggregateId: threadId,
        occurredAt: now,
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "thread.message-sent",
        payload: {} as never,
      };

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            latestSequence: Effect.succeed(1),
            readEvents: () => Stream.make(event),
          },
          projectionSnapshotQuery: {
            getThreadShellById: () =>
              Effect.suspend(() => {
                attempts += 1;
                return attempts === 1
                  ? Effect.fail(
                      new PersistenceSqlError({
                        operation: "test.shell-refetch",
                        detail: "transient failure",
                      }),
                    )
                  : Effect.succeed(
                      Option.some(makeDefaultOrchestrationThreadShell({ id: threadId })),
                    );
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const items = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeShell]({ afterSequence: 0 }).pipe(
            Stream.take(1),
            Stream.runCollect,
          ),
        ),
      );

      const [first] = Array.from(items);
      assert.equal(first?.kind, "thread-upserted");
      assert.equal(first?.kind === "thread-upserted" ? first.thread.id : null, threadId);
      assert.equal(attempts, 2);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("subscribeShell coalescing still removes a project after a trailing update", () =>
    Effect.gen(function* () {
      const projectId = ProjectId.make("project-gone");
      const now = "2026-01-01T00:00:00.000Z";

      const makeProjectEvent = (
        sequence: number,
        type: "project.deleted" | "project.meta-updated",
      ): OrchestrationEvent =>
        ({
          sequence,
          eventId: EventId.make(`event-project-${sequence}`),
          aggregateKind: "project",
          aggregateId: projectId,
          occurredAt: now,
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          type,
          payload:
            type === "project.deleted"
              ? { projectId, deletedAt: now }
              : { projectId, title: "Still deleted", updatedAt: now },
        }) as OrchestrationEvent;

      yield* buildAppUnderTest({
        layers: {
          orchestrationEngine: {
            latestSequence: Effect.succeed(2),
            readEvents: () =>
              Stream.fromIterable([
                makeProjectEvent(1, "project.deleted"),
                makeProjectEvent(2, "project.meta-updated"),
              ]),
          },
          projectionSnapshotQuery: {
            getProjectShellById: () => Effect.succeed(Option.none()),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const items = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.subscribeShell]({ afterSequence: 0 }).pipe(
            Stream.take(1),
            Stream.runCollect,
          ),
        ),
      );

      const [first] = Array.from(items);
      assert.equal(first?.kind, "project-removed");
      assert.equal(first?.kind === "project-removed" ? first.projectId : null, projectId);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("stops the provider session and closes thread terminals after archive", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-archive");
      const effects: string[] = [];
      const dispatchedCommands: Array<OrchestrationCommand> = [];
      const now = "2026-01-01T00:00:00.000Z";

      yield* buildAppUnderTest({
        layers: {
          terminalManager: {
            close: (input) =>
              Effect.sync(() => {
                effects.push(`terminal.close:${input.threadId}`);
              }),
          },
          orchestrationEngine: {
            dispatch: (command) =>
              Effect.sync(() => {
                dispatchedCommands.push(command);
                effects.push(`dispatch:${command.type}`);
                return { sequence: dispatchedCommands.length };
              }),
          },
          projectionSnapshotQuery: {
            getThreadShellById: () =>
              Effect.succeed(
                Option.some(
                  makeDefaultOrchestrationThreadShell({
                    id: threadId,
                    updatedAt: now,
                    session: {
                      threadId,
                      status: "ready",
                      providerName: "claudeAgent",
                      runtimeMode: "full-access",
                      activeTurnId: null,
                      lastError: null,
                      updatedAt: now,
                    },
                  }),
                ),
              ),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const dispatchResult = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.archive",
            commandId: CommandId.make("cmd-thread-archive"),
            threadId,
          }),
        ),
      );

      assert.equal(dispatchResult.sequence, 1);
      assert.deepEqual(effects, [
        "dispatch:thread.archive",
        "dispatch:thread.session.stop",
        `terminal.close:${threadId}`,
      ]);
      const sessionStopCommand = dispatchedCommands[1];
      assert.equal(sessionStopCommand?.type, "thread.session.stop");
      if (sessionStopCommand?.type === "thread.session.stop") {
        assert.equal(sessionStopCommand.threadId, threadId);
      }
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("rejects moving a General Chat while a terminal is running", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-general-chat-terminal-open");
      let dispatched = false;

      yield* buildAppUnderTest({
        layers: {
          terminalManager: {
            hasRunningSessionsForThread: (candidateThreadId) =>
              Effect.succeed(candidateThreadId === threadId),
          },
          orchestrationEngine: {
            dispatch: () =>
              Effect.sync(() => {
                dispatched = true;
                return { sequence: 1 };
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.meta.update",
            commandId: CommandId.make("cmd-move-general-chat-terminal-open"),
            threadId,
            moveToProjectId: ProjectId.make("project-destination"),
          }),
        ).pipe(Effect.result),
      );

      assertTrue(result._tag === "Failure");
      assertTrue(result.failure._tag === "OrchestrationDispatchCommandError");
      assert.include(result.failure.message, "Close every terminal");
      assert.equal(dispatched, false);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("checks session status before archiving removes the thread from active lookups", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-archive-precheck");
      const effects: string[] = [];
      const dispatchedCommands: Array<OrchestrationCommand> = [];
      const now = "2026-01-01T00:00:00.000Z";
      let archived = false;

      yield* buildAppUnderTest({
        layers: {
          terminalManager: {
            close: (input) =>
              Effect.sync(() => {
                effects.push(`terminal.close:${input.threadId}`);
              }),
          },
          orchestrationEngine: {
            dispatch: (command) =>
              Effect.sync(() => {
                dispatchedCommands.push(command);
                effects.push(`dispatch:${command.type}`);
                if (command.type === "thread.archive") {
                  archived = true;
                }
                return { sequence: dispatchedCommands.length };
              }),
          },
          projectionSnapshotQuery: {
            getThreadShellById: () =>
              Effect.sync(() => {
                effects.push(`query:thread-shell:${archived ? "archived" : "active"}`);
                return archived
                  ? Option.none()
                  : Option.some(
                      makeDefaultOrchestrationThreadShell({
                        id: threadId,
                        updatedAt: now,
                        session: {
                          threadId,
                          status: "ready",
                          providerName: "claudeAgent",
                          runtimeMode: "full-access",
                          activeTurnId: null,
                          lastError: null,
                          updatedAt: now,
                        },
                      }),
                    );
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const dispatchResult = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.archive",
            commandId: CommandId.make("cmd-thread-archive-precheck"),
            threadId,
          }),
        ),
      );

      assert.equal(dispatchResult.sequence, 1);
      assert.deepEqual(effects, [
        "query:thread-shell:active",
        "dispatch:thread.archive",
        "dispatch:thread.session.stop",
        `terminal.close:${threadId}`,
      ]);
      assert.deepEqual(
        dispatchedCommands.map((command) => command.type),
        ["thread.archive", "thread.session.stop"],
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("archives without dispatching session stop when the thread has no session", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-archive-no-session");
      const effects: string[] = [];
      const dispatchedCommands: Array<OrchestrationCommand> = [];

      yield* buildAppUnderTest({
        layers: {
          terminalManager: {
            close: (input) =>
              Effect.sync(() => {
                effects.push(`terminal.close:${input.threadId}`);
              }),
          },
          orchestrationEngine: {
            dispatch: (command) =>
              Effect.sync(() => {
                dispatchedCommands.push(command);
                effects.push(`dispatch:${command.type}`);
                return { sequence: dispatchedCommands.length };
              }),
          },
          projectionSnapshotQuery: {
            getThreadShellById: () =>
              Effect.succeed(
                Option.some(makeDefaultOrchestrationThreadShell({ id: threadId, session: null })),
              ),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const dispatchResult = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.archive",
            commandId: CommandId.make("cmd-thread-archive-no-session"),
            threadId,
          }),
        ),
      );

      assert.equal(dispatchResult.sequence, 1);
      assert.deepEqual(effects, ["dispatch:thread.archive", `terminal.close:${threadId}`]);
      assert.deepEqual(
        dispatchedCommands.map((command) => command.type),
        ["thread.archive"],
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "archives without dispatching session stop when the thread session is already stopped",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("thread-archive-stopped-session");
        const effects: string[] = [];
        const dispatchedCommands: Array<OrchestrationCommand> = [];
        const now = "2026-01-01T00:00:00.000Z";

        yield* buildAppUnderTest({
          layers: {
            terminalManager: {
              close: (input) =>
                Effect.sync(() => {
                  effects.push(`terminal.close:${input.threadId}`);
                }),
            },
            orchestrationEngine: {
              dispatch: (command) =>
                Effect.sync(() => {
                  dispatchedCommands.push(command);
                  effects.push(`dispatch:${command.type}`);
                  return { sequence: dispatchedCommands.length };
                }),
            },
            projectionSnapshotQuery: {
              getThreadShellById: () =>
                Effect.succeed(
                  Option.some(
                    makeDefaultOrchestrationThreadShell({
                      id: threadId,
                      updatedAt: now,
                      session: {
                        threadId,
                        status: "stopped",
                        providerName: "claudeAgent",
                        runtimeMode: "full-access",
                        activeTurnId: null,
                        lastError: null,
                        updatedAt: now,
                      },
                    }),
                  ),
                ),
            },
          },
        });

        const wsUrl = yield* getWsServerUrl("/ws");
        const dispatchResult = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
              type: "thread.archive",
              commandId: CommandId.make("cmd-thread-archive-stopped-session"),
              threadId,
            }),
          ),
        );

        assert.equal(dispatchResult.sequence, 1);
        assert.deepEqual(effects, ["dispatch:thread.archive", `terminal.close:${threadId}`]);
        assert.deepEqual(
          dispatchedCommands.map((command) => command.type),
          ["thread.archive"],
        );
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("stops the provider session after settle without closing terminals", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-settle");
      const effects: string[] = [];
      const dispatchedCommands: Array<OrchestrationCommand> = [];
      const now = "2026-01-01T00:00:00.000Z";

      yield* buildAppUnderTest({
        layers: {
          terminalManager: {
            close: (input) =>
              Effect.sync(() => {
                effects.push(`terminal.close:${input.threadId}`);
              }),
          },
          orchestrationEngine: {
            dispatch: (command) =>
              Effect.sync(() => {
                dispatchedCommands.push(command);
                effects.push(`dispatch:${command.type}`);
                return { sequence: dispatchedCommands.length };
              }),
          },
          projectionSnapshotQuery: {
            getThreadShellById: () =>
              Effect.succeed(
                Option.some(
                  makeDefaultOrchestrationThreadShell({
                    id: threadId,
                    updatedAt: now,
                    session: {
                      threadId,
                      status: "ready",
                      providerName: "claudeAgent",
                      runtimeMode: "full-access",
                      activeTurnId: null,
                      lastError: null,
                      updatedAt: now,
                    },
                  }),
                ),
              ),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const dispatchResult = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.settle",
            commandId: CommandId.make("cmd-thread-settle"),
            threadId,
          }),
        ),
      );

      assert.equal(dispatchResult.sequence, 1);
      assert.deepEqual(effects, ["dispatch:thread.settle", "dispatch:thread.session.stop"]);
      const sessionStopCommand = dispatchedCommands[1];
      assert.equal(sessionStopCommand?.type, "thread.session.stop");
      if (sessionStopCommand?.type === "thread.session.stop") {
        assert.equal(sessionStopCommand.threadId, threadId);
        assert.equal(sessionStopCommand.commandId, "session-stop-for-settle:cmd-thread-settle");
        assert.equal(sessionStopCommand.onlyIfSettled, true);
      }
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("settles without dispatching session stop when the thread has no session", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-settle-no-session");
      const effects: string[] = [];
      const dispatchedCommands: Array<OrchestrationCommand> = [];

      yield* buildAppUnderTest({
        layers: {
          terminalManager: {
            close: (input) =>
              Effect.sync(() => {
                effects.push(`terminal.close:${input.threadId}`);
              }),
          },
          orchestrationEngine: {
            dispatch: (command) =>
              Effect.sync(() => {
                dispatchedCommands.push(command);
                effects.push(`dispatch:${command.type}`);
                return { sequence: dispatchedCommands.length };
              }),
          },
          projectionSnapshotQuery: {
            getThreadShellById: () =>
              Effect.succeed(
                Option.some(makeDefaultOrchestrationThreadShell({ id: threadId, session: null })),
              ),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const dispatchResult = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.settle",
            commandId: CommandId.make("cmd-thread-settle-no-session"),
            threadId,
          }),
        ),
      );

      assert.equal(dispatchResult.sequence, 1);
      assert.deepEqual(effects, ["dispatch:thread.settle"]);
      assert.deepEqual(
        dispatchedCommands.map((command) => command.type),
        ["thread.settle"],
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("archives and still closes terminals when session stop fails", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-archive-stop-failure");
      const effects: string[] = [];
      const dispatchedCommands: Array<OrchestrationCommand> = [];
      const now = "2026-01-01T00:00:00.000Z";

      yield* buildAppUnderTest({
        layers: {
          terminalManager: {
            close: (input) =>
              Effect.sync(() => {
                effects.push(`terminal.close:${input.threadId}`);
              }),
          },
          orchestrationEngine: {
            dispatch: (command) => {
              dispatchedCommands.push(command);
              effects.push(`dispatch:${command.type}`);
              if (command.type === "thread.session.stop") {
                return Effect.fail(
                  new OrchestrationListenerCallbackError({
                    listener: "domain-event",
                    detail: "simulated archive stop failure",
                  }),
                );
              }
              return Effect.succeed({ sequence: dispatchedCommands.length });
            },
          },
          projectionSnapshotQuery: {
            getThreadShellById: () =>
              Effect.succeed(
                Option.some(
                  makeDefaultOrchestrationThreadShell({
                    id: threadId,
                    updatedAt: now,
                    session: {
                      threadId,
                      status: "ready",
                      providerName: "claudeAgent",
                      runtimeMode: "full-access",
                      activeTurnId: null,
                      lastError: null,
                      updatedAt: now,
                    },
                  }),
                ),
              ),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const dispatchResult = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.archive",
            commandId: CommandId.make("cmd-thread-archive-stop-failure"),
            threadId,
          }),
        ),
      );

      assert.equal(dispatchResult.sequence, 1);
      assert.deepEqual(effects, [
        "dispatch:thread.archive",
        "dispatch:thread.session.stop",
        `terminal.close:${threadId}`,
      ]);
      assert.deepEqual(
        dispatchedCommands.map((command) => command.type),
        ["thread.archive", "thread.session.stop"],
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("archives and still closes terminals when session stop defects", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-archive-stop-defect");
      const effects: string[] = [];
      const dispatchedCommands: Array<OrchestrationCommand> = [];
      const now = "2026-01-01T00:00:00.000Z";

      yield* buildAppUnderTest({
        layers: {
          terminalManager: {
            close: (input) =>
              Effect.sync(() => {
                effects.push(`terminal.close:${input.threadId}`);
              }),
          },
          orchestrationEngine: {
            dispatch: (command) => {
              dispatchedCommands.push(command);
              effects.push(`dispatch:${command.type}`);
              if (command.type === "thread.session.stop") {
                return Effect.die(new Error("simulated archive stop defect"));
              }
              return Effect.succeed({ sequence: dispatchedCommands.length });
            },
          },
          projectionSnapshotQuery: {
            getThreadShellById: () =>
              Effect.succeed(
                Option.some(
                  makeDefaultOrchestrationThreadShell({
                    id: threadId,
                    updatedAt: now,
                    session: {
                      threadId,
                      status: "ready",
                      providerName: "claudeAgent",
                      runtimeMode: "full-access",
                      activeTurnId: null,
                      lastError: null,
                      updatedAt: now,
                    },
                  }),
                ),
              ),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const dispatchResult = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.archive",
            commandId: CommandId.make("cmd-thread-archive-stop-defect"),
            threadId,
          }),
        ),
      );

      assert.equal(dispatchResult.sequence, 1);
      assert.deepEqual(effects, [
        "dispatch:thread.archive",
        "dispatch:thread.session.stop",
        `terminal.close:${threadId}`,
      ]);
      assert.deepEqual(
        dispatchedCommands.map((command) => command.type),
        ["thread.archive", "thread.session.stop"],
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "bootstraps first-send worktree turns on the server before dispatching turn start",
    () =>
      Effect.gen(function* () {
        const dispatchedCommands: Array<OrchestrationCommand> = [];
        const bootstrapGitOperations: string[] = [];
        const refreshStatus = vi.fn((_: string) =>
          Effect.succeed({
            isRepo: true,
            hasPrimaryRemote: true,
            isDefaultRef: false,
            refName: "t3code/bootstrap-refName",
            hasWorkingTreeChanges: false,
            workingTree: {
              files: [],
              insertions: 0,
              deletions: 0,
            },
            hasUpstream: true,
            aheadCount: 0,
            behindCount: 0,
            pr: null,
          }),
        );
        const remoteExists = vi.fn(
          (_: Parameters<GitVcsDriver.GitVcsDriver["Service"]["remoteExists"]>[0]) =>
            Effect.sync(() => {
              bootstrapGitOperations.push("remote-exists");
              return true;
            }),
        );
        const fetchRemote = vi.fn(
          (_: Parameters<GitVcsDriver.GitVcsDriver["Service"]["fetchRemote"]>[0]) =>
            Effect.sync(() => {
              bootstrapGitOperations.push("fetch");
            }),
        );
        const fetchedOriginCommit = "0123456789abcdef0123456789abcdef01234567";
        const resolveRemoteTrackingCommit = vi.fn(
          (_: Parameters<GitVcsDriver.GitVcsDriver["Service"]["resolveRemoteTrackingCommit"]>[0]) =>
            Effect.sync(() => {
              bootstrapGitOperations.push("resolve-remote-commit");
              return {
                commitSha: fetchedOriginCommit,
                remoteRefName: "origin/main",
              };
            }),
        );
        const createWorktree = vi.fn(
          (_: Parameters<GitVcsDriver.GitVcsDriver["Service"]["createWorktree"]>[0]) =>
            Effect.sync(() => {
              bootstrapGitOperations.push("create-worktree");
              return {
                worktree: {
                  refName: "t3code/bootstrap-refName",
                  path: "/tmp/bootstrap-worktree",
                },
              };
            }),
        );
        const runForThread = vi.fn(
          (
            _: Parameters<
              ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"]
            >[0],
          ) =>
            Effect.succeed({
              status: "started" as const,
              scriptId: "setup",
              scriptName: "Setup",
              terminalId: "setup-setup",
              cwd: "/tmp/bootstrap-worktree",
            }),
        );

        yield* buildAppUnderTest({
          layers: {
            gitVcsDriver: {
              remoteExists,
              fetchRemote,
              resolveRemoteTrackingCommit,
              createWorktree,
            },
            vcsStatusBroadcaster: {
              refreshStatus,
            },
            orchestrationEngine: {
              dispatch: (command) =>
                Effect.sync(() => {
                  dispatchedCommands.push(command);
                  return { sequence: dispatchedCommands.length };
                }),
              readEvents: () => Stream.empty,
            },
            projectSetupScriptRunner: {
              runForThread,
            },
          },
        });

        const createdAt = "2026-01-01T00:00:00.000Z";
        const wsUrl = yield* getWsServerUrl("/ws");
        const response = yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
              type: "thread.turn.start",
              commandId: CommandId.make("cmd-bootstrap-turn-start"),
              threadId: ThreadId.make("thread-bootstrap"),
              message: {
                messageId: MessageId.make("msg-bootstrap"),
                role: "user",
                text: "hello",
                attachments: [],
              },
              modelSelection: defaultModelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              bootstrap: {
                createThread: {
                  projectId: defaultProjectId,
                  title: "Bootstrap Thread",
                  modelSelection: defaultModelSelection,
                  runtimeMode: "full-access",
                  interactionMode: "default",
                  branch: "main",
                  worktreePath: null,
                  createdAt,
                },
                prepareWorktree: {
                  projectCwd: "/tmp/project",
                  baseBranch: "main",
                  branch: "t3code/bootstrap-refName",
                  startFromOrigin: true,
                },
                runSetupScript: true,
              },
              createdAt,
            }),
          ),
        );

        assert.equal(response.sequence, 5);
        assert.deepEqual(
          dispatchedCommands.map((command) => command.type),
          [
            "thread.create",
            "thread.meta.update",
            "thread.activity.append",
            "thread.activity.append",
            "thread.turn.start",
          ],
        );
        assert.deepEqual(createWorktree.mock.calls[0]?.[0], {
          cwd: "/tmp/project",
          refName: fetchedOriginCommit,
          newRefName: "t3code/bootstrap-refName",
          baseRefName: "main",
          path: null,
        });
        assert.deepEqual(fetchRemote.mock.calls[0]?.[0], {
          cwd: "/tmp/project",
          remoteName: "origin",
        });
        assert.deepEqual(resolveRemoteTrackingCommit.mock.calls[0]?.[0], {
          cwd: "/tmp/project",
          refName: "main",
          fallbackRemoteName: "origin",
        });
        assert.deepEqual(bootstrapGitOperations, [
          "remote-exists",
          "fetch",
          "resolve-remote-commit",
          "create-worktree",
        ]);
        assert.deepEqual(runForThread.mock.calls[0]?.[0], {
          threadId: ThreadId.make("thread-bootstrap"),
          projectId: defaultProjectId,
          projectCwd: "/tmp/project",
          worktreePath: "/tmp/bootstrap-worktree",
        });
        assert.deepEqual(refreshStatus.mock.calls[0]?.[0], "/tmp/bootstrap-worktree");

        const setupActivities = dispatchedCommands.filter(
          (command): command is Extract<OrchestrationCommand, { type: "thread.activity.append" }> =>
            command.type === "thread.activity.append",
        );
        assert.deepEqual(
          setupActivities.map((command) => command.activity.kind),
          ["setup-script.requested", "setup-script.started"],
        );
        const finalCommand = dispatchedCommands[4];
        assertTrue(finalCommand?.type === "thread.turn.start");
        if (finalCommand?.type === "thread.turn.start") {
          assert.equal(finalCommand.bootstrap, undefined);
        }
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect(
    "falls back to the local base branch when startFromOrigin is set but no origin remote exists",
    () =>
      Effect.gen(function* () {
        const dispatchedCommands: Array<OrchestrationCommand> = [];
        const remoteExists = vi.fn(
          (_: Parameters<GitVcsDriver.GitVcsDriver["Service"]["remoteExists"]>[0]) =>
            Effect.succeed(false),
        );
        const fetchRemote = vi.fn(
          (_: Parameters<GitVcsDriver.GitVcsDriver["Service"]["fetchRemote"]>[0]) => Effect.void,
        );
        const resolveRemoteTrackingCommit = vi.fn(
          (_: Parameters<GitVcsDriver.GitVcsDriver["Service"]["resolveRemoteTrackingCommit"]>[0]) =>
            Effect.succeed({
              commitSha: "0123456789abcdef0123456789abcdef01234567",
              remoteRefName: "origin/main",
            }),
        );
        const createWorktree = vi.fn(
          (_: Parameters<GitVcsDriver.GitVcsDriver["Service"]["createWorktree"]>[0]) =>
            Effect.succeed({
              worktree: {
                refName: "t3code/bootstrap-refName",
                path: "/tmp/bootstrap-worktree",
              },
            }),
        );

        yield* buildAppUnderTest({
          layers: {
            gitVcsDriver: {
              remoteExists,
              fetchRemote,
              resolveRemoteTrackingCommit,
              createWorktree,
            },
            orchestrationEngine: {
              dispatch: (command) =>
                Effect.sync(() => {
                  dispatchedCommands.push(command);
                  return { sequence: dispatchedCommands.length };
                }),
              readEvents: () => Stream.empty,
            },
          },
        });

        const createdAt = "2026-01-01T00:00:00.000Z";
        const wsUrl = yield* getWsServerUrl("/ws");
        yield* Effect.scoped(
          withWsRpcClient(wsUrl, (client) =>
            client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
              type: "thread.turn.start",
              commandId: CommandId.make("cmd-bootstrap-turn-start-no-origin"),
              threadId: ThreadId.make("thread-bootstrap-no-origin"),
              message: {
                messageId: MessageId.make("msg-bootstrap-no-origin"),
                role: "user",
                text: "hello",
                attachments: [],
              },
              modelSelection: defaultModelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              bootstrap: {
                createThread: {
                  projectId: defaultProjectId,
                  title: "Bootstrap Thread",
                  modelSelection: defaultModelSelection,
                  runtimeMode: "full-access",
                  interactionMode: "default",
                  branch: "main",
                  worktreePath: null,
                  createdAt,
                },
                prepareWorktree: {
                  projectCwd: "/tmp/project",
                  baseBranch: "main",
                  branch: "t3code/bootstrap-refName",
                  startFromOrigin: true,
                },
              },
              createdAt,
            }),
          ),
        );

        assert.deepEqual(remoteExists.mock.calls[0]?.[0], {
          cwd: "/tmp/project",
          remoteName: "origin",
        });
        assert.equal(fetchRemote.mock.calls.length, 0);
        assert.equal(resolveRemoteTrackingCommit.mock.calls.length, 0);
        assert.deepEqual(createWorktree.mock.calls[0]?.[0], {
          cwd: "/tmp/project",
          refName: "main",
          newRefName: "t3code/bootstrap-refName",
          baseRefName: "main",
          path: null,
        });
      }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("records setup-script failures without aborting bootstrap turn start", () =>
    Effect.gen(function* () {
      const dispatchedCommands: Array<OrchestrationCommand> = [];
      const createWorktree = vi.fn(
        (_: Parameters<GitVcsDriver.GitVcsDriver["Service"]["createWorktree"]>[0]) =>
          Effect.succeed({
            worktree: {
              refName: "t3code/bootstrap-refName",
              path: "/tmp/bootstrap-worktree",
            },
          }),
      );
      const runForThread = vi.fn(
        (
          input: Parameters<
            ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"]
          >[0],
        ) =>
          Effect.fail(
            new ProjectSetupScriptRunner.ProjectSetupScriptOperationError({
              threadId: input.threadId,
              worktreePath: input.worktreePath,
              operation: "openTerminal",
              cause: { message: "pty unavailable" },
            }),
          ),
      );

      yield* buildAppUnderTest({
        layers: {
          gitVcsDriver: {
            createWorktree,
          },
          orchestrationEngine: {
            dispatch: (command) =>
              Effect.sync(() => {
                dispatchedCommands.push(command);
                return { sequence: dispatchedCommands.length };
              }),
            readEvents: () => Stream.empty,
          },
          projectSetupScriptRunner: {
            runForThread,
          },
        },
      });

      const createdAt = "2026-01-01T00:00:00.000Z";
      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.turn.start",
            commandId: CommandId.make("cmd-bootstrap-turn-start-setup-failure"),
            threadId: ThreadId.make("thread-bootstrap-setup-failure"),
            message: {
              messageId: MessageId.make("msg-bootstrap-setup-failure"),
              role: "user",
              text: "hello",
              attachments: [],
            },
            modelSelection: defaultModelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            bootstrap: {
              createThread: {
                projectId: defaultProjectId,
                title: "Bootstrap Thread",
                modelSelection: defaultModelSelection,
                runtimeMode: "full-access",
                interactionMode: "default",
                branch: "main",
                worktreePath: null,
                createdAt,
              },
              prepareWorktree: {
                projectCwd: "/tmp/project",
                baseBranch: "main",
                branch: "t3code/bootstrap-refName",
              },
              runSetupScript: true,
            },
            createdAt,
          }),
        ),
      );

      assert.equal(response.sequence, 4);
      assert.deepEqual(
        dispatchedCommands.map((command) => command.type),
        ["thread.create", "thread.meta.update", "thread.activity.append", "thread.turn.start"],
      );
      const setupFailureActivity = dispatchedCommands.find(
        (command): command is Extract<OrchestrationCommand, { type: "thread.activity.append" }> =>
          command.type === "thread.activity.append",
      );
      assert.equal(setupFailureActivity?.activity.kind, "setup-script.failed");
      assert.deepEqual(setupFailureActivity?.activity.payload, {
        detail: "pty unavailable",
        worktreePath: "/tmp/bootstrap-worktree",
      });
      assertTrue(dispatchedCommands.every((command) => command.type !== "thread.delete"));
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("does not misattribute setup activity dispatch failures as setup launch failures", () =>
    Effect.gen(function* () {
      const dispatchedCommands: Array<OrchestrationCommand> = [];
      const createWorktree = vi.fn(
        (_: Parameters<GitVcsDriver.GitVcsDriver["Service"]["createWorktree"]>[0]) =>
          Effect.succeed({
            worktree: {
              refName: "t3code/bootstrap-refName",
              path: "/tmp/bootstrap-worktree",
            },
          }),
      );
      const runForThread = vi.fn(
        (
          _: Parameters<
            ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"]
          >[0],
        ) =>
          Effect.succeed({
            status: "started" as const,
            scriptId: "setup",
            scriptName: "Setup",
            terminalId: "setup-setup",
            cwd: "/tmp/bootstrap-worktree",
          }),
      );
      let setupActivityAppendAttempt = 0;

      yield* buildAppUnderTest({
        layers: {
          gitVcsDriver: {
            createWorktree,
          },
          orchestrationEngine: {
            dispatch: (command) => {
              if (
                command.type === "thread.activity.append" &&
                command.activity.kind.startsWith("setup-script.")
              ) {
                setupActivityAppendAttempt += 1;
                if (setupActivityAppendAttempt === 2) {
                  return Effect.fail(
                    new OrchestrationListenerCallbackError({
                      listener: "domain-event",
                      detail: "failed to append setup-script.started activity",
                    }),
                  );
                }
              }

              return Effect.sync(() => {
                dispatchedCommands.push(command);
                return { sequence: dispatchedCommands.length };
              });
            },
            readEvents: () => Stream.empty,
          },
          projectSetupScriptRunner: {
            runForThread,
          },
        },
      });

      const createdAt = "2026-01-01T00:00:00.000Z";
      const wsUrl = yield* getWsServerUrl("/ws");
      const response = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.turn.start",
            commandId: CommandId.make("cmd-bootstrap-turn-start-setup-activity-failure"),
            threadId: ThreadId.make("thread-bootstrap-setup-activity-failure"),
            message: {
              messageId: MessageId.make("msg-bootstrap-setup-activity-failure"),
              role: "user",
              text: "hello",
              attachments: [],
            },
            modelSelection: defaultModelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            bootstrap: {
              createThread: {
                projectId: defaultProjectId,
                title: "Bootstrap Thread",
                modelSelection: defaultModelSelection,
                runtimeMode: "full-access",
                interactionMode: "default",
                branch: "main",
                worktreePath: null,
                createdAt,
              },
              prepareWorktree: {
                projectCwd: "/tmp/project",
                baseBranch: "main",
                branch: "t3code/bootstrap-refName",
              },
              runSetupScript: true,
            },
            createdAt,
          }),
        ),
      );

      assert.equal(response.sequence, 4);
      assert.deepEqual(
        dispatchedCommands.map((command) => command.type),
        ["thread.create", "thread.meta.update", "thread.activity.append", "thread.turn.start"],
      );
      const setupActivities = dispatchedCommands.filter(
        (command): command is Extract<OrchestrationCommand, { type: "thread.activity.append" }> =>
          command.type === "thread.activity.append",
      );
      assert.deepEqual(
        setupActivities.map((command) => command.activity.kind),
        ["setup-script.requested"],
      );
      assertTrue(
        setupActivities.every((command) => command.activity.kind !== "setup-script.failed"),
      );
      assertTrue(dispatchedCommands.every((command) => command.type !== "thread.delete"));
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("cleans up created bootstrap threads when worktree creation defects", () =>
    Effect.gen(function* () {
      const dispatchedCommands: Array<OrchestrationCommand> = [];
      const createWorktree = vi.fn(
        (_: Parameters<GitVcsDriver.GitVcsDriver["Service"]["createWorktree"]>[0]) =>
          Effect.die(new Error("worktree exploded")),
      );

      yield* buildAppUnderTest({
        layers: {
          gitVcsDriver: {
            createWorktree,
          },
          orchestrationEngine: {
            dispatch: (command) =>
              Effect.sync(() => {
                dispatchedCommands.push(command);
                return { sequence: dispatchedCommands.length };
              }),
            readEvents: () => Stream.empty,
          },
        },
      });

      const createdAt = "2026-01-01T00:00:00.000Z";
      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
            type: "thread.turn.start",
            commandId: CommandId.make("cmd-bootstrap-turn-start-defect"),
            threadId: ThreadId.make("thread-bootstrap-defect"),
            message: {
              messageId: MessageId.make("msg-bootstrap-defect"),
              role: "user",
              text: "hello",
              attachments: [],
            },
            modelSelection: defaultModelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            bootstrap: {
              createThread: {
                projectId: defaultProjectId,
                title: "Bootstrap Thread",
                modelSelection: defaultModelSelection,
                runtimeMode: "full-access",
                interactionMode: "default",
                branch: "main",
                worktreePath: null,
                createdAt,
              },
              prepareWorktree: {
                projectCwd: "/tmp/project",
                baseBranch: "main",
                branch: "t3code/bootstrap-refName",
              },
              runSetupScript: false,
            },
            createdAt,
          }),
        ).pipe(Effect.result),
      );

      assertTrue(result._tag === "Failure");
      assertTrue(result.failure._tag === "OrchestrationDispatchCommandError");
      assert.include(result.failure.message, "worktree exploded");
      assert.deepEqual(
        dispatchedCommands.map((command) => command.type),
        ["thread.create", "thread.delete"],
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc terminal methods", () =>
    Effect.gen(function* () {
      const snapshot = {
        threadId: "thread-1",
        terminalId: "default",
        cwd: "/tmp/project",
        worktreePath: null,
        status: "running" as const,
        pid: 1234,
        history: "",
        exitCode: null,
        exitSignal: null,
        label: "Primary",
        updatedAt: "2026-01-01T00:00:00.000Z",
      };

      yield* buildAppUnderTest({
        layers: {
          projectionSnapshotQuery: {
            getThreadShellById: () =>
              Effect.succeed(
                Option.some(makeDefaultOrchestrationThreadShell({ id: ThreadId.make("thread-1") })),
              ),
          },
          terminalManager: {
            open: () => Effect.succeed(snapshot),
            write: () => Effect.void,
            resize: () => Effect.void,
            clear: () => Effect.void,
            restart: () => Effect.succeed(snapshot),
            close: () => Effect.void,
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");

      const opened = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalOpen]({
            threadId: "thread-1",
            terminalId: "default",
            cwd: "/tmp/project",
          }),
        ),
      );
      assert.equal(opened.terminalId, "default");

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalWrite]({
            threadId: "thread-1",
            terminalId: "default",
            data: "echo hi\n",
          }),
        ),
      );

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalResize]({
            threadId: "thread-1",
            terminalId: "default",
            cols: 120,
            rows: 40,
          }),
        ),
      );

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalClear]({
            threadId: "thread-1",
            terminalId: "default",
          }),
        ),
      );

      const restarted = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalRestart]({
            threadId: "thread-1",
            terminalId: "default",
            cwd: "/tmp/project",
            cols: 120,
            rows: 40,
          }),
        ),
      );
      assert.equal(restarted.terminalId, "default");

      yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalClose]({
            threadId: "thread-1",
            terminalId: "default",
          }),
        ),
      );
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("authorizes General Chat terminals only at the authoritative workspace", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-general-chat-terminal");
      const openedCwds: string[] = [];
      const snapshot = {
        threadId,
        terminalId: "default",
        cwd: "/tmp/general-chat-workspace",
        worktreePath: null,
        status: "running" as const,
        pid: 1234,
        history: "",
        exitCode: null,
        exitSignal: null,
        label: "Primary",
        updatedAt: "2026-01-01T00:00:00.000Z",
      };

      yield* buildAppUnderTest({
        layers: {
          projectionSnapshotQuery: {
            getThreadShellById: () =>
              Effect.succeed(
                Option.some(
                  makeDefaultOrchestrationThreadShell({
                    id: threadId,
                    projectId: null,
                    workspaceRoot: "/tmp/general-chat-workspace",
                  }),
                ),
              ),
          },
          terminalManager: {
            open: (input) =>
              Effect.sync(() => {
                openedCwds.push(input.cwd);
                return snapshot;
              }),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const opened = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalOpen]({
            threadId,
            terminalId: "default",
            cwd: "/tmp/general-chat-workspace",
          }),
        ),
      );
      assert.equal(opened.cwd, "/tmp/general-chat-workspace");

      const rejected = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalOpen]({
            threadId,
            terminalId: "other",
            cwd: "/tmp/other-workspace",
          }),
        ).pipe(Effect.result),
      );
      assertTrue(rejected._tag === "Failure");
      assertTrue(rejected.failure._tag === "TerminalWorkspaceMismatchError");

      const rejectedAttach = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalAttach]({
            threadId,
            terminalId: "attached-elsewhere",
            cwd: "/tmp/other-workspace",
            restartIfNotRunning: true,
          }).pipe(Stream.runHead),
        ).pipe(Effect.result),
      );
      assertTrue(rejectedAttach._tag === "Failure");
      assertTrue(rejectedAttach.failure._tag === "TerminalWorkspaceMismatchError");

      const rejectedRestart = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalRestart]({
            threadId,
            terminalId: "default",
            cwd: "/tmp/other-workspace",
            cols: 120,
            rows: 40,
          }),
        ).pipe(Effect.result),
      );
      assertTrue(rejectedRestart._tag === "Failure");
      assertTrue(rejectedRestart.failure._tag === "TerminalWorkspaceMismatchError");
      assert.deepEqual(openedCwds, ["/tmp/general-chat-workspace"]);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );

  it.effect("routes websocket rpc terminal.write errors", () =>
    Effect.gen(function* () {
      const terminalError = new TerminalNotRunningError({
        threadId: "thread-1",
        terminalId: "default",
      });
      yield* buildAppUnderTest({
        layers: {
          terminalManager: {
            write: () => Effect.fail(terminalError),
          },
        },
      });

      const wsUrl = yield* getWsServerUrl("/ws");
      const result = yield* Effect.scoped(
        withWsRpcClient(wsUrl, (client) =>
          client[WS_METHODS.terminalWrite]({
            threadId: "thread-1",
            terminalId: "default",
            data: "echo fail\n",
          }),
        ).pipe(Effect.result),
      );

      assertFailure(result, terminalError);
    }).pipe(Effect.provide(NodeHttpServer.layerTest)),
  );
});

it.live(
  "reports thread HTTP and WebSocket transfer budgets",
  () =>
    Effect.gen(function* () {
      const providers = [
        ProviderDriverKind.make("codex"),
        ProviderDriverKind.make("claudeAgent"),
      ] as const;

      const runs = yield* Effect.forEach(
        providers,
        (provider) =>
          Effect.acquireUseRelease(
            makeOrchestrationIntegrationHarness({ provider }),
            (harness) =>
              Effect.gen(function* () {
                yield* seedTransferBudgetHistory(harness, provider);
                yield* buildAppUnderTest({
                  layers: {
                    orchestrationEngine: harness.engine,
                    projectionSnapshotQuery: harness.snapshotQuery,
                  },
                });

                const baseUrl = yield* getHttpServerUrl();
                const cookie = yield* getAuthenticatedSessionCookieHeader();

                const recorder = makeWebSocketTransferRecorder();
                const wsUrl = baseUrl.replace(/^http:/, "ws:") + "/ws";
                const protocolLayer = countingWsRpcProtocolLayer({
                  url: wsUrl,
                  cookie,
                  recorder,
                });

                return yield* Effect.scoped(
                  Effect.gen(function* () {
                    const client = yield* makeCountingWsRpcClient;

                    const threadSnapshot = yield* measureHttpGet({
                      url: `${baseUrl}/api/orchestration/threads/${TRANSFER_THREAD_ID}`,
                      headers: { cookie },
                    });
                    assert.equal(threadSnapshot.status, 200);
                    assert.equal(threadSnapshot.contentEncoding, "gzip");
                    const decodedThread = yield* decodeTransferThreadSnapshot(
                      Buffer.from(threadSnapshot.decodedBody).toString("utf8"),
                    );
                    assert.equal(
                      decodedThread.thread.messages.length,
                      TRANSFER_HISTORY_TURN_COUNT * 2,
                    );

                    const threadItems = yield* Queue.unbounded<OrchestrationThreadStreamItem>();
                    yield* client[ORCHESTRATION_WS_METHODS.subscribeThread]({
                      threadId: TRANSFER_THREAD_ID,
                      afterSequence: decodedThread.snapshotSequence,
                      requestCompletionMarker: true,
                    }).pipe(
                      Stream.runForEach((item) =>
                        Queue.offer(threadItems, item).pipe(Effect.asVoid),
                      ),
                      Effect.forkScoped,
                    );
                    const initialThreadItems = yield* collectQueueUntil(
                      threadItems,
                      (item) => item.kind === "synchronized",
                      `${provider} thread subscription to synchronize`,
                    );
                    assert.isFalse(initialThreadItems.some((item) => item.kind === "snapshot"));
                    assert.include(recorder.negotiatedExtensions(), "permessage-deflate");

                    yield* queueMeasuredTransferTurn(harness, provider);
                    const turnStartTotals = recorder.totals();
                    yield* client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
                      type: "thread.turn.start",
                      commandId: CommandId.make(`transfer:${provider}:measured-turn`),
                      threadId: TRANSFER_THREAD_ID,
                      message: {
                        messageId: MessageId.make("transfer-user-measured"),
                        role: "user",
                        text: "Measure the client-bound transfer for this turn.",
                        attachments: [],
                      },
                      modelSelection: transferModelSelection(provider),
                      runtimeMode: "approval-required",
                      interactionMode: "default",
                      createdAt: TRANSFER_MEASURED_TURN_CREATED_AT,
                    });
                    yield* waitForTurnQuiesced(harness, TRANSFER_MEASURED_TURN_INDEX + 1);
                    const finalThreadSequence = yield* harness.engine
                      .readEvents(decodedThread.snapshotSequence, 10_000)
                      .pipe(
                        Stream.runFold(
                          () => decodedThread.snapshotSequence,
                          (sequence, event) =>
                            event.aggregateId === TRANSFER_THREAD_ID && isThreadDetailEvent(event)
                              ? Math.max(sequence, event.sequence)
                              : sequence,
                        ),
                      );
                    assert.isAbove(finalThreadSequence, decodedThread.snapshotSequence);

                    yield* collectQueueUntil(
                      threadItems,
                      (item) =>
                        item.kind === "event" && item.event.sequence === finalThreadSequence,
                      `${provider} thread stream to reach sequence ${finalThreadSequence}`,
                    );
                    const measuredTurnWebSocket = transferDelta(turnStartTotals, recorder.totals());

                    const finalThreadSnapshot = yield* harness.snapshotQuery
                      .getThreadDetailSnapshot(TRANSFER_THREAD_ID)
                      .pipe(Effect.map(Option.getOrThrow));
                    const expectedAssistantText = expectedMeasuredAssistantText(provider);
                    const measuredAssistant = finalThreadSnapshot.thread.messages.find(
                      (message) =>
                        message.role === "assistant" && message.text === expectedAssistantText,
                    );
                    assert.isDefined(measuredAssistant);
                    assert.isTrue(
                      finalThreadSnapshot.thread.messages.length >= TRANSFER_HISTORY_TURN_COUNT * 2,
                    );
                    assert.equal(measuredAssistant?.streaming, false);
                    assert.equal(finalThreadSnapshot.thread.session?.status, "ready");
                    assert.equal(
                      finalThreadSnapshot.thread.checkpoints.length,
                      TRANSFER_HISTORY_TURN_COUNT + 1,
                    );

                    return {
                      provider,
                      threadSnapshot,
                      measuredTurnWebSocket,
                    } satisfies TransferBudgetRun;
                  }).pipe(Effect.provide(protocolLayer)),
                );
              }),
            (harness) => harness.dispose,
          ).pipe(Effect.provide(NodeHttpServerTestWithWsDeflate)),
        { concurrency: 1 },
      );

      const report = formatTransferBudgetReport(runs);
      yield* Effect.logInfo(`\n${report}`);
      const reportPath = yield* Config.string("T3CODE_TRANSFER_BUDGET_REPORT_PATH").pipe(
        Config.option,
      );
      if (Option.isSome(reportPath)) {
        const fileSystem = yield* FileSystem.FileSystem;
        yield* fileSystem.writeFileString(reportPath.value, report);
      }
      const resultPath = yield* Config.string("T3CODE_TRANSFER_BUDGET_RESULT_PATH").pipe(
        Config.option,
      );
      if (Option.isSome(resultPath)) {
        const fileSystem = yield* FileSystem.FileSystem;
        yield* fileSystem.writeFileString(resultPath.value, formatTransferBudgetResult(runs));
      }
      assert.deepEqual(transferBudgetViolations(runs), []);
    }).pipe(Effect.provide(NodeServices.layer)),
  120_000,
);
