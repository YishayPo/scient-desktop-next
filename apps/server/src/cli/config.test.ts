// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";

import { assert, expect, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import {
  DesktopBackendBootstrap,
  type DesktopBackendBootstrap as DesktopBackendBootstrapValue,
} from "@t3tools/contracts";
import * as NetService from "@t3tools/shared/Net";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { deriveServerPaths } from "../config.ts";
import { resolveServerConfig } from "./config.ts";

const deriveExplicitServerPaths = (baseDir: string, devUrl: URL | undefined) =>
  deriveServerPaths(baseDir, devUrl, { baseDirIsExplicit: true });

const encodeDesktopBootstrap = Schema.encodeEffect(Schema.fromJsonString(DesktopBackendBootstrap));

const makeDesktopBootstrap = (
  overrides: Partial<DesktopBackendBootstrapValue> = {},
): DesktopBackendBootstrapValue => ({
  mode: "desktop",
  noBrowser: true,
  port: 4888,
  t3Home: "/tmp/t3-bootstrap-home",
  host: "127.0.0.1",
  desktopBootstrapToken: "desktop-bootstrap-token",
  tailscaleServeEnabled: false,
  tailscaleServePort: 443,
  ...overrides,
});

it.layer(NodeServices.layer)("cli config resolution", (it) => {
  const defaultObservabilityConfig = {
    traceMinLevel: "Info",
    traceTimingEnabled: true,
    traceBatchWindowMs: 1_000,
    traceMaxBytes: 10 * 1024 * 1024,
    traceMaxFiles: 10,
    otlpTracesUrl: undefined,
    otlpMetricsUrl: undefined,
    otlpExportIntervalMs: 10_000,
    otlpServiceName: "t3-server",
    devAllowedOrigins: [],
  } as const;

  const openBootstrapFd = Effect.fn(function* (payload: DesktopBackendBootstrapValue) {
    const fs = yield* FileSystem.FileSystem;
    const filePath = yield* fs.makeTempFileScoped({ prefix: "t3-bootstrap-", suffix: ".ndjson" });
    const encoded = yield* encodeDesktopBootstrap(payload);
    yield* fs.writeFileString(filePath, `${encoded}\n`);
    return yield* Effect.acquireRelease(
      Effect.sync(() => NodeFS.openSync(filePath, "r")),
      (fd) => Effect.sync(() => NodeFS.closeSync(fd)),
    );
  });

  it.effect("falls back to effect/config values when flags are omitted", () =>
    Effect.gen(function* () {
      const { join } = yield* Path.Path;
      const baseDir = join(NodeOS.tmpdir(), "t3-cli-config-env-base");
      const derivedPaths = yield* deriveServerPaths(baseDir, new URL("http://127.0.0.1:5173"), {
        developmentStateDirName: "scient-next-dev",
        baseDirIsExplicit: false,
      });
      const resolved = yield* resolveServerConfig(
        {
          mode: Option.none(),
          port: Option.none(),
          host: Option.none(),
          baseDir: Option.none(),
          cwd: Option.none(),
          devUrl: Option.none(),
          noBrowser: Option.none(),
          bootstrapFd: Option.none(),
          autoBootstrapProjectFromCwd: Option.none(),
          logWebSocketEvents: Option.none(),
          tailscaleServeEnabled: Option.none(),
          tailscaleServePort: Option.none(),
        },
        Option.none(),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            ConfigProvider.layer(
              ConfigProvider.fromEnv({
                env: {
                  T3CODE_LOG_LEVEL: "Warn",
                  T3CODE_MODE: "desktop",
                  T3CODE_PORT: "4001",
                  T3CODE_HOST: "0.0.0.0",
                  SCIENT_NEXT_HOME: baseDir,
                  T3CODE_HOME: join(NodeOS.tmpdir(), "ignored-legacy-t3-home"),
                  VITE_DEV_SERVER_URL: "http://127.0.0.1:5173",
                  T3CODE_DEV_ALLOWED_ORIGINS:
                    "https://host.example.ts.net, https://phone.example.ts.net ",
                  T3CODE_NO_BROWSER: "true",
                  T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "false",
                  T3CODE_LOG_WS_EVENTS: "true",
                },
              }),
            ),
            NetService.layer,
          ),
        ),
      );

      expect(resolved).toEqual({
        logLevel: "Warn",
        ...defaultObservabilityConfig,
        mode: "desktop",
        port: 4001,
        cwd: process.cwd(),
        baseDir,
        ...derivedPaths,
        host: "0.0.0.0",
        staticDir: undefined,
        devUrl: new URL("http://127.0.0.1:5173"),
        devAllowedOrigins: ["https://host.example.ts.net", "https://phone.example.ts.net"],
        noBrowser: true,
        startupPresentation: "browser",
        desktopBootstrapToken: undefined,
        autoBootstrapProjectFromCwd: false,
        logWebSocketEvents: true,
        tailscaleServeEnabled: false,
        tailscaleServePort: 443,
      });
      assert.equal(resolved.stateDir, join(baseDir, "scient-next-dev"));
    }),
  );

  it.effect("uses CLI flags when provided", () =>
    Effect.gen(function* () {
      const { join } = yield* Path.Path;
      const baseDir = join(NodeOS.tmpdir(), "t3-cli-config-flags-base");
      const derivedPaths = yield* deriveExplicitServerPaths(
        baseDir,
        new URL("http://127.0.0.1:4173"),
      );
      const resolved = yield* resolveServerConfig(
        {
          mode: Option.some("web"),
          port: Option.some(8788),
          host: Option.some("127.0.0.1"),
          baseDir: Option.some(baseDir),
          cwd: Option.none(),
          devUrl: Option.some(new URL("http://127.0.0.1:4173")),
          noBrowser: Option.some(true),
          bootstrapFd: Option.none(),
          autoBootstrapProjectFromCwd: Option.some(true),
          logWebSocketEvents: Option.some(true),
          tailscaleServeEnabled: Option.some(true),
          tailscaleServePort: Option.some(8443),
        },
        Option.some("Debug"),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            ConfigProvider.layer(
              ConfigProvider.fromEnv({
                env: {
                  T3CODE_LOG_LEVEL: "Warn",
                  T3CODE_MODE: "desktop",
                  T3CODE_PORT: "4001",
                  T3CODE_HOST: "0.0.0.0",
                  SCIENT_NEXT_HOME: join(NodeOS.tmpdir(), "ignored-candidate-home"),
                  T3CODE_HOME: join(NodeOS.tmpdir(), "ignored-base"),
                  VITE_DEV_SERVER_URL: "http://127.0.0.1:5173",
                  T3CODE_NO_BROWSER: "false",
                  T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "false",
                  T3CODE_LOG_WS_EVENTS: "false",
                },
              }),
            ),
            NetService.layer,
          ),
        ),
      );

      expect(resolved).toEqual({
        logLevel: "Debug",
        ...defaultObservabilityConfig,
        mode: "web",
        port: 8788,
        cwd: process.cwd(),
        baseDir,
        ...derivedPaths,
        host: "127.0.0.1",
        staticDir: undefined,
        devUrl: new URL("http://127.0.0.1:4173"),
        noBrowser: true,
        startupPresentation: "browser",
        desktopBootstrapToken: undefined,
        autoBootstrapProjectFromCwd: true,
        logWebSocketEvents: true,
        tailscaleServeEnabled: true,
        tailscaleServePort: 8443,
      });
      assert.equal(resolved.dbPath, join(baseDir, "userdata", "state.sqlite"));
    }),
  );

  it.effect("forces development state when a desktop child loses its dev URL", () =>
    Effect.gen(function* () {
      const { join } = yield* Path.Path;
      const baseDir = join(NodeOS.tmpdir(), "scient-cli-config-development-state");
      const resolved = yield* resolveServerConfig(
        {
          mode: Option.some("desktop"),
          port: Option.none(),
          host: Option.none(),
          baseDir: Option.none(),
          cwd: Option.none(),
          devUrl: Option.none(),
          noBrowser: Option.none(),
          bootstrapFd: Option.none(),
          autoBootstrapProjectFromCwd: Option.none(),
          logWebSocketEvents: Option.none(),
          tailscaleServeEnabled: Option.none(),
          tailscaleServePort: Option.none(),
        },
        Option.none(),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            ConfigProvider.layer(
              ConfigProvider.fromEnv({
                env: {
                  SCIENT_NEXT_HOME: baseDir,
                  SCIENT_NEXT_DEVELOPMENT_STATE: "true",
                },
              }),
            ),
            NetService.layer,
          ),
        ),
      );

      expect(resolved.devUrl).toBeUndefined();
      expect(resolved.stateDir).toBe(join(baseDir, "scient-next-dev"));
      expect(resolved.dbPath).toBe(join(baseDir, "scient-next-dev", "state.sqlite"));
    }),
  );

  it.effect("preserves explicit false CLI boolean flags over env and bootstrap values", () =>
    Effect.gen(function* () {
      const { join } = yield* Path.Path;
      const baseDir = join(NodeOS.tmpdir(), "t3-cli-config-false-flags");
      const fd = yield* openBootstrapFd(
        makeDesktopBootstrap({
          noBrowser: true,
          tailscaleServeEnabled: false,
          tailscaleServePort: 443,
        }),
      );
      const derivedPaths = yield* deriveExplicitServerPaths(
        baseDir,
        new URL("http://127.0.0.1:4173"),
      );

      const resolved = yield* resolveServerConfig(
        {
          mode: Option.some("web"),
          port: Option.some(8788),
          host: Option.some("127.0.0.1"),
          baseDir: Option.some(baseDir),
          cwd: Option.none(),
          devUrl: Option.some(new URL("http://127.0.0.1:4173")),
          noBrowser: Option.some(false),
          bootstrapFd: Option.none(),
          autoBootstrapProjectFromCwd: Option.some(false),
          logWebSocketEvents: Option.some(false),
          tailscaleServeEnabled: Option.none(),
          tailscaleServePort: Option.none(),
        },
        Option.none(),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            ConfigProvider.layer(
              ConfigProvider.fromEnv({
                env: {
                  T3CODE_BOOTSTRAP_FD: String(fd),
                  T3CODE_NO_BROWSER: "true",
                  T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "true",
                  T3CODE_LOG_WS_EVENTS: "true",
                },
              }),
            ),
            NetService.layer,
          ),
        ),
      );

      expect(resolved).toEqual({
        logLevel: "Info",
        ...defaultObservabilityConfig,
        mode: "web",
        port: 8788,
        cwd: process.cwd(),
        baseDir,
        ...derivedPaths,
        host: "127.0.0.1",
        staticDir: undefined,
        devUrl: new URL("http://127.0.0.1:4173"),
        noBrowser: false,
        startupPresentation: "browser",
        desktopBootstrapToken: "desktop-bootstrap-token",
        autoBootstrapProjectFromCwd: false,
        logWebSocketEvents: false,
        tailscaleServeEnabled: false,
        tailscaleServePort: 443,
      });
    }),
  );

  it.effect("uses bootstrap values but ignores its legacy T3 home in the candidate", () =>
    Effect.gen(function* () {
      const { join } = yield* Path.Path;
      const baseDir = "/tmp/scient-next-bootstrap-home";
      const fd = yield* openBootstrapFd(
        makeDesktopBootstrap({
          port: 4888,
          host: "127.0.0.2",
          t3Home: "/tmp/ignored-legacy-t3-bootstrap-home",
          noBrowser: true,
          desktopBootstrapToken: "desktop-token",
          desktopTelemetryFd: 4,
          desktopTelemetryControlFd: 5,
          tailscaleServeEnabled: false,
          tailscaleServePort: 443,
          otlpTracesUrl: "http://localhost:4318/v1/traces",
          otlpMetricsUrl: "http://localhost:4318/v1/metrics",
        }),
      );
      const derivedPaths = yield* deriveServerPaths(baseDir, undefined);

      const resolved = yield* resolveServerConfig(
        {
          mode: Option.none(),
          port: Option.none(),
          host: Option.none(),
          baseDir: Option.none(),
          cwd: Option.none(),
          devUrl: Option.none(),
          noBrowser: Option.none(),
          bootstrapFd: Option.none(),
          autoBootstrapProjectFromCwd: Option.none(),
          logWebSocketEvents: Option.none(),
          tailscaleServeEnabled: Option.none(),
          tailscaleServePort: Option.none(),
        },
        Option.none(),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            ConfigProvider.layer(
              ConfigProvider.fromEnv({
                env: {
                  T3CODE_BOOTSTRAP_FD: String(fd),
                  SCIENT_NEXT_HOME: baseDir,
                },
              }),
            ),
            NetService.layer,
          ),
        ),
      );

      expect(resolved).toEqual({
        logLevel: "Info",
        ...defaultObservabilityConfig,
        otlpTracesUrl: undefined,
        otlpMetricsUrl: undefined,
        mode: "desktop",
        port: 4888,
        cwd: process.cwd(),
        baseDir,
        ...derivedPaths,
        host: "127.0.0.2",
        staticDir: resolved.staticDir,
        devUrl: undefined,
        noBrowser: true,
        startupPresentation: "browser",
        desktopBootstrapToken: "desktop-token",
        desktopTelemetryFd: 4,
        desktopTelemetryControlFd: 5,
        resourceMonitorPath: undefined,
        autoBootstrapProjectFromCwd: false,
        logWebSocketEvents: false,
        tailscaleServeEnabled: false,
        tailscaleServePort: 443,
      });
      assert.equal(join(baseDir, "userdata"), resolved.stateDir);
      assert.equal(resolved.desktopTelemetryFd, 4);
      assert.equal(resolved.desktopTelemetryControlFd, 5);
    }),
  );

  it.effect("creates derived runtime directories during config resolution", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-cli-config-dirs-" });
      const customCwd = path.join(baseDir, "nested", "project");

      const resolved = yield* resolveServerConfig(
        {
          mode: Option.some("desktop"),
          port: Option.some(4888),
          host: Option.none(),
          baseDir: Option.some(baseDir),
          cwd: Option.some(customCwd),
          devUrl: Option.some(new URL("http://127.0.0.1:5173")),
          noBrowser: Option.none(),
          bootstrapFd: Option.none(),
          autoBootstrapProjectFromCwd: Option.none(),
          logWebSocketEvents: Option.none(),
          tailscaleServeEnabled: Option.none(),
          tailscaleServePort: Option.none(),
        },
        Option.none(),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} })),
            NetService.layer,
          ),
        ),
      );

      for (const directory of [
        customCwd,
        resolved.stateDir,
        resolved.logsDir,
        resolved.providerLogsDir,
        resolved.terminalLogsDir,
        resolved.attachmentsDir,
        resolved.documentArtifactsDir,
        resolved.worktreesDir,
        path.dirname(resolved.serverLogPath),
        path.dirname(resolved.serverTracePath),
      ]) {
        expect(yield* fs.exists(directory)).toBe(true);
      }
      expect(resolved.cwd).toBe(path.resolve(customCwd));
    }),
  );

  it.effect("applies flag then env precedence over bootstrap envelope values", () =>
    Effect.gen(function* () {
      const { join } = yield* Path.Path;
      const baseDir = join(NodeOS.tmpdir(), "t3-cli-config-env-wins");
      const fd = yield* openBootstrapFd(
        makeDesktopBootstrap({
          port: 4888,
          host: "127.0.0.2",
          t3Home: "/tmp/t3-bootstrap-home",
          noBrowser: false,
          desktopBootstrapToken: "desktop-token",
          tailscaleServeEnabled: false,
          tailscaleServePort: 443,
        }),
      );
      const derivedPaths = yield* deriveServerPaths(baseDir, new URL("http://127.0.0.1:4173"), {
        developmentStateDirName: "scient-next-dev",
        baseDirIsExplicit: false,
      });

      const resolved = yield* resolveServerConfig(
        {
          mode: Option.none(),
          port: Option.some(8788),
          host: Option.some("127.0.0.1"),
          baseDir: Option.none(),
          cwd: Option.none(),
          devUrl: Option.some(new URL("http://127.0.0.1:4173")),
          noBrowser: Option.none(),
          bootstrapFd: Option.none(),
          autoBootstrapProjectFromCwd: Option.none(),
          logWebSocketEvents: Option.none(),
          tailscaleServeEnabled: Option.none(),
          tailscaleServePort: Option.none(),
        },
        Option.some("Debug"),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            ConfigProvider.layer(
              ConfigProvider.fromEnv({
                env: {
                  T3CODE_MODE: "web",
                  T3CODE_BOOTSTRAP_FD: String(fd),
                  SCIENT_NEXT_HOME: baseDir,
                  T3CODE_HOME: join(NodeOS.tmpdir(), "ignored-legacy-t3-home"),
                  T3CODE_NO_BROWSER: "true",
                  T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "true",
                  T3CODE_LOG_WS_EVENTS: "true",
                },
              }),
            ),
            NetService.layer,
          ),
        ),
      );

      expect(resolved).toEqual({
        logLevel: "Debug",
        ...defaultObservabilityConfig,
        mode: "web",
        port: 8788,
        cwd: process.cwd(),
        baseDir,
        ...derivedPaths,
        host: "127.0.0.1",
        staticDir: undefined,
        devUrl: new URL("http://127.0.0.1:4173"),
        noBrowser: true,
        startupPresentation: "browser",
        desktopBootstrapToken: "desktop-token",
        autoBootstrapProjectFromCwd: true,
        logWebSocketEvents: true,
        tailscaleServeEnabled: false,
        tailscaleServePort: 443,
      });
    }),
  );

  it.effect("falls back to persisted observability settings when env vars are absent", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-cli-config-settings-" });
      const derivedPaths = yield* deriveExplicitServerPaths(baseDir, undefined);
      yield* fs.makeDirectory(path.dirname(derivedPaths.settingsPath), { recursive: true });
      yield* fs.writeFileString(
        derivedPaths.settingsPath,
        // @effect-diagnostics-next-line preferSchemaOverJson:off
        `${JSON.stringify({
          observability: {
            otlpTracesUrl: "http://localhost:4318/v1/traces",
            otlpMetricsUrl: "http://localhost:4318/v1/metrics",
          },
        })}\n`,
      );

      const resolved = yield* resolveServerConfig(
        {
          mode: Option.some("desktop"),
          port: Option.some(4888),
          host: Option.none(),
          baseDir: Option.some(baseDir),
          cwd: Option.none(),
          devUrl: Option.none(),
          noBrowser: Option.none(),
          bootstrapFd: Option.none(),
          autoBootstrapProjectFromCwd: Option.none(),
          logWebSocketEvents: Option.none(),
          tailscaleServeEnabled: Option.none(),
          tailscaleServePort: Option.none(),
        },
        Option.none(),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} })),
            NetService.layer,
          ),
        ),
      );

      expect(resolved.otlpTracesUrl).toBeUndefined();
      expect(resolved.otlpMetricsUrl).toBeUndefined();
      expect(resolved).toEqual({
        logLevel: "Info",
        ...defaultObservabilityConfig,
        otlpTracesUrl: undefined,
        otlpMetricsUrl: undefined,
        mode: "desktop",
        port: 4888,
        cwd: process.cwd(),
        baseDir,
        ...derivedPaths,
        host: "127.0.0.1",
        staticDir: resolved.staticDir,
        devUrl: undefined,
        noBrowser: true,
        startupPresentation: "browser",
        desktopBootstrapToken: undefined,
        autoBootstrapProjectFromCwd: false,
        logWebSocketEvents: false,
        tailscaleServeEnabled: false,
        tailscaleServePort: 443,
      });
    }),
  );

  it.effect("fails closed for outbound OTLP destinations in the D4 envelope", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "scient-next-cli-config-safe-" });
      const resolved = yield* resolveServerConfig(
        {
          mode: Option.some("desktop"),
          port: Option.some(4888),
          host: Option.none(),
          baseDir: Option.some(baseDir),
          cwd: Option.none(),
          devUrl: Option.none(),
          noBrowser: Option.none(),
          bootstrapFd: Option.none(),
          autoBootstrapProjectFromCwd: Option.none(),
          logWebSocketEvents: Option.none(),
          tailscaleServeEnabled: Option.none(),
          tailscaleServePort: Option.none(),
        },
        Option.none(),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            ConfigProvider.layer(
              ConfigProvider.fromEnv({
                env: {
                  SCIENT_NEXT_SAFETY_ENVELOPE: "true",
                  T3CODE_OTLP_TRACES_URL: "https://telemetry.example/traces",
                  T3CODE_OTLP_METRICS_URL: "https://telemetry.example/metrics",
                },
              }),
            ),
            NetService.layer,
          ),
        ),
      );

      expect(resolved.otlpTracesUrl).toBeUndefined();
      expect(resolved.otlpMetricsUrl).toBeUndefined();
    }),
  );

  it.effect("forces noBrowser and disables auto-bootstrap for headless startup presentation", () =>
    Effect.gen(function* () {
      const { join } = yield* Path.Path;
      const baseDir = join(NodeOS.tmpdir(), "t3-cli-config-headless-base");
      const derivedPaths = yield* deriveExplicitServerPaths(baseDir, undefined);

      const resolved = yield* resolveServerConfig(
        {
          mode: Option.some("web"),
          port: Option.some(3773),
          host: Option.none(),
          baseDir: Option.some(baseDir),
          cwd: Option.none(),
          devUrl: Option.none(),
          noBrowser: Option.none(),
          bootstrapFd: Option.none(),
          autoBootstrapProjectFromCwd: Option.none(),
          logWebSocketEvents: Option.none(),
          tailscaleServeEnabled: Option.none(),
          tailscaleServePort: Option.none(),
        },
        Option.none(),
        {
          startupPresentation: "headless",
        },
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            ConfigProvider.layer(
              ConfigProvider.fromEnv({
                env: {
                  T3CODE_NO_BROWSER: "false",
                  T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "true",
                },
              }),
            ),
            NetService.layer,
          ),
        ),
      );

      expect(resolved).toEqual({
        logLevel: "Info",
        ...defaultObservabilityConfig,
        mode: "web",
        port: 3773,
        cwd: process.cwd(),
        baseDir,
        ...derivedPaths,
        host: undefined,
        staticDir: resolved.staticDir,
        devUrl: undefined,
        noBrowser: true,
        startupPresentation: "headless",
        desktopBootstrapToken: undefined,
        autoBootstrapProjectFromCwd: false,
        logWebSocketEvents: false,
        tailscaleServeEnabled: false,
        tailscaleServePort: 443,
      });
    }),
  );
});
