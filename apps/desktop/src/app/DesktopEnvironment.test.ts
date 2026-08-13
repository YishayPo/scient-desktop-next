import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as DesktopEnvironment from "./DesktopEnvironment.ts";
import * as DesktopConfig from "./DesktopConfig.ts";

const defaultInput = {
  dirname: "/repo/apps/desktop/dist-electron",
  homeDirectory: "/Users/alice",
  platform: "darwin",
  processArch: "arm64",
  appVersion: "0.0.22",
  appPath: "/Applications/Scient.app/Contents/Resources/app.asar",
  isPackaged: false,
  resourcesPath: "/Applications/Scient.app/Contents/Resources",
  runningUnderArm64Translation: false,
} satisfies DesktopEnvironment.MakeDesktopEnvironmentInput;

const makeEnvironmentLayer = (
  overrides: Partial<DesktopEnvironment.MakeDesktopEnvironmentInput> = {},
  env: Record<string, string | undefined> = {},
) =>
  DesktopEnvironment.layer({
    ...defaultInput,
    ...overrides,
  }).pipe(Layer.provide(Layer.mergeAll(NodeServices.layer, DesktopConfig.layerTest(env))));

const makeEnvironment = (
  overrides: Partial<DesktopEnvironment.MakeDesktopEnvironmentInput> = {},
  env: Record<string, string | undefined> = {},
) =>
  DesktopEnvironment.DesktopEnvironment.pipe(Effect.provide(makeEnvironmentLayer(overrides, env)));

describe("DesktopEnvironment", () => {
  it.effect("derives state paths and development identity inside Effect", () =>
    Effect.gen(function* () {
      const environment = yield* makeEnvironment(
        {},
        {
          SCIENT_NEXT_HOME: " /tmp/scient-next ",
          T3CODE_COMMIT_HASH: " 0123456789abcdef ",
          T3CODE_PORT: "4949",
          VITE_DEV_SERVER_URL: "http://localhost:5173",
          T3CODE_DEV_REMOTE_T3_SERVER_ENTRY_PATH: " /remote/server.mjs ",
          T3CODE_OTLP_TRACES_URL: " http://127.0.0.1:4318/v1/traces ",
          T3CODE_OTLP_EXPORT_INTERVAL_MS: "2500",
        },
      );

      assert.equal(environment.isDevelopment, true);
      assert.equal(environment.appDataDirectory, "/Users/alice/Library/Application Support");
      assert.equal(environment.baseDir, "/tmp/scient-next");
      assert.equal(environment.stateDir, "/tmp/scient-next/scient-next-dev");
      assert.equal(
        environment.desktopSettingsPath,
        "/tmp/scient-next/scient-next-dev/desktop-settings.json",
      );
      assert.equal(
        environment.clientSettingsPath,
        "/tmp/scient-next/scient-next-dev/client-settings.json",
      );
      assert.equal(
        environment.savedEnvironmentRegistryPath,
        "/tmp/scient-next/scient-next-dev/saved-environments.json",
      );
      assert.equal(
        environment.serverSettingsPath,
        "/tmp/scient-next/scient-next-dev/settings.json",
      );
      assert.equal(environment.logDir, "/tmp/scient-next/scient-next-dev/logs");
      assert.equal(
        environment.browserArtifactsDir,
        "/tmp/scient-next/scient-next-dev/browser-artifacts",
      );
      assert.equal(environment.rootDir, "/repo");
      assert.equal(environment.appRoot, "/repo");
      assert.equal(environment.backendEntryPath, "/repo/apps/server/dist/bin.mjs");
      assert.equal(environment.backendCwd, "/repo");
      assert.equal(environment.appUserModelId, "com.scientfactory.scient.next.dev");
      assert.equal(environment.linuxWmClass, "scient-next-dev");
      assert.deepEqual(
        Option.map(environment.devServerUrl, (url) => url.href),
        Option.some("http://localhost:5173/"),
      );
      assert.deepEqual(environment.devRemoteT3ServerEntryPath, Option.some("/remote/server.mjs"));
      assert.deepEqual(environment.configuredBackendPort, Option.some(4949));
      assert.deepEqual(environment.commitHashOverride, Option.some("0123456789abcdef"));
      assert.deepEqual(environment.otlpTracesUrl, Option.some("http://127.0.0.1:4318/v1/traces"));
      assert.equal(environment.otlpExportIntervalMs, 2500);
    }),
  );

  it.effect("stores production state under userdata in an explicit home", () =>
    Effect.gen(function* () {
      const environment = yield* makeEnvironment(
        { isPackaged: true },
        {
          SCIENT_NEXT_HOME: "/tmp/scient-next",
        },
      );

      assert.equal(environment.isDevelopment, false);
      assert.equal(environment.stateDir, "/tmp/scient-next/userdata");
      assert.equal(environment.logDir, "/tmp/scient-next/userdata/logs");
      assert.equal(environment.browserArtifactsDir, "/tmp/scient-next/userdata/browser-artifacts");
      assert.equal(environment.serverSettingsPath, "/tmp/scient-next/userdata/settings.json");
    }),
  );

  it.effect("never opens production state from an unpackaged direct launch", () =>
    Effect.gen(function* () {
      const environment = yield* makeEnvironment(
        { isPackaged: false },
        { SCIENT_NEXT_HOME: "/tmp/scient-next" },
      );

      assert.equal(environment.isDevelopment, true);
      assert.equal(environment.stateDir, "/tmp/scient-next/scient-next-dev");
      assert.equal(environment.userDataDirName, "scient-next-dev");
    }),
  );

  it.effect("keeps implicit development state separate from production state", () =>
    Effect.gen(function* () {
      const development = yield* makeEnvironment(
        {},
        { VITE_DEV_SERVER_URL: "http://localhost:5173" },
      );
      const production = yield* makeEnvironment({ isPackaged: true });

      assert.equal(development.stateDir, "/Users/alice/.scient-next/scient-next-dev");
      assert.equal(production.stateDir, "/Users/alice/.scient-next/userdata");
    }),
  );

  it.effect("keeps the candidate app identity fixed", () =>
    Effect.gen(function* () {
      const environment = yield* makeEnvironment(
        {},
        {
          T3CODE_DESKTOP_APP_USER_MODEL_ID: " com.t3tools.t3code.dev.local ",
          VITE_DEV_SERVER_URL: "http://localhost:5173",
        },
      );

      assert.equal(environment.appUserModelId, "com.scientfactory.scient.next.dev");
    }),
  );

  it.effect("resolves picker defaults without nullish sentinels", () =>
    Effect.gen(function* () {
      const environment = yield* makeEnvironment();

      assert.deepEqual(environment.resolvePickFolderDefaultPath(null), Option.none());
      assert.deepEqual(
        environment.resolvePickFolderDefaultPath({ initialPath: " " }),
        Option.none(),
      );
      assert.deepEqual(
        environment.resolvePickFolderDefaultPath({ initialPath: "~" }),
        Option.some("/Users/alice"),
      );
      assert.deepEqual(
        environment.resolvePickFolderDefaultPath({ initialPath: "~/project" }),
        Option.some("/Users/alice/project"),
      );
    }),
  );
});
