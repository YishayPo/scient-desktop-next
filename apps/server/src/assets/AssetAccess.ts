import {
  ArtifactAuthority,
  ArtifactId,
  ArtifactRevisionId,
} from "@scientfactory/document-artifacts";
import { AnalysisArtifactResourceRef } from "@scientfactory/analysis";
import type { AssetResource } from "@t3tools/contracts";
import {
  AssetAnalysisArtifactNotFoundError,
  AssetAttachmentNotFoundError,
  AssetGeneratedDocumentAuthorityMismatchError,
  AssetGeneratedDocumentNotFoundError,
  AssetPreviewTypeValidationError,
  AssetProjectFaviconInspectionError,
  AssetProjectFaviconNotFoundError,
  AssetProjectFaviconResolutionError,
  AssetSigningKeyLoadError,
  AssetWorkspaceAssetInspectionError,
  AssetWorkspaceAssetNotFoundError,
  AssetWorkspaceContextNotFoundError,
  AssetWorkspacePathValidationError,
  AssetWorkspaceResolutionError,
  AssetWorkspaceRootNormalizationError,
} from "@t3tools/contracts";
import {
  isWorkspaceImagePreviewPath,
  isWorkspacePdfPreviewPath,
  isWorkspacePreviewEntryPath,
  WORKSPACE_BROWSER_PREVIEW_EXTENSIONS,
  WORKSPACE_IMAGE_PREVIEW_EXTENSIONS,
} from "@t3tools/shared/filePreview";
import { PROJECT_FAVICON_FALLBACK_MARKER } from "@t3tools/shared/projectFavicon";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";

import {
  base64UrlDecodeUtf8,
  base64UrlEncode,
  signPayload,
  timingSafeEqualBase64Url,
} from "../auth/utils.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { resolveAttachmentPathById } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as ProjectFaviconResolver from "../project/ProjectFaviconResolver.ts";
import type { ResolvedGeneratedDocumentRevision } from "../scient/documentArtifacts/GeneratedDocumentStore.ts";
import type { ResolvedAnalysisArtifactRepresentation } from "../scient/analysis/LocalAnalysisStore.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";

export const ASSET_ROUTE_PREFIX = "/api/assets";

const SIGNING_SECRET_NAME = "asset-access-signing-key";
const ASSET_TOKEN_TTL_MS = 60 * 60 * 1000;
const PROJECT_FAVICON_TOKEN_BUCKET_MS = 30 * 60 * 1000;
const PROJECT_FAVICON_VERSION_PREFIX = "v";
const PREVIEW_ASSET_EXTENSIONS = new Set([
  ...WORKSPACE_BROWSER_PREVIEW_EXTENSIONS,
  ...WORKSPACE_IMAGE_PREVIEW_EXTENSIONS,
  ".css",
  ".js",
  ".mjs",
  ".otf",
  ".ttf",
  ".woff",
  ".woff2",
]);

const AssetClaimsSchema = Schema.Union([
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("workspace-file"),
    workspaceRoot: Schema.String,
    baseRelativePath: Schema.String,
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("workspace-file-exact"),
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
    expiresAt: Schema.Number,
    revisionSize: Schema.optional(Schema.Number),
    revisionMtimeMs: Schema.optional(Schema.NullOr(Schema.Number)),
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("attachment"),
    attachmentId: Schema.String,
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("project-favicon"),
    workspaceRoot: Schema.String,
    relativePath: Schema.NullOr(Schema.String),
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("generated-document"),
    authority: ArtifactAuthority,
    artifactId: ArtifactId,
    revisionId: ArtifactRevisionId,
    path: Schema.String,
    fileName: Schema.String,
    expiresAt: Schema.Number,
    revisionSize: Schema.Number,
    revisionMtimeMs: Schema.NullOr(Schema.Number),
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("analysis-artifact"),
    ...AnalysisArtifactResourceRef.fields,
    path: Schema.String,
    fileName: Schema.String,
    expiresAt: Schema.Number,
    revisionSize: Schema.Number,
    revisionMtimeMs: Schema.NullOr(Schema.Number),
  }),
]);
type AssetClaims = typeof AssetClaimsSchema.Type;

const AssetClaimsJson = Schema.fromJsonString(AssetClaimsSchema);
const decodeAssetClaims = Schema.decodeUnknownOption(AssetClaimsJson);
const encodeAssetClaims = Schema.encodeSync(AssetClaimsJson);

export type ResolvedAsset = {
  readonly kind: "file";
  readonly path: string;
  readonly revision?: { readonly size: number; readonly mtimeMs: number | null };
};

function decodeClaims(encodedPayload: string): AssetClaims | null {
  try {
    return Option.getOrNull(decodeAssetClaims(base64UrlDecodeUtf8(encodedPayload)));
  } catch {
    return null;
  }
}

function decodeRelativePath(value: string): string | null {
  try {
    return decodeURIComponent(value);
  } catch {
    return null;
  }
}

const optionOnNotFound = <A, R>(
  effect: Effect.Effect<A, PlatformError.PlatformError, R>,
): Effect.Effect<Option.Option<A>, PlatformError.PlatformError, R> =>
  effect.pipe(
    Effect.map(Option.some),
    Effect.catchTags({
      PlatformError: (error) =>
        error.reason._tag === "NotFound" ? Effect.succeed(Option.none<A>()) : Effect.fail(error),
    }),
  );

const resolveCanonicalWorkspaceFile = Effect.fn("AssetAccess.resolveCanonicalWorkspaceFile")(
  function* (input: { readonly workspaceRoot: string; readonly relativePath: string }) {
    const fileSystem = yield* FileSystem.FileSystem;
    const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
    const resolved = yield* workspacePaths.resolveRelativePathWithinRoot(input).pipe(
      Effect.map(Option.some),
      Effect.catchTags({
        WorkspacePathOutsideRootError: () => Effect.succeed(Option.none()),
      }),
    );
    if (Option.isNone(resolved)) return null;

    const [canonicalRoot, canonicalFile] = yield* Effect.all([
      optionOnNotFound(fileSystem.realPath(input.workspaceRoot)),
      optionOnNotFound(fileSystem.realPath(resolved.value.absolutePath)),
    ]);
    if (Option.isNone(canonicalRoot) || Option.isNone(canonicalFile)) return null;

    const path = yield* Path.Path;
    const relative = path.relative(canonicalRoot.value, canonicalFile.value);
    if (relative === "" || relative.startsWith("..") || path.isAbsolute(relative)) return null;

    const info = yield* optionOnNotFound(fileSystem.stat(canonicalFile.value));
    return Option.isSome(info) && info.value.type === "File" ? canonicalFile.value : null;
  },
);

const resolveCanonicalWorkspaceFileForRequest = (input: {
  readonly workspaceRoot: string;
  readonly relativePath: string;
}) =>
  resolveCanonicalWorkspaceFile(input).pipe(
    Effect.tapError((cause) =>
      Effect.logError("Failed to resolve canonical asset path.", {
        workspaceRoot: input.workspaceRoot,
        relativePath: input.relativePath,
        cause,
      }),
    ),
    Effect.orElseSucceed(() => null),
  );

export const issueAssetUrl = Effect.fn("AssetAccess.issueAssetUrl")(function* (input: {
  readonly resource: AssetResource;
  readonly workspaceRoot?: string;
  readonly projectFaviconPath?: string;
  readonly generatedDocument?: ResolvedGeneratedDocumentRevision;
  readonly analysisArtifact?: ResolvedAnalysisArtifactRepresentation;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
  let expiresAt = (yield* Clock.currentTimeMillis) + ASSET_TOKEN_TTL_MS;
  let claims: AssetClaims;
  let fileName: string;
  let sourcePath: string | undefined;

  switch (input.resource._tag) {
    case "workspace-file": {
      if (!input.workspaceRoot) {
        return yield* new AssetWorkspaceContextNotFoundError({
          resource: input.resource,
        });
      }
      const workspaceRoot = yield* workspacePaths.normalizeWorkspaceRoot(input.workspaceRoot).pipe(
        Effect.mapError(
          (cause) =>
            new AssetWorkspaceRootNormalizationError({
              resource: input.resource,
              cause,
            }),
        ),
      );
      const relativePath = path.isAbsolute(input.resource.path)
        ? path.relative(workspaceRoot, input.resource.path)
        : input.resource.path;
      const resolved = yield* workspacePaths
        .resolveRelativePathWithinRoot({ workspaceRoot, relativePath })
        .pipe(
          Effect.mapError(
            (cause) =>
              new AssetWorkspacePathValidationError({
                resource: input.resource,
                cause,
              }),
          ),
        );
      if (!isWorkspacePreviewEntryPath(resolved.relativePath)) {
        return yield* new AssetPreviewTypeValidationError({
          resource: input.resource,
        });
      }
      const canonicalFile = yield* resolveCanonicalWorkspaceFile({
        workspaceRoot,
        relativePath: resolved.relativePath,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new AssetWorkspaceAssetInspectionError({
              resource: input.resource,
              cause,
            }),
        ),
      );
      if (!canonicalFile) {
        return yield* new AssetWorkspaceAssetNotFoundError({
          resource: input.resource,
        });
      }
      const canonicalWorkspaceRoot = yield* fileSystem.realPath(workspaceRoot).pipe(
        Effect.mapError(
          (cause) =>
            new AssetWorkspaceResolutionError({
              resource: input.resource,
              cause,
            }),
        ),
      );
      claims =
        isWorkspaceImagePreviewPath(resolved.relativePath) ||
        isWorkspacePdfPreviewPath(resolved.relativePath)
          ? yield* Effect.gen(function* () {
              const revision = isWorkspacePdfPreviewPath(resolved.relativePath)
                ? yield* fileSystem.stat(canonicalFile).pipe(
                    Effect.map(
                      (info) =>
                        ({
                          revisionSize: Number(info.size),
                          revisionMtimeMs: Option.match(info.mtime, {
                            onNone: () => null,
                            onSome: (mtime) => mtime.getTime(),
                          }),
                        }) as const,
                    ),
                    Effect.mapError(
                      (cause) =>
                        new AssetWorkspaceAssetInspectionError({
                          resource: input.resource,
                          cause,
                        }),
                    ),
                  )
                : {};
              return {
                version: 1 as const,
                kind: "workspace-file-exact" as const,
                workspaceRoot: canonicalWorkspaceRoot,
                relativePath: resolved.relativePath,
                expiresAt,
                ...revision,
              };
            })
          : {
              version: 1,
              kind: "workspace-file",
              workspaceRoot: canonicalWorkspaceRoot,
              baseRelativePath: path.dirname(resolved.relativePath),
              expiresAt,
            };
      fileName = path.basename(resolved.relativePath);
      break;
    }
    case "attachment": {
      const config = yield* ServerConfig.ServerConfig;
      const attachmentPath = resolveAttachmentPathById({
        attachmentsDir: config.attachmentsDir,
        attachmentId: input.resource.attachmentId,
      });
      if (!attachmentPath) {
        return yield* new AssetAttachmentNotFoundError({
          resource: input.resource,
        });
      }
      claims = {
        version: 1,
        kind: "attachment",
        attachmentId: input.resource.attachmentId,
        expiresAt,
      };
      fileName = path.basename(attachmentPath);
      break;
    }
    case "project-favicon": {
      const workspaceRoot = yield* workspacePaths.normalizeWorkspaceRoot(input.resource.cwd).pipe(
        Effect.mapError(
          (cause) =>
            new AssetWorkspaceRootNormalizationError({
              resource: input.resource,
              cause,
            }),
        ),
      );
      const faviconResolver = yield* ProjectFaviconResolver.ProjectFaviconResolver;
      const faviconPath = yield* faviconResolver
        .resolvePath(workspaceRoot, input.projectFaviconPath ?? undefined)
        .pipe(
          Effect.mapError(
            (cause) =>
              new AssetProjectFaviconResolutionError({
                resource: input.resource,
                cause,
              }),
          ),
        );
      const relativePath = faviconPath ? path.relative(workspaceRoot, faviconPath) : null;
      if (relativePath && !isWorkspaceImagePreviewPath(relativePath)) {
        return yield* new AssetPreviewTypeValidationError({ resource: input.resource });
      }
      sourcePath = relativePath ?? undefined;
      const canonicalFaviconPath = relativePath
        ? yield* resolveCanonicalWorkspaceFile({ workspaceRoot, relativePath }).pipe(
            Effect.mapError(
              (cause) =>
                new AssetProjectFaviconInspectionError({
                  resource: input.resource,
                  cause,
                }),
            ),
          )
        : null;
      if (relativePath && !canonicalFaviconPath) {
        return yield* new AssetProjectFaviconNotFoundError({
          resource: input.resource,
        });
      }
      claims = {
        version: 1,
        kind: "project-favicon",
        workspaceRoot: yield* fileSystem.realPath(workspaceRoot).pipe(
          Effect.mapError(
            (cause) =>
              new AssetWorkspaceResolutionError({
                resource: input.resource,
                cause,
              }),
          ),
        ),
        relativePath,
        expiresAt,
      };
      if (relativePath && canonicalFaviconPath) {
        const crypto = yield* Crypto.Crypto;
        const faviconBytes = yield* fileSystem.readFile(canonicalFaviconPath).pipe(
          Effect.mapError(
            (cause) =>
              new AssetProjectFaviconInspectionError({
                resource: input.resource,
                cause,
              }),
          ),
        );
        const revision = yield* crypto.digest("SHA-256", faviconBytes).pipe(
          Effect.map(Encoding.encodeHex),
          Effect.mapError(
            (cause) =>
              new AssetProjectFaviconInspectionError({
                resource: input.resource,
                cause,
              }),
          ),
        );
        fileName = `${PROJECT_FAVICON_VERSION_PREFIX}${revision}-${path.basename(relativePath)}`;
      } else {
        fileName = PROJECT_FAVICON_FALLBACK_MARKER;
      }
      break;
    }
    case "generated-document": {
      if (!input.generatedDocument) {
        return yield* new AssetGeneratedDocumentNotFoundError({ resource: input.resource });
      }
      if (input.generatedDocument.artifact.authority !== input.resource.authority) {
        return yield* new AssetGeneratedDocumentAuthorityMismatchError({
          resource: input.resource,
        });
      }
      if (
        input.generatedDocument.artifact.artifactId !== input.resource.artifactId ||
        input.generatedDocument.artifact.revisionId !== input.resource.revisionId
      ) {
        return yield* new AssetGeneratedDocumentNotFoundError({ resource: input.resource });
      }
      const config = yield* ServerConfig.ServerConfig;
      const [canonicalArtifactsRoot, canonicalGeneratedPath] = yield* Effect.all([
        fileSystem.realPath(config.documentArtifactsDir),
        fileSystem.realPath(input.generatedDocument.path),
      ]).pipe(
        Effect.mapError(
          () => new AssetGeneratedDocumentNotFoundError({ resource: input.resource }),
        ),
      );
      const generatedRelativePath = path.relative(canonicalArtifactsRoot, canonicalGeneratedPath);
      if (
        generatedRelativePath === "" ||
        generatedRelativePath.startsWith("..") ||
        path.isAbsolute(generatedRelativePath)
      ) {
        return yield* new AssetGeneratedDocumentNotFoundError({ resource: input.resource });
      }
      claims = {
        version: 1,
        kind: "generated-document",
        authority: input.generatedDocument.artifact.authority,
        artifactId: input.generatedDocument.artifact.artifactId,
        revisionId: input.generatedDocument.artifact.revisionId,
        path: canonicalGeneratedPath,
        fileName: input.generatedDocument.fileName,
        expiresAt,
        revisionSize: input.generatedDocument.revision.size,
        revisionMtimeMs: input.generatedDocument.revision.mtimeMs,
      };
      fileName = input.generatedDocument.fileName;
      break;
    }
    case "analysis-artifact": {
      const resolved = input.analysisArtifact;
      if (
        !resolved ||
        resolved.artifact.artifactId !== input.resource.artifactId ||
        resolved.representation.representationId !== input.resource.representationId
      ) {
        return yield* new AssetAnalysisArtifactNotFoundError({ resource: input.resource });
      }
      const config = yield* ServerConfig.ServerConfig;
      const [canonicalAnalysisRoot, canonicalArtifactPath] = yield* Effect.all([
        fileSystem.realPath(config.analysisDir),
        fileSystem.realPath(resolved.path),
      ]).pipe(
        Effect.mapError(() => new AssetAnalysisArtifactNotFoundError({ resource: input.resource })),
      );
      const analysisRelativePath = path.relative(canonicalAnalysisRoot, canonicalArtifactPath);
      if (
        analysisRelativePath === "" ||
        analysisRelativePath.startsWith("..") ||
        path.isAbsolute(analysisRelativePath)
      ) {
        return yield* new AssetAnalysisArtifactNotFoundError({ resource: input.resource });
      }
      claims = {
        version: 1,
        kind: "analysis-artifact",
        projectId: input.resource.projectId,
        runId: input.resource.runId,
        artifactId: input.resource.artifactId,
        representationId: input.resource.representationId,
        path: canonicalArtifactPath,
        fileName: resolved.representation.fileName,
        expiresAt,
        revisionSize: resolved.revision.size,
        revisionMtimeMs: resolved.revision.mtimeMs,
      };
      fileName = resolved.representation.fileName;
      break;
    }
  }

  const secretStore = yield* ServerSecretStore.ServerSecretStore;
  const signingSecret = yield* secretStore.getOrCreateRandom(SIGNING_SECRET_NAME, 32).pipe(
    Effect.mapError(
      (cause) =>
        new AssetSigningKeyLoadError({
          resource: input.resource,
          cause,
        }),
    ),
  );
  if (claims.kind === "project-favicon") {
    const issuedAt = yield* Clock.currentTimeMillis;
    expiresAt =
      (Math.floor(issuedAt / PROJECT_FAVICON_TOKEN_BUCKET_MS) + 2) *
      PROJECT_FAVICON_TOKEN_BUCKET_MS;
    claims = { ...claims, expiresAt };
  }
  const encodedPayload = base64UrlEncode(encodeAssetClaims(claims));
  const token = `${encodedPayload}.${signPayload(encodedPayload, signingSecret)}`;
  return {
    relativeUrl: `${ASSET_ROUTE_PREFIX}/${token}/${encodeURIComponent(fileName)}`,
    expiresAt,
    ...(sourcePath !== undefined ? { sourcePath } : {}),
  };
});

export const resolveAsset = Effect.fn("AssetAccess.resolveAsset")(function* (
  token: string,
  relativePath: string,
) {
  const [encodedPayload, signature] = token.split(".");
  if (!encodedPayload || !signature) return null;

  const secretStore = yield* ServerSecretStore.ServerSecretStore;
  const signingSecret = yield* secretStore.getOrCreateRandom(SIGNING_SECRET_NAME, 32).pipe(
    Effect.tapError((cause) => Effect.logError("Failed to load the asset signing key.", { cause })),
    Effect.orElseSucceed(() => null),
  );
  if (!signingSecret) return null;
  if (!timingSafeEqualBase64Url(signature, signPayload(encodedPayload, signingSecret))) return null;

  const claims = decodeClaims(encodedPayload);
  if (!claims || claims.expiresAt <= (yield* Clock.currentTimeMillis)) return null;

  if (claims.kind === "attachment") {
    const config = yield* ServerConfig.ServerConfig;
    const attachmentPath = resolveAttachmentPathById({
      attachmentsDir: config.attachmentsDir,
      attachmentId: claims.attachmentId,
    });
    if (!attachmentPath) return null;
    const fileSystem = yield* FileSystem.FileSystem;
    const info = yield* optionOnNotFound(fileSystem.stat(attachmentPath)).pipe(
      Effect.tapError((cause) =>
        Effect.logError("Failed to inspect attachment asset.", {
          attachmentId: claims.attachmentId,
          path: attachmentPath,
          cause,
        }),
      ),
      Effect.orElseSucceed(() => Option.none()),
    );
    return Option.isSome(info) && info.value.type === "File"
      ? ({ kind: "file", path: attachmentPath } satisfies ResolvedAsset)
      : null;
  }

  if (claims.kind === "project-favicon") {
    if (claims.relativePath === null) return null;
    const faviconPath = yield* resolveCanonicalWorkspaceFileForRequest({
      workspaceRoot: claims.workspaceRoot,
      relativePath: claims.relativePath,
    });
    return faviconPath ? ({ kind: "file", path: faviconPath } satisfies ResolvedAsset) : null;
  }

  if (claims.kind === "generated-document") {
    const decodedPath = decodeRelativePath(relativePath);
    if (decodedPath === null || decodedPath !== claims.fileName) return null;
    return {
      kind: "file",
      path: claims.path,
      revision: {
        size: claims.revisionSize,
        mtimeMs: claims.revisionMtimeMs,
      },
    } satisfies ResolvedAsset;
  }

  if (claims.kind === "analysis-artifact") {
    const decodedPath = decodeRelativePath(relativePath);
    if (decodedPath === null || decodedPath !== claims.fileName) return null;
    return {
      kind: "file",
      path: claims.path,
      revision: {
        size: claims.revisionSize,
        mtimeMs: claims.revisionMtimeMs,
      },
    } satisfies ResolvedAsset;
  }

  const decodedPath = decodeRelativePath(relativePath);
  if (decodedPath === null) return null;
  const path = yield* Path.Path;
  if (claims.kind === "workspace-file-exact") {
    if (decodedPath !== path.basename(claims.relativePath)) return null;
    const exactWorkspaceFile = yield* resolveCanonicalWorkspaceFileForRequest({
      workspaceRoot: claims.workspaceRoot,
      relativePath: claims.relativePath,
    });
    return exactWorkspaceFile
      ? ({
          kind: "file",
          path: exactWorkspaceFile,
          ...(claims.revisionSize === undefined
            ? {}
            : {
                revision: {
                  size: claims.revisionSize,
                  mtimeMs: claims.revisionMtimeMs ?? null,
                },
              }),
        } satisfies ResolvedAsset)
      : null;
  }
  const segments = decodedPath.split(/[\\/]/);
  if (
    decodedPath.length === 0 ||
    decodedPath.includes("\0") ||
    segments.some((segment) => segment === "." || segment === ".." || segment.startsWith(".")) ||
    !PREVIEW_ASSET_EXTENSIONS.has(path.extname(decodedPath).toLowerCase())
  ) {
    return null;
  }
  const joinedRelativePath =
    claims.baseRelativePath === "." ? decodedPath : path.join(claims.baseRelativePath, decodedPath);
  const workspaceFile = yield* resolveCanonicalWorkspaceFileForRequest({
    workspaceRoot: claims.workspaceRoot,
    relativePath: joinedRelativePath,
  });
  return workspaceFile ? ({ kind: "file", path: workspaceFile } satisfies ResolvedAsset) : null;
});
