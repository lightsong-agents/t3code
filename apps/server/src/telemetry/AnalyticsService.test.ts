import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientError from "effect/http/HttpClientError";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as HostProcess from "@t3tools/shared/HostProcess";

import * as ServerConfig from "../config.ts";
import { getTelemetryIdentity } from "./Identify.ts";
import * as AnalyticsService from "./AnalyticsService.ts";

type Respond = (
  request: HttpClientRequest.HttpClientRequest,
) => Effect.Effect<HttpClientResponse.HttpClientResponse, HttpClientError.HttpClientError>;

const respondWithStatus =
  (status: number): Respond =>
  (request) =>
    Effect.succeed(HttpClientResponse.fromWeb(request, new Response(null, { status })));

const recordTwentyEvents = Effect.gen(function* () {
  const analytics = yield* AnalyticsService.AnalyticsService;
  for (let index = 0; index < 20; index += 1) {
    yield* analytics.record("test.retry", { index });
  }
});

/**
 * Runs `record` (20 events by default), then lets `minutes` of background
 * flushes run against a client that answers every send with `respond`.
 * Returns the URL of every request the service made.
 */
const sendFor = (
  minutes: number,
  respond: Respond,
  record: Effect.Effect<
    void,
    never,
    AnalyticsService.AnalyticsService | Effect.Services<typeof getTelemetryIdentity>
  > = recordTwentyEvents,
) =>
  Effect.gen(function* () {
    const requests: Array<string> = [];
    const client = HttpClient.make((request) =>
      Effect.suspend(() => {
        requests.push(request.url);
        return respond(request);
      }),
    );
    const runtimeLayer = AnalyticsService.layer.pipe(
      Layer.provideMerge(
        ServerConfig.ServerConfig.layerTest(process.cwd(), { prefix: "t3-telemetry-retry-" }),
      ),
      Layer.provide(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown({
            T3CODE_TELEMETRY_ENABLED: true,
            T3CODE_POSTHOG_KEY: "phc_test_key",
            T3CODE_POSTHOG_HOST: "http://localhost",
          }),
        ),
      ),
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(HostProcess.Platform, "win32"),
          Layer.succeed(HostProcess.Architecture, "x64"),
          Layer.succeed(HttpClient.HttpClient, client),
        ),
      ),
    );

    yield* Effect.gen(function* () {
      yield* record;
      for (let second = 0; second < minutes * 60; second += 1) {
        yield* TestClock.adjust("1 second");
      }
    }).pipe(Effect.provide(runtimeLayer));
    return requests;
  });

it.layer(NodeServices.layer)("AnalyticsService test", (it) => {
  // Lightsong fork: delivery is permanently disabled, so the upstream delivery and retry tests
  // were removed. These tests prove nothing is sent even when telemetry is requested.
  it.effect(
    "sends nothing over 10 minutes of events and owner reports with telemetry requested",
    () =>
      Effect.gen(function* () {
        const requests = yield* sendFor(
          10,
          respondWithStatus(204),
          Effect.gen(function* () {
            yield* recordTwentyEvents;
            const analytics = yield* AnalyticsService.AnalyticsService;
            yield* analytics.recordOwners(
              [{ source: "connect", ownerId: "a".repeat(64) }],
              "server",
            );
            yield* analytics.flush;
          }),
        );
        assert.deepEqual(requests, []);
      }),
  );

  it.effect.each([false, true])(
    "does not send batch requests even when T3CODE_TELEMETRY_ENABLED=%s (fork lock)",
    (requested) =>
      Effect.gen(function* () {
        const capturedPaths: Array<string> = [];
        const layerServerConfig = ServerConfig.ServerConfig.layerTest(process.cwd(), {
          prefix: "t3-telemetry-disabled-",
        });
        const layerTelemetry = AnalyticsService.layer.pipe(Layer.provideMerge(layerServerConfig));
        const layerConfig = ConfigProvider.layer(
          ConfigProvider.fromUnknown({
            T3CODE_TELEMETRY_ENABLED: requested,
            T3CODE_POSTHOG_KEY: "phc_test_key",
            T3CODE_POSTHOG_HOST: "http://localhost",
          }),
        );
        const layerBatchServer = HttpServer.serve(
          Effect.gen(function* () {
            const request = yield* HttpServerRequest.HttpServerRequest;
            capturedPaths.push(request.url);
            return HttpServerResponse.jsonUnsafe({});
          }),
        );
        const layerRuntime = layerTelemetry.pipe(
          Layer.provide(layerConfig),
          Layer.provide(
            Layer.mergeAll(
              Layer.succeed(HostProcess.Platform, "linux"),
              Layer.succeed(HostProcess.Architecture, "arm64"),
            ),
          ),
          Layer.provideMerge(NodeHttpServer.layerTest),
        );

        yield* Effect.gen(function* () {
          yield* Layer.launch(layerBatchServer).pipe(Effect.forkScoped);
          const analytics = yield* AnalyticsService.AnalyticsService;
          yield* analytics.record("test.disabled", { index: 1 });
          yield* analytics.flush;
        }).pipe(Effect.provide(layerRuntime));

        assert.deepEqual(capturedPaths, []);
      }),
  );
});
