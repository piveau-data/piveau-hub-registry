package io.piveau.hub.services.metrics;

import io.piveau.hub.util.DataUploadConnector;
import io.piveau.hub.util.TSConnector;
import io.piveau.pipe.PipeLauncher;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface MetricsService {
    String SERVICE_ADDRESS = "io.piveau.hub.metrics.queue";

    static MetricsService create(TSConnector connector, JsonObject config, Vertx vertx, Handler<AsyncResult<MetricsService>> readyHandler) {
        return new MetricsServiceImpl(connector, config, vertx, readyHandler);
    }

    static MetricsService createProxy(Vertx vertx, String address) {
        return new MetricsServiceVertxEBProxy(vertx, address);
    }

    @Fluent
    MetricsService getMetric(String datasetId, String catalogueId, String consumes, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    MetricsService putMetric(String datasetId, String dataset, String contentType, String catalogueId, String hash, Handler<AsyncResult<JsonObject>> handler);

/*    @Fluent
    MetricsService postMetric(String dataset, String contentType, Handler<AsyncResult<JsonObject>> handler);
*/
    @Fluent
    MetricsService deleteMetric(String datasetId, String catalogueId, Handler<AsyncResult<JsonObject>> handler);

}
