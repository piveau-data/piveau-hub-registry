package io.piveau.hub.services.distributions;

import io.piveau.hub.util.TSConnector;
import io.piveau.hub.util.ValidationConnector;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface DistributionsService {

    String SERVICE_ADDRESS = "io.piveau.hub.distributions.queue";


    static DistributionsService create(TSConnector connector,  JsonObject config, Vertx vertx, Handler<AsyncResult<DistributionsService>> readyHandler) {
        return new DistributionsServiceImpl(connector,  config, vertx, readyHandler);
    }

    static DistributionsService createProxy(Vertx vertx, String address) {
        return new DistributionsServiceVertxEBProxy(vertx, address);
    }



    @Fluent
    DistributionsService getDistribution(String id, String consumes, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DistributionsService postDistribution (String distribution, String datasetId, String contentType, String catalogueId, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DistributionsService putDistribution(String distribution, String distributionID, String contentType, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    public DistributionsService putDistributionWithIdentifier(String distribution, String identifier, String contentType, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DistributionsService deleteDistribution(String id, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DistributionsService getDistributionByIdentifier(String id, String acceptType, Handler<AsyncResult<JsonObject>> getResponseHandler);
}
