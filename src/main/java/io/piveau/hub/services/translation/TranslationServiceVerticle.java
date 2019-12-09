package io.piveau.hub.services.translation;

import io.piveau.hub.util.Constants;
import io.piveau.hub.util.TSConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class TranslationServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject conf = ConfigHelper.forConfig(config()).forceJsonObject(Constants.ENV_PIVEAU_HUB_TRIPLESTORE_CONFIG);
        WebClient webClient = WebClient.create(vertx);
        CircuitBreaker circuitBreaker = CircuitBreaker.create("virtuoso-breaker", vertx, new CircuitBreakerOptions().setMaxRetries(5)).retryPolicy(count -> count * 1000L);
        TSConnector tsConnector = TSConnector.create(webClient, circuitBreaker, conf);

        TranslationService.create(vertx, webClient, config(), tsConnector, readyHandler -> {
            if (readyHandler.succeeded()) {
                new ServiceBinder(vertx)
                        .setAddress(TranslationService.SERVICE_ADDRESS)
                        .register(TranslationService.class, readyHandler.result());
                startPromise.complete();
            } else if (readyHandler.failed()) {
                startPromise.fail(readyHandler.cause());
            }
        });
    }
}
