package io.piveau.hub.services.catalogues;

import io.piveau.hub.util.Constants;
import io.piveau.hub.util.TSConnector;
import io.piveau.hub.util.ValidationConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class CataloguesServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        WebClient client = WebClient.create(vertx);

        ConfigHelper configHelper = ConfigHelper.forConfig(config());
        JsonObject conf = configHelper.forceJsonObject(Constants.ENV_PIVEAU_HUB_TRIPLESTORE_CONFIG);

        CircuitBreaker breaker = CircuitBreaker.create("virtuoso-breaker", vertx, new CircuitBreakerOptions().setMaxRetries(5))
                .retryPolicy(count -> count * 1000L);

        TSConnector connector = TSConnector.create(client, breaker, conf);

        CataloguesService.create(connector, vertx, ready -> {
            if (ready.succeeded()) {
                new ServiceBinder(vertx).setAddress(CataloguesService.SERVICE_ADDRESS).register(CataloguesService.class, ready.result());
                startPromise.complete();
            } else {
                startPromise.fail(ready.cause());
            }
        });
    }
}
