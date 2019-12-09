package io.piveau.hub.services.metrics;

import io.piveau.hub.util.Constants;
import io.piveau.hub.util.TSConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class MetricsServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        WebClient client = WebClient.create(vertx);
        JsonObject conf = ConfigHelper.forConfig(config()).forceJsonObject(Constants.ENV_PIVEAU_HUB_TRIPLESTORE_CONFIG);

        CircuitBreaker breaker = CircuitBreaker.create("virtuoso-metrics-breaker", vertx, new CircuitBreakerOptions().setMaxRetries(5))
                .retryPolicy(count -> count * 1000L);
        TSConnector connector = TSConnector.create(client, breaker, conf);

        MetricsService.create(connector, config(), vertx, ready -> {
            if (ready.succeeded()) {
                new ServiceBinder(vertx).setAddress(MetricsService.SERVICE_ADDRESS).register(MetricsService.class, ready.result());
                startPromise.complete();
            } else {
                startPromise.fail(ready.cause());
            }
        });
    }

}
