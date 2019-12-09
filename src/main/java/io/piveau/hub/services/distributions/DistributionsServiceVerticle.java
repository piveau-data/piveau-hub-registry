package io.piveau.hub.services.distributions;


import io.piveau.hub.util.Constants;
import io.piveau.hub.util.TSConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class DistributionsServiceVerticle  extends AbstractVerticle {

        @Override
        public void start(Future<Void> startFuture) {
            WebClient client = WebClient.create(vertx);

            JsonObject conf = ConfigHelper.forConfig(config()).getJson(Constants.ENV_PIVEAU_HUB_TRIPLESTORE_CONFIG);

            CircuitBreaker breaker = CircuitBreaker.create("virtuoso-breaker", vertx, new CircuitBreakerOptions().setMaxRetries(5))
                    .retryPolicy(count -> count * 1000L);

            TSConnector connector = TSConnector.create(client, breaker, conf);


            DistributionsService.create(connector, config(), vertx, ready -> {
                if (ready.succeeded()) {
                    new ServiceBinder(vertx).setAddress(DistributionsService.SERVICE_ADDRESS).register(DistributionsService.class, ready.result());
                    startFuture.complete();
                } else {
                    startFuture.fail(ready.cause());
                }
            });
        }

    }
