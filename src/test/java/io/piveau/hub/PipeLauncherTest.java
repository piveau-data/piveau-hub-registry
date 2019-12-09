package io.piveau.hub;

import com.fasterxml.jackson.databind.util.JSONPObject;
import io.piveau.hub.services.datasets.DatasetsService;
import io.piveau.hub.services.datasets.DatasetsServiceVerticle;
import io.piveau.hub.util.Constants;
import io.piveau.hub.util.MockTripleStore;
import io.piveau.pipe.Payload;
import io.piveau.pipe.Pipe;
import io.piveau.rdf.RDFMimeTypes;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Testing the launcher")
@ExtendWith(VertxExtension.class)
class PipeLauncherTest {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private DatasetsService datasetsService;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        String baseUri = "https://example.eu/";
        DCATAPUriSchema.INSTANCE.setConfig(new JsonObject().put("baseUri", baseUri));

        DeploymentOptions options = new DeploymentOptions()
                .setConfig(new JsonObject()
                        .put(Constants.ENV_PIVEAU_HUB_TRIPLESTORE_CONFIG, MockTripleStore.getTriplestoreConfig(true))
                        .put(Constants.ENV_PIVEAU_HUB_BASE_URI, baseUri)
                        .put(Constants.ENV_PIVEAU_HUB_LOAD_VOCABULARIES, false)
                        .put(Constants.ENV_PIVEAU_CLUSTER_CONFIG, "{\"serviceDiscovery\": {\"launcher-test-segment\": {\"endpoints\": {\"http\": {\"address\": \"http://localhost:8098/pipe\"}}}}}")
                        .put(Constants.ENV_PIVEAU_HUB_VALIDATOR, new JsonObject().put("url", "localhost").put("enabled", true).put("validationPipeName", "launcher-test-pipe"))
                        .put(Constants.ENV_PIVEAU_TRANSLATION_SERVICE, new JsonObject().put("enable", false)));

        Checkpoint checkpoint = testContext.checkpoint(3);
        vertx.deployVerticle(MockTripleStore.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                checkpoint.flag();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        vertx.deployVerticle(DatasetsServiceVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                datasetsService = DatasetsService.createProxy(vertx, DatasetsService.SERVICE_ADDRESS);
                checkpoint.flag();
            } else {
                testContext.failNow(ar.cause());
            }
        });

        vertx.eventBus().consumer("io.piveau.hub.index.queue", message -> {
            message.reply(new JsonObject());
        });
        vertx.eventBus().<JsonObject>consumer("io.piveau.hub.translationservice.queue", message -> {
            message.reply(message.body().getJsonObject("helper"));
        });
        checkpoint.flag();
    }

    @Test
    @DisplayName("Put dataset and launch test pipe")
    void testLaunchPipe(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);

        vertx.createHttpServer().requestHandler(request -> {
            testContext.verify(() -> {
                assertEquals(request.method(), HttpMethod.POST);
                assertEquals(request.path(), "/pipe");
                assertEquals(request.getHeader("Content-Type"), "application/json");

                request.bodyHandler(buffer -> {
                    log.debug(buffer.toJsonObject().encodePrettily());
                    Pipe pipe = Json.mapper.convertValue(buffer.toJsonObject(), Pipe.class);

                    Payload payload = pipe.getBody().getSegments().get(0).getBody().getPayload();
                    assertEquals(payload.getBody().getDataMimeType(), RDFMimeTypes.TURTLE);
                    assertEquals(payload.getBody().getDataInfo().get("identifier").textValue(), "launch-test-dataset");
                    assertNotNull(payload.getBody().getData());
                });
            });
            request.response().setStatusCode(202).end();
            checkpoint.flag();
        }).listen(8098);

        datasetsService.putDataset("launch-test-dataset", vertx.fileSystem().readFileBlocking("misc/example_dataset.ttl").toString(), "text/turtle", "test-catalog", null, false, ar -> {
            if (ar.succeeded()) {
                checkpoint.flag();
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

}
