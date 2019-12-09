package io.piveau.hub;

import io.piveau.hub.services.metrics.MetricsService;
import io.piveau.hub.services.metrics.MetricsServiceVerticle;
import io.piveau.hub.util.Constants;
import io.piveau.hub.util.MockTripleStore;
import io.piveau.utils.JenaUtils;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.jena.rdf.model.Model;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("Testing the metrics service")
@ExtendWith(VertxExtension.class)
class MetricImplServiceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricImplServiceTest.class);
    private MetricsService metricsService;

    private String catalogueID = "test-catalog";
    private String baseURI = "https://europeandataportal.eu/";

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        DeploymentOptions options = new DeploymentOptions()
                .setConfig(new JsonObject()
                        .put(Constants.ENV_PIVEAU_HUB_TRIPLESTORE_CONFIG, MockTripleStore.getTriplestoreConfig(true))
                        .put(Constants.ENV_PIVEAU_HUB_BASE_URI, baseURI));

        Checkpoint checkpoint = testContext.checkpoint(3);
        vertx.deployVerticle(MockTripleStore.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                checkpoint.flag();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        vertx.deployVerticle(MetricsServiceVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                metricsService = MetricsService.createProxy(vertx, MetricsService.SERVICE_ADDRESS);
                checkpoint.flag();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        checkpoint.flag();
    }

    @AfterEach
    void tearDown(Vertx vertx, VertxTestContext testContext) {
        testContext.completeNow();
    }

    @Test
    @DisplayName("Create an example metric")
    void testCreateExampleMetric(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "test-put+metric";
        FileSystem fileSystem = vertx.fileSystem();
        Buffer buffer = fileSystem.readFileBlocking("misc/example_metric.ttl");

        metricsService.putMetric(datasetID, buffer.toString(), "text/turtle", catalogueID, null, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                assertEquals(result.getString("status", ""), "created");
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Receive an example metric")
    void testGetExampleMetric(Vertx vertx, VertxTestContext testContext) {

        DCATAPUriSchema.INSTANCE.setConfig(new JsonObject().put("baseUri", baseURI));
        FileSystem fileSystem = vertx.fileSystem();
        Buffer buffer = fileSystem.readFileBlocking("misc/example_metric.ttl");

        metricsService.putMetric("get-test-Metric", buffer.toString(), "text/turtle", catalogueID, null, ar -> {
            if (ar.succeeded()) {
                metricsService.getMetric("get-test-Metric", catalogueID, "text/turtle", ar2 -> {

                    if (ar2.succeeded()) {
                        JsonObject result = ar2.result();

                        assertEquals("success", result.getString("status", ""));
                        LOGGER.info(result.encodePrettily());
                        String model = result.getString("content");
                        assertNotNull(model);
                        Model jenaModel = JenaUtils.read(model.getBytes(), "text/turtle");
                        assertNotNull(jenaModel);
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar2.cause());
                    }
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Delete an example metric")
    void testDeleteExampleMetric(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "delete-test-dataset";
        DCATAPUriSchema.INSTANCE.setConfig(new JsonObject().put("baseUri", baseURI));
        FileSystem fileSystem = vertx.fileSystem();
        Buffer buffer = fileSystem.readFileBlocking("misc/example_metric.ttl");

        metricsService.putMetric(datasetID, buffer.toString(), "text/turtle", catalogueID, null, ar -> {
            if (ar.succeeded()) {

                metricsService.deleteMetric(datasetID, catalogueID, handler -> {
                    if (handler.succeeded()) {

                        metricsService.getMetric(datasetID, catalogueID, "text/turtle", ar2 -> {
                            if (ar2.succeeded()) {
                                JsonObject result = ar2.result();
                                assertNotNull(result);
                                assertEquals("not found", result.getString("status", ""));
                                testContext.completeNow();
                            } else {
                                testContext.failNow(ar2.cause());
                            }
                        });
                    } else {
                        testContext.failNow(handler.cause());
                    }
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

}
