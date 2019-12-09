package io.piveau.hub;

import io.piveau.hub.services.catalogues.CataloguesService;
import io.piveau.hub.services.catalogues.CataloguesServiceVerticle;
import io.piveau.hub.util.Constants;
import io.piveau.hub.util.MockTripleStore;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.utils.JenaUtils;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.sparql.resultset.ResultsFormat;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Testing the catalogue service")
@ExtendWith(VertxExtension.class)
class CatalogueImplServiceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogueImplServiceTest.class);
    private CataloguesService cataloguesService;

    private String exampleCatalogue;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {

        DeploymentOptions options = new DeploymentOptions()
                .setConfig(new JsonObject()
                        .put(Constants.ENV_PIVEAU_HUB_TRIPLESTORE_CONFIG, MockTripleStore.getTriplestoreConfig(false)));

        Checkpoint checkpoint = testContext.checkpoint(3);

        vertx.deployVerticle(MockTripleStore.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                checkpoint.flag();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        vertx.deployVerticle(CataloguesServiceVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                cataloguesService = CataloguesService.createProxy(vertx, CataloguesService.SERVICE_ADDRESS);
                checkpoint.flag();
            } else {
                testContext.failNow(ar.cause());
            }
        });

        vertx.fileSystem().readFile("misc/example_catalog.ttl", ar -> {
            if (ar.succeeded()) {
                exampleCatalogue = ar.result().toString();
                checkpoint.flag();
            } else {
                testContext.failNow(ar.cause());
            }
        });

        vertx.eventBus().consumer("io.piveau.hub.index.queue", message -> {
            message.reply(new JsonObject());
        });
        vertx.eventBus().consumer("io.piveau.hub.translationservice.queue", message -> {
            message.reply(new JsonObject());
        });
    }

    @Test
    @DisplayName("Create an example catalogue")
    void testCreateExampleCatalog(Vertx vertx, VertxTestContext testContext) {
        cataloguesService.putCatalogue("create-test-catalogue", exampleCatalogue, "text/turtle", null, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                LOGGER.info(result.encodePrettily());
                testContext.verify(() -> {
                    assertEquals(result.getString("status", ""), "created");
                });
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Receive an example catalogue")
    void testGetExampleCatalog(Vertx vertx, VertxTestContext testContext) {
        cataloguesService.putCatalogue("get-test-catalogue", exampleCatalogue, "text/turtle", null, ar -> {
            if (ar.succeeded()) {
                cataloguesService.getCatalogue("get-test-catalogue", "text/turtle", ar2 -> {
                    if (ar2.succeeded()) {
                        JsonObject result = ar2.result();
                        testContext.verify(() -> {
                            assertEquals("success", result.getString("status", ""));
                            LOGGER.info(result.encodePrettily());
                            String model = result.getString("content");
                            assertNotNull(model);
                            Model jenaModel = JenaUtils.read(model.getBytes(), "text/turtle");
                            assertNotNull(jenaModel);

                            Resource cat = jenaModel.getResource(DCATAPUriSchema.applyFor("get-test-catalogue").getCatalogueUriRef());
                            assertNotNull(cat);
                            assertNotNull(cat.getProperty(RDF.type));
                            assertEquals(cat.getProperty(RDF.type).getObject(), DCAT.Catalog);
                        });
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
    @DisplayName("Delete an example catalogue")
    void testDeleteExampleCatalog(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "delete-test-catalogue";
        cataloguesService.putCatalogue(datasetID, exampleCatalogue, "text/turtle", null, ar -> {
            if (ar.succeeded()) {
               cataloguesService.deleteCatalogue(datasetID, handler -> {
                    if (handler.succeeded()) {
                        cataloguesService.getCatalogue(datasetID, "text/turtle", ar2 -> {
                            if (ar2.succeeded()) {
                                JsonObject result = ar2.result();
                                testContext.verify(() -> {
                                    assertNotNull(result);
                                    assertEquals("not found", result.getString("status", ""));
                                });
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

    @Test
    @DisplayName("Delete an non existing catalogue")
    void testDeleteMissingCatalog(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "delete-missing-test-catalogue";
        cataloguesService.deleteCatalogue(datasetID, handler -> {
            testContext.verify(() -> assertFalse(handler.succeeded()));
            testContext.completeNow();
        });
    }

    @Test
    @DisplayName("Counting zero catalogues")
    void testCountZeroCatalogs(Vertx vertx, VertxTestContext testContext) {
        cataloguesService.listCatalogues("application/json", 20, 0, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                testContext.verify(() -> {
                    assertNotNull(result);
                    assertEquals("success", result.getString("status", ""));
                    assertNotNull(result.getString("content"));
                    PiveauLoggerFactory.getLogger(getClass()).debug(result.getString("content"));
                    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(result.getString("content").getBytes());

                    ResultSet rs = ResultSetMgr.read(byteArrayInputStream, ResultsFormat.convert(ResultsFormat.FMT_RS_JSON));

                    assertFalse(rs.hasNext(), "Found results for listCatalog, expected none");
                });
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Counting one catalogue")
    void testCountOneCatalog(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "count-one-test-catalogue";
        cataloguesService.putCatalogue(datasetID, exampleCatalogue, "text/turtle", null, ar -> {
            if (ar.succeeded()) {
                cataloguesService.listCatalogues("application/json", 20, 0, ar2 -> {
                    if (ar2.succeeded()) {
                        JsonObject result = ar2.result();
                        testContext.verify(() -> {
                            assertNotNull(result);
                            assertEquals("success", result.getString("status", ""));
                            assertNotNull(result.getString("content"));
                            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(result.getString("content").getBytes());

                            ResultSet rs = ResultSetMgr.read(byteArrayInputStream, ResultsFormat.convert(ResultsFormat.FMT_RS_JSON));

                            assertNotNull(rs.getResultVars());
                            assertFalse(rs.getResultVars().isEmpty());
                            final AtomicInteger count = new AtomicInteger();
                            rs.forEachRemaining(r -> count.incrementAndGet());
                            assertEquals(1, count.get());
                        });
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
    @DisplayName("Counting two catalogues")
    void testCountTwoCatalogs(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "delete-missing-test-catalogue";
        cataloguesService.putCatalogue(datasetID, exampleCatalogue, "text/turtle", null, ar -> {
            if (ar.succeeded()) {
                cataloguesService.putCatalogue(datasetID + "2", exampleCatalogue, "text/turtle", null, putHandler -> {
                    if (putHandler.succeeded()) {
                        cataloguesService.listCatalogues("application/json", 20, 0, ar2 -> {
                            if (ar2.succeeded()) {
                                JsonObject result = ar2.result();
                                testContext.verify(() -> {
                                    assertNotNull(result);
                                    assertEquals("success", result.getString("status", ""));
                                    assertNotNull(result.getString("content"));
                                    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(result.getString("content").getBytes());

                                    ResultSet rs = ResultSetMgr.read(byteArrayInputStream, ResultsFormat.convert(ResultsFormat.FMT_RS_JSON));

                                    ArrayList<String> resultList = new ArrayList<>();
                                    assertNotNull(rs.getResultVars());
                                    assertFalse(rs.getResultVars().isEmpty());

                                    String var = rs.getResultVars().get(0);
                                    rs.forEachRemaining(cat -> resultList.add(cat.get(var).toString()));
                                    assertEquals(2, resultList.size());
                                });
                                testContext.completeNow();
                            } else {
                                testContext.failNow(ar2.cause());
                            }
                        });
                    } else {
                        testContext.failNow(putHandler.cause());
                    }
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Counting one thousand catalogues")
    void testCountOneThousandCatalogs(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "count-hundred-test-catalogues";

        int numberToCount = 1000;

        ArrayList<Future> futureList = new ArrayList<>();

        for (int i = 0; i < numberToCount; i++) {
            Future<JsonObject> fut = Future.future();
            futureList.add(fut);
            cataloguesService.putCatalogue(datasetID + "-" + i, exampleCatalogue, "text/turtle", null, fut);
        }

        CompositeFuture.all(futureList).setHandler(ar -> {
            if (ar.succeeded()) {
                cataloguesService.listCatalogues("application/json", numberToCount, 0, ar2 -> {
                    if (ar2.succeeded()) {
                        JsonObject result = ar2.result();
                        testContext.verify(() -> {
                            assertNotNull(result);
                            assertEquals("success", result.getString("status", ""));
                            assertNotNull(result.getString("content"));
                            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(result.getString("content").getBytes());

                            ResultSet rs = ResultSetMgr.read(byteArrayInputStream, ResultsFormat.convert(ResultsFormat.FMT_RS_JSON));

                            ArrayList<String> resultList = new ArrayList<>();
                            assertNotNull(rs.getResultVars());
                            assertFalse(rs.getResultVars().isEmpty());

                            String var = rs.getResultVars().get(0);
                            rs.forEachRemaining(cat -> resultList.add(cat.get(var).toString()));
                            assertEquals(numberToCount, resultList.size());
                        });
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
    @DisplayName("Counting one thousand catalogues with limit")
    void testCountOneThousandCatalogsWithLimit(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "count-hundred-test-catalogues";

        int numberToCount = 1000;

        ArrayList<Future> futureList = new ArrayList<>();

        for (int i = 0; i < numberToCount; i++) {
            Future<JsonObject> fut = Future.future();
            futureList.add(fut);
            cataloguesService.putCatalogue(datasetID + "-" + i, exampleCatalogue, "text/turtle", null, fut);
        }

        CompositeFuture.all(futureList).setHandler(ar -> {
            if (ar.succeeded()) {
                cataloguesService.listCatalogues("application/json", 100, 0, ar2 -> {
                    if (ar2.succeeded()) {
                        JsonObject result = ar2.result();
                        testContext.verify(() -> {
                            assertNotNull(result);
                            assertEquals("success", result.getString("status", ""));
                            assertNotNull(result.getString("content"));
                            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(result.getString("content").getBytes());

                            ResultSet rs = ResultSetMgr.read(byteArrayInputStream, ResultsFormat.convert(ResultsFormat.FMT_RS_JSON));

                            ArrayList<String> resultList = new ArrayList<>();
                            assertNotNull(rs.getResultVars());
                            assertFalse(rs.getResultVars().isEmpty());

                            String var = rs.getResultVars().get(0);
                            rs.forEachRemaining(cat -> resultList.add(cat.get(var).toString()));
                            assertEquals(100, resultList.size());
                        });
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
    @DisplayName("Counting catalogues with limit and offset")
    void testCountCatalogsWithLimitAndOffset(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "count-hundred-test-catalogues";

        int numberToCount = 100;

        ArrayList<Future> futureList = new ArrayList<>();

        for (int i = 0; i < numberToCount; i++) {
            Future<JsonObject> fut = Future.future();
            futureList.add(fut);
            cataloguesService.putCatalogue(datasetID + "-" + i, exampleCatalogue, "text/turtle", null, fut);
        }

        CompositeFuture.all(futureList).setHandler(ar -> {
            if (ar.succeeded()) {
                cataloguesService.listCatalogues("application/json", 10, 10, ar2 -> {
                    if (ar2.succeeded()) {
                        JsonObject result = ar2.result();
                        testContext.verify(() -> {
                            assertNotNull(result);
                            assertEquals("success", result.getString("status", ""));
                            assertNotNull(result.getString("content"));
                            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(result.getString("content").getBytes());

                            ResultSet rs = ResultSetMgr.read(byteArrayInputStream, ResultsFormat.convert(ResultsFormat.FMT_RS_JSON));

                            ArrayList<String> resultList = new ArrayList<>();
                            assertNotNull(rs.getResultVars());
                            assertFalse(rs.getResultVars().isEmpty());

                            String var = rs.getResultVars().get(0);
                            rs.forEachRemaining(cat -> {
                                LOGGER.info(cat.get(var).toString());
                                assertNotNull(DCATAPUriSchema.parseUriRef(cat.get(var).toString()).getId());
                                resultList.add(cat.get(var).toString());
                            });
                            assertEquals(10, resultList.size());
                        });
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

}
