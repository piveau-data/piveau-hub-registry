package io.piveau.hub;

import io.piveau.hub.services.datasets.DatasetsService;
import io.piveau.hub.services.datasets.DatasetsServiceVerticle;
import io.piveau.hub.util.Constants;
import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.util.MockTripleStore;
import io.piveau.utils.JenaUtils;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.http.HttpHeaders;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Testing the datasets service")
@ExtendWith(VertxExtension.class)
class DatasetImplServiceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetImplServiceTest.class);
    private DatasetsService datasetsService;

    private final String catalogueID = "test-catalog";

    private static String exampleDataset;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        DeploymentOptions options = new DeploymentOptions()
                .setConfig(new JsonObject()
                        .put(Constants.ENV_PIVEAU_HUB_TRIPLESTORE_CONFIG, MockTripleStore.getTriplestoreConfig(true))
                        .put(Constants.ENV_PIVEAU_HUB_VALIDATOR, new JsonObject().put("url", "localhost").put("enabled", false))
                        .put(Constants.ENV_PIVEAU_TRANSLATION_SERVICE, new JsonObject().put("enable", false)));

        Checkpoint checkpoint = testContext.checkpoint(3);

        vertx.fileSystem().readFile("misc/example_dataset.ttl", readResult -> {
            if (readResult.succeeded()) {
                exampleDataset = readResult.result().toString();
                checkpoint.flag();
            } else {
                testContext.failNow(readResult.cause());
            }
        });

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
    }

    @Test
    @DisplayName("Create an example dataset")
    void testCreateExampleDataset(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "create-test-dataset";

        datasetsService.putDataset(datasetID, exampleDataset, "text/turtle", catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                testContext.verify(() -> {
                    assertEquals(result.getString("status", ""), "created");
                    String location = result.getString(HttpHeaders.LOCATION);
                    assertNotNull(location);
                    assertEquals(DCATAPUriSchema.applyFor(datasetID).getDatasetUriRef(), location);
                });
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Delete an example dataset")
    void testDeleteExampleDataset(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "delete-test-dataset";

        datasetsService.putDataset(datasetID, exampleDataset, "text/turtle", catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                datasetsService.deleteDataset(datasetID, catalogueID, handler -> {
                    if (handler.succeeded()) {
                        datasetsService.getDataset(datasetID, catalogueID, "text/turtle", ar2 -> {
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
    @DisplayName("Receive an example dataset")
    void testGetExampleDataset(Vertx vertx, VertxTestContext testContext) {

        datasetsService.putDataset("get-test-dataset", exampleDataset, "text/turtle", catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                datasetsService.getDataset("get-test-dataset", catalogueID, "text/turtle", ar2 -> {
                    if (ar2.succeeded()) {
                        JsonObject result = ar2.result();
                        testContext.verify(() -> {
                            assertEquals("success", result.getString("status", ""));
                            LOGGER.info(result.encodePrettily());
                            String content = result.getString("content");
                            assertNotNull(content);
                            DatasetHelper.create(content, "text/turtle", modelResult -> {
                                assertTrue(modelResult.succeeded());
                                assertEquals("get-test-dataset", modelResult.result().id());
                            });
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

//    @Test
    @DisplayName("Receive an example dataset from normalized id")
    void testGetExampleDatasetNormalizedID(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "test-Get-normalized-Dataset .id";

        datasetsService.putDataset(datasetID, exampleDataset, "text/turtle", catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                datasetsService.getDatasetByNormalizedId(DCATAPUriSchema.applyFor(datasetID).getId(), "text/turtle", ar2 -> {
                    if (ar2.succeeded()) {
                        JsonObject result = ar2.result();
                        testContext.verify(() -> {
                            assertEquals("success", result.getString("status", ""));
                            LOGGER.info(result.encodePrettily());
                            String model = result.getString("content");
                            assertNotNull(model);
                            Model jenaModel = JenaUtils.read(model.getBytes(), "text/turtle");
                            assertNotNull(jenaModel);

                            Resource cat = jenaModel.getResource(DCATAPUriSchema.applyFor(datasetID).getDatasetUriRef());
                            assertNotNull(cat);
                            assertNotNull(cat.getProperty(RDF.type));
                            assertEquals(cat.getProperty(RDF.type).getObject(), DCAT.Dataset);
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
    @DisplayName("Delete a non existing dataset")
    void testDeleteMissingDataset(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "delete-missing-test-dataset";
        datasetsService.deleteDataset(datasetID, catalogueID, ar -> {
            testContext.verify(() -> {
                assertFalse(ar.succeeded());
            });
            testContext.completeNow();
        });
    }

    /*

        @Test
        public void testCountZeroCatalogs(TestContext tc) {

            DCATAPUriSchema.config(new JsonObject().put("baseUri", "https://example.com/"));

            Async async = tc.async();

            datasetsService.listDatasets("application/json",null,20,0,false, ar->{
                if (ar.succeeded()) {
                    JsonObject result = ar.result();
                    tc.assertNotNull(result);
                    tc.assertEquals("success", result.getString("status", ""));
                    tc.assertNotNull(result.getString("content"));
                    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( result.getString("content").getBytes());
                    PiveauLoggerFactory.getLogger(getClass()).info(result.getString("content"));
                    ResultSet rs = ResultSetMgr.read(byteArrayInputStream, ResultsFormat.convert(ResultsFormat.FMT_RS_JSON));

                   tc.assertFalse(rs.hasNext(), "Found results for listCatalog, expected none");

                    async.complete();

                } else {
                    tc.fail(ar.cause());
                    async.complete();

                }
            });

        }
    */

    @Test
    @DisplayName("Counting one dataset")
    void testCountOneDataset(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "count-one-test-dataset";

        datasetsService.putDataset(datasetID, exampleDataset, "text/turtle", catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                datasetsService.listDatasets("application/json", catalogueID, 20, 0, true, ar2 -> {
                    if (ar2.succeeded()) {
                        JsonObject result = ar2.result();
                        testContext.verify(() -> {
                            assertNotNull(result);
                            assertEquals("success", result.getString("status", ""));
                            assertNotNull(result.getJsonArray("content"));
                            assertFalse(result.getJsonArray("content").isEmpty());
                            assertEquals(1, result.getJsonArray("content").size());
                            assertEquals(datasetID, result.getJsonArray("content").getString(0));
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
    @DisplayName("Counting two datasets")
    void testCountTwoDatasetsInOneCat(Vertx vertx, VertxTestContext testContext) {
        String datasetID = "count-two-test-dataset";

        datasetsService.putDataset(datasetID, exampleDataset, "text/turtle", catalogueID, null, false, ar -> {
            if (ar.succeeded()) {
                datasetsService.putDataset(datasetID + "2", exampleDataset, "text/turtle", catalogueID, null, false, putHandler -> {
                    if (putHandler.succeeded()) {
                        datasetsService.listDatasets("application/json", catalogueID, 20, 0, true, ar2 -> {
                            if (ar2.succeeded()) {
                                JsonObject result = ar2.result();
                                testContext.verify(() -> {
                                    assertNotNull(result);
                                    assertEquals("success", result.getString("status", ""));
                                    assertNotNull(result.getJsonArray("content"));
                                    assertFalse(result.getJsonArray("content").isEmpty());
                                    assertEquals(2, result.getJsonArray("content").size());
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
/*

  @Test
    public void testCountTwoDatasetsInTwoCat(TestContext tc) {
        String datasetID = "count-test-dataset";
        DCATAPUriSchema.config(new JsonObject().put("baseUri", baseURI));
        FileSystem fileSystem = vertx.fileSystem();
        Buffer cbuffer = fileSystem.readFileBlocking("doc/example_catalog.ttl");
      Buffer buffer = fileSystem.readFileBlocking("doc/example_dataset.ttl");


        Async async = tc.async();
        cataloguesService.putCatalogue(catalogueID+"2", cbuffer.toString(), "text/turtle", null, cr -> {
            if (cr.succeeded()) {
                datasetsService.putDataset(datasetID, buffer.toString(), "text/turtle",catalogueID, null,false, ar -> {
                    if (ar.succeeded()) {
                        datasetsService.putDataset(datasetID, buffer.toString(), "text/turtle",catalogueID+"2", null,false, putHandler -> {
                            if (putHandler.succeeded()) {


                                datasetsService.listDatasets("application/json",null,20,0,true, ar2->{
                                    if (ar2.succeeded()) {


                                        JsonObject result = ar2.result();
                                        tc.assertNotNull(result);
                                        tc.assertEquals("success", result.getString("status", ""));
                                        tc.assertNotNull(result.getJsonArray("content"));
                                        tc.assertFalse(result.getJsonArray("content").isEmpty());
                                        tc.assertEquals(2,result.getJsonArray("content").size());
                                        async.complete();

                                    } else {
                                        tc.fail(ar2.cause());
                                        async.complete();
                                    }
                                });

                            } else {
                                tc.fail(putHandler.cause());
                                async.complete();
                            }

                        });
                    } else {
                        tc.fail(ar.cause());
                        async.complete();
                    }
                });
            } else {
                tc.fail(cr.cause());
                async.complete();
            }
        });
    }

/*
    @Test
    public void testCountOnethousandCatalogs(TestContext tc) {
        String datasetID = "count-hundred-test-catalogues";
        DCATAPUriSchema.config(new JsonObject().put("baseUri", "https://example.com/"));
        FileSystem fileSystem = vertx.fileSystem();
        Buffer buffer = fileSystem.readFileBlocking("doc/example_catalog.ttl");


        Async async = tc.async();
        int numberToCount = 1000;

        ArrayList<Future> futureList = new ArrayList<>();

        for (int i = 0; i < numberToCount; i++) {
            Future<JsonObject> fut = Future.future();
            futureList.add(fut);
            cataloguesService.putCatalogue(datasetID+"-"+i, buffer.toString(), "text/turtle", null, fut);
        }


        CompositeFuture.all(futureList).setHandler(ar -> {
            if (ar.succeeded()) {
                cataloguesService.listCatalogues("application/json",numberToCount,0, ar2->{
                    if (ar2.succeeded()) {

                        JsonObject result = ar2.result();
                        tc.assertNotNull(result);
                        tc.assertEquals("success", result.getString("status", ""));
                        tc.assertNotNull(result.getString("content"));
                        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( result.getString("content").getBytes());

                        ResultSet rs = ResultSetMgr.read(byteArrayInputStream, ResultsFormat.convert(ResultsFormat.FMT_RS_JSON));

                        ArrayList<String> resultList = new ArrayList<>();
                        tc.assertNotNull(rs.getResultVars());
                        tc.assertFalse(rs.getResultVars().isEmpty());

                        String var = rs.getResultVars().get(0);
                        rs.forEachRemaining(cat-> {
                            resultList.add(cat.get(var).toString());
                        });
                        tc.assertEquals(numberToCount,resultList.size());


                        async.complete();

                    } else {
                        tc.fail(ar2.cause());
                        async.complete();

                    }
                });
            } else {

                tc.fail(ar.cause());
                async.complete();
            }
        });



    }


    @Test
    public void testCountOnethousandCatalogswithLimit(TestContext tc) {
        String datasetID = "count-hundred-test-catalogues";
        DCATAPUriSchema.config(new JsonObject().put("baseUri", "https://example.com/"));
        FileSystem fileSystem = vertx.fileSystem();
        Buffer buffer = fileSystem.readFileBlocking("doc/example_catalog.ttl");


        Async async = tc.async();
        int numberToCount = 1000;

        ArrayList<Future> futureList = new ArrayList<>();

        for (int i = 0; i < numberToCount; i++) {
            Future<JsonObject> fut = Future.future();
            futureList.add(fut);
            cataloguesService.putCatalogue(datasetID+"-"+i, buffer.toString(), "text/turtle", null, fut);
        }


        CompositeFuture.all(futureList).setHandler(ar -> {
            if (ar.succeeded()) {
                cataloguesService.listCatalogues("application/json",100,0, ar2->{
                    if (ar2.succeeded()) {

                        JsonObject result = ar2.result();
                        tc.assertNotNull(result);
                        tc.assertEquals("success", result.getString("status", ""));
                        tc.assertNotNull(result.getString("content"));
                        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( result.getString("content").getBytes());

                        ResultSet rs = ResultSetMgr.read(byteArrayInputStream, ResultsFormat.convert(ResultsFormat.FMT_RS_JSON));

                        ArrayList<String> resultList = new ArrayList<>();
                        tc.assertNotNull(rs.getResultVars());
                        tc.assertFalse(rs.getResultVars().isEmpty());

                        String var = rs.getResultVars().get(0);
                        rs.forEachRemaining(cat-> {
                            resultList.add(cat.get(var).toString());
                        });
                        tc.assertEquals(100,resultList.size());


                        async.complete();

                    } else {
                        tc.fail(ar2.cause());
                        async.complete();

                    }
                });
            } else {

                tc.fail(ar.cause());
                async.complete();
            }
        });
    }
    @Test
    public void testCountCatalogswithLimitAndOffset(TestContext tc) {
        String datasetID = "count-hundred-test-catalogues";
        DCATAPUriSchema.config(new JsonObject().put("baseUri", "https://example.com/"));
        FileSystem fileSystem = vertx.fileSystem();
        Buffer buffer = fileSystem.readFileBlocking("doc/example_catalog.ttl");


        Async async = tc.async();
        int numberToCount = 100;

        ArrayList<Future> futureList = new ArrayList<>();

        for (int i = 0; i < numberToCount; i++) {
            Future<JsonObject> fut = Future.future();
            futureList.add(fut);
            cataloguesService.putCatalogue(datasetID+"-"+i, buffer.toString(), "text/turtle", null, fut);
        }


        CompositeFuture.all(futureList).setHandler(ar -> {
            if (ar.succeeded()) {
                cataloguesService.listCatalogues("application/json",10,10, ar2->{
                    if (ar2.succeeded()) {

                        JsonObject result = ar2.result();
                        tc.assertNotNull(result);
                        tc.assertEquals("success", result.getString("status", ""));
                        tc.assertNotNull(result.getString("content"));
                        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( result.getString("content").getBytes());

                        ResultSet rs = ResultSetMgr.read(byteArrayInputStream, ResultsFormat.convert(ResultsFormat.FMT_RS_JSON));

                        ArrayList<String> resultList = new ArrayList<>();
                        tc.assertNotNull(rs.getResultVars());
                        tc.assertFalse(rs.getResultVars().isEmpty());

                        String var = rs.getResultVars().get(0);
                        rs.forEachRemaining(cat-> {
                            LOGGER.info(cat.get(var).toString());
                            tc.assertNotNull(DCATAPUriSchema.parseUriRef(cat.get(var).toString()).id());
                            resultList.add(cat.get(var).toString());
                        });
                        tc.assertEquals(10,resultList.size());


                        async.complete();

                    } else {
                        tc.fail(ar2.cause());
                        async.complete();

                    }
                });
            } else {

                tc.fail(ar.cause());
                async.complete();
            }
        });
    }*/

    //TODO: create Dataset & check if distribution is renamed
    //TODO: add update dataset with a dataset that has an additional Dist (without dct:idenifier) & dist is correctly renamed
    //TODO: add update dataset with a dataset that has an additional Dist (with dct:idenifier)  & dist is correctly renamed
    //TODO: list from empty catalogue, list from nonexisting catalog, (list sources, list datasets )x(with&, without catalog)

}
