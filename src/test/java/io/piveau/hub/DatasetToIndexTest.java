package io.piveau.hub;

import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.indexing.Indexing;
import io.piveau.rdf.RDFMimeTypes;
import io.piveau.utils.JenaUtils;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

@DisplayName("Testing indexing")
@ExtendWith(VertxExtension.class)
class DatasetToIndexTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetToIndexTest.class);

//    @BeforeClass
//    public static void globalSetup(){
//        rdfUtil = new RDFUtil("http://test.de");
//        vertx = Vertx.vertx();
//
//
//        VocabularyManager.init(new JsonObject().put(Constants.ENV_PIVEAU_HUB_LOAD_VOCABULARIES, true), vertx);
//        vocabularyManager = VocabularyManager.getInstance();
//        Future<Void> f2 = vocabularyManager.loadVocabularies();
//        // Super hacky, but working
//        while(!f2.isComplete()){}
//
//
//        DatasetToIndexConverter.init(rdfUtil, new JsonObject().put(Constants.ENV_PIVEAU_HUB_LOAD_VOCABULARIES, false));
//        datasetToIndexConverter = DatasetToIndexConverter.getInstance();
//        Future<Void> f = datasetToIndexConverter.loadVocabulary(vertx);
//
//        LOGGER.info("Global setup finished");
//    }

    @Test
    @DisplayName("Indexing an example dataset")
    @Timeout(timeUnit = TimeUnit.MINUTES, value = 5)
    void testExampleDataset(Vertx vertx, VertxTestContext testContext) {
        vertx.fileSystem().readFile("misc/example_index_dataset.ttl", f -> {
            if (f.succeeded()) {
                DatasetHelper.create(f.result().toString(), RDFMimeTypes.TURTLE, ar -> {
                    if (ar.succeeded()) {
                        DatasetHelper helper = ar.result();
                        JsonObject result = Indexing.indexingDataset(helper.resource(), "test-catalog", "en");
                        LOGGER.debug(result.encodePrettily());
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
            } else {
                testContext.failNow(f.cause());
            }
        });
    }

    //    @Test
//    @DisplayName("Indexing an example dataset (alternative code)")
//    @Timeout(timeUnit = TimeUnit.MINUTES, value = 3)
    void testExampleDatasetAlt(Vertx vertx, VertxTestContext testContext) {
        Buffer buffer = vertx.fileSystem().readFileBlocking("misc/example_dataset.ttl");
        DatasetHelper.create(buffer.toString(), RDFMimeTypes.TURTLE, ar -> {
            if (ar.succeeded()) {
                JsonObject result = Indexing.indexingDataset(ar.result().resource(), ar.result().catalogueId(), ar.result().sourceLang());
                LOGGER.info(result.encodePrettily());
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    @DisplayName("Indexing an example catalogue")
    void testExampleCatalog(Vertx vertx, VertxTestContext testContext) {
        vertx.fileSystem().readFile("misc/example_catalog.ttl", f -> {
            if (f.succeeded()) {
                Model model = JenaUtils.read(f.result().getBytes(), "text/turtle");
                Resource catalogue = model.listSubjectsWithProperty(RDF.type, DCAT.Catalog).next();
                JsonObject result = Indexing.indexingCatalogue(catalogue);
                LOGGER.debug(result.encodePrettily());
                testContext.completeNow();
            } else {
                testContext.failNow(f.cause());
            }
        });
    }

}
