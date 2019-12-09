package io.piveau.hub.services.datasets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.services.translation.TranslationService;
import io.piveau.hub.services.validation.ValidationServiceVerticle;
import io.piveau.hub.util.*;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.hub.util.rdf.SPDX;
import io.piveau.indexing.Indexing;
import io.piveau.pipe.PipeLauncher;
import io.piveau.rdf.RDFMimeTypes;
import io.piveau.utils.ConfigHelper;
import io.piveau.utils.experimental.DCATAPUriRef;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.http.HttpHeaders;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.Lang;
import org.apache.jena.util.ResourceUtils;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;

import java.io.ByteArrayInputStream;
import java.util.concurrent.atomic.AtomicInteger;

public class DatasetsServiceImpl implements DatasetsService {

    private TSConnector connector;
    private DataUploadConnector dataUploadConnector;
    private Vertx vertx;
    private IndexService indexService;
    private TranslationService translationService;

    private PipeLauncher launcher;

    private JsonObject validationConfig;
    private JsonObject translationConfig;

    DatasetsServiceImpl(TSConnector connector, DataUploadConnector dataUploadConnector, JsonObject config, PipeLauncher launcher, Vertx vertx, Handler<AsyncResult<DatasetsService>> readyHandler) {
        this.vertx = vertx;
        this.launcher = launcher;
        this.connector = connector;
        this.dataUploadConnector = dataUploadConnector;
        this.indexService = IndexService.createProxy(vertx, IndexService.SERVICE_ADDRESS);
        this.translationService = TranslationService.createProxy(vertx, TranslationService.SERVICE_ADDRESS);
        validationConfig = ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_HUB_VALIDATOR);
        translationConfig = ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_TRANSLATION_SERVICE);
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public DatasetsService listDatasets(String consumes, String catalogueId, Integer limit, Integer offset, Boolean sourceIds, Handler<AsyncResult<JsonObject>> handler) {
        String catalogueUriRef = catalogueId != null && !catalogueId.isEmpty() ? DCATAPUriSchema.applyFor(catalogueId).getCatalogueUriRef() : null;

        if (sourceIds) {
            connector.listDatasetSources(catalogueUriRef, ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture(new JsonObject()
                            .put("contentType", "application/json")
                            .put("status", "success")
                            .put("content", ar.result())
                    ));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        } else {
            connector.listDatasets(consumes, catalogueUriRef, limit, offset, ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture(new JsonObject()
                            .put("contentType", consumes != null ? consumes : "application/json")
                            .put("status", "success")
                            .put("content", ar.result())
                    ));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        }

        return this;
    }

    @Override
    public DatasetsService getDataset(String datasetId, String catalogueId, String consumes, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(datasetId, catalogueId, getClass());
        Promise<JsonObject> uriPromise = Promise.promise();
        connector.getDatasetUriRefs(datasetId, catalogueId, uriPromise);
        uriPromise.future().compose(uriRefs -> {
            Promise<String> graphPromise = Promise.promise();
            String datasetUriRef = uriRefs.getString("datasetUriRef");
            connector.getGraph(DCATAPUriSchema.parseUriRef(datasetUriRef).getDatasetGraphName(), consumes, graphPromise);
            return graphPromise.future();
        }).setHandler(ar -> {
            if (ar.succeeded()) {

                // we could remove record here

                handler.handle(Future.succeededFuture(new JsonObject()
                        .put("status", "success")
                        .put("content", ar.result())
                ));
            } else {

                try {

                    ReplyException s = (io.vertx.core.eventbus.ReplyException) ar.cause();
                    if (s.failureCode() == 404) {
                        log.error("no dataset: {}", s.failureType());
                        handler.handle(Future.succeededFuture(new JsonObject()
                                .put("status", "not found")
                        ));
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }


                } catch (ClassCastException cce) {
                    log.error("casting is a no:", cce);
                    handler.handle(Future.failedFuture(ar.cause()));
                } catch (Exception e) {
                    log.error("this does not work:", e);
                    handler.handle(Future.failedFuture(ar.cause()));
                }

            }
        });
        return this;
    }


    @Override
    public DatasetsService getDatasetByNormalizedId(String datasetSuffix, String consumes, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getDatasetLogger(datasetSuffix, getClass());
        Future<String> graphFuture = Future.future();
        DCATAPUriRef dcapatSchema = DCATAPUriSchema.applyFor(datasetSuffix);
        connector.getGraph(dcapatSchema.getDatasetGraphName(), consumes, graphFuture);

        graphFuture.setHandler(ar -> {
            if (ar.succeeded()) {

                // we could remove record here

                handler.handle(Future.succeededFuture(new JsonObject()
                        .put("status", "success")
                        .put("content", ar.result())
                ));
            } else {
                try {
                    ReplyException s = (io.vertx.core.eventbus.ReplyException) ar.cause();
                    if (s.failureCode() == 404) {
                        log.error("no dataset: {}", s.failureType());
                        handler.handle(Future.succeededFuture(new JsonObject()
                                .put("status", "not found")
                        ));
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }


                } catch (ClassCastException cce) {
                    log.error("casting is a no:", cce);
                    handler.handle(Future.failedFuture(ar.cause()));
                } catch (Exception e) {
                    log.error("this does not work:", e);
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            }
        });
        return this;
    }


    @Override
    public DatasetsService getRecord(String datasetId, String catalogueId, String consumes, Handler<AsyncResult<JsonObject>> handler) {
        connector.getDatasetUriRefs(datasetId, catalogueId, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                String datasetUriRef = result.getString("datasetUriRef");
                String recordUriRef = result.getString("recordUriRef");
                DCATAPUriRef uriSchema = DCATAPUriSchema.parseUriRef(datasetUriRef);
                connector.getRecord(uriSchema.getDatasetGraphName(), recordUriRef, consumes, rr -> {
                    if (rr.succeeded()) {
                        handler.handle(Future.succeededFuture(new JsonObject()
                                .put("status", "success")
                                .put("content", rr.result())
                        ));
                    } else {
                        handler.handle(Future.failedFuture(rr.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public DatasetsService putDataset(String datasetId, String dataset, String contentType, String catalogueId, String hash, Boolean createAccessURLs, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(datasetId, catalogueId, getClass());
        DatasetHelper.create(datasetId, dataset, contentType, hash, catalogueId, dr -> {
            if (dr.succeeded()) {
                DatasetHelper datasetHelper = dr.result();
                Promise<JsonObject> existsPromise = Promise.promise();
                connector.catalogueExists(datasetHelper.catalogueUriRef(), existsPromise);
                existsPromise.future().compose(cat -> {
                    Promise<JsonObject> hashPromise = Promise.promise();
                    datasetHelper.sourceType(cat.getString("type"));
                    datasetHelper.sourceLang(cat.getString("lang"));
                    getHash(datasetHelper, hashPromise);
                    return hashPromise.future();
                }).compose(hr -> {
                    Promise<Void> createOrUpdatePromise = Promise.promise();
                    if (hr.getBoolean("success") && hr.getString("hash").equals(datasetHelper.hash())) {
                        log.debug("hash equal, skipping");
                        // self repair...
                        catalogue(datasetHelper);
                        createOrUpdatePromise.fail("skipped");
                    } else {
                        if (hr.getBoolean("success")) {
                            log.debug("update");
                            String recordUriRef = hr.getString("recordUriRef");
                            connector.getGraph(DCATAPUriSchema.parseUriRef(recordUriRef).getDatasetGraphName(), ar -> {
                                if (ar.succeeded()) {
                                    datasetHelper.update(ar.result(), recordUriRef);
                                    createOrUpdatePromise.complete();
                                } else {
                                    createOrUpdatePromise.fail(ar.cause());
                                }
                            });
                        } else {
                            log.debug("create");
                            connector.findFreeNormalized(datasetHelper, new AtomicInteger(0), ar -> {
                                if (ar.succeeded()) {
                                    datasetHelper.init(ar.result());
                                    if (createAccessURLs) {
                                        datasetHelper.setAccessURLs(dataUploadConnector);
                                    }
                                    createOrUpdatePromise.complete();
                                } else {
                                    createOrUpdatePromise.fail(ar.cause());
                                }
                            });
                        }
                    }
                    return createOrUpdatePromise.future();
                }).compose(v -> {
                    Promise<DatasetHelper> translationPromise = Promise.promise();
                    if (translationConfig.getBoolean("enable")) {
                        translate(datasetHelper).setHandler(ar -> {
                            DatasetHelper finalHelper = ar.succeeded() ? ar.result() : datasetHelper;
                            translationPromise.complete(finalHelper);
                        });
                    } else {
                        translationPromise.complete(datasetHelper);
                    }
                    return translationPromise.future();
                }).setHandler(dh -> {
                    if (dh.succeeded()) {
                        DatasetHelper finalHelper = dh.result();
                        index(finalHelper);
                        if (validationConfig.getBoolean("enabled")) {
                            validate(finalHelper);
                        }
                        if (!finalHelper.model().containsResource(ModelFactory.createDefaultModel().createResource(finalHelper.uriRef()))) {
                            finalHelper.model().listSubjectsWithProperty(RDF.type, DCAT.Dataset).forEachRemaining(ds -> ResourceUtils.renameResource(ds, finalHelper.uriRef()));
                            finalHelper.model().listSubjectsWithProperty(RDF.type, DCAT.CatalogRecord).forEachRemaining(ds -> ResourceUtils.renameResource(ds, finalHelper.recordUriRef()));
                        }
                        store(finalHelper).setHandler(handler);
                        catalogue(finalHelper);
                    } else {
                        handler.handle(Future.failedFuture(dh.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(dr.cause()));
            }
        });
        return this;
    }

    @Override
    @Deprecated
    public DatasetsService postDataset(String dataset, String contentType, Handler<AsyncResult<JsonObject>> handler) {
        handler.handle(Future.failedFuture("Not yet implemented"));
        return this;
    }

    @Override
    public DatasetsService deleteDataset(String datasetId, String catalogueId, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(datasetId, catalogueId, getClass());
        connector.getDatasetUriRefs(datasetId, catalogueId, ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                String datasetUriRef = result.getString("datasetUriRef");
                connector.deleteGraph(datasetUriRef, dr -> {
                    if (dr.failed()) {
                        handler.handle(Future.failedFuture(ar.cause()));
                    } else {
                        indexService.deleteDataset(DCATAPUriSchema.parseUriRef(datasetUriRef).getId(), ir -> {
                            if (ir.failed()) {
                                log.error("Remove index", ir.cause());
                            }
                        });
                        connector.removeDatasetFromCatalogue(datasetUriRef, DCATAPUriSchema.parseUriRef(datasetUriRef).getRecordUriRef(), res -> {
                            if (res.failed()) {
                                log.error("Remove catalogue entries", ar.cause());
                            }
                        });
                        handler.handle(Future.succeededFuture(new JsonObject().put("status", "deleted")));
                    }
                });
                if (result.containsKey("validationUriRef")) {
                    String validationUriRef = result.getString("validationUriRef");
                    connector.deleteGraph(validationUriRef, vr -> {
                        if (vr.failed()) {
                            log.warn("Delete validation graph", vr.cause());
                        }
                    });
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public DatasetsService indexDataset(String datasetId, String catalogueId, String defaultLang, Handler<AsyncResult<JsonObject>> handler) {
        String contentType = "application/n-triples";
        getDataset(datasetId, catalogueId, contentType, ar -> {
            if (ar.succeeded()) {
                DatasetHelper.create(datasetId, ar.result().getString("content"), contentType, null, catalogueId, dr -> {
                    if (dr.succeeded()) {
                        DatasetHelper helper = dr.result();
                        JsonObject indexObject = Indexing.indexingDataset(helper.resource(), catalogueId, defaultLang);
                        indexService.addDatasetPut(indexObject, ir -> {
                            if (ir.failed()) {
                                handler.handle(Future.failedFuture(ir.cause()));
                            } else {
                                handler.handle(Future.succeededFuture());
                            }
                        });
                    } else {
                        handler.handle(Future.failedFuture(dr.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public DatasetsService getDataUploadInformation(String datasetId, String catalogueId, String resultDataset, Handler<AsyncResult<JsonObject>> handler) {
        DatasetHelper.create(resultDataset, Lang.NTRIPLES.getHeaderString(), dr -> {
            if (dr.succeeded()) {
                DatasetHelper helper = dr.result();
                JsonArray uploadResponse = dataUploadConnector.getResponse(helper);
                JsonObject result = new JsonObject();
                result.put("status", "success");
                result.put("distributions", uploadResponse);
                handler.handle(Future.succeededFuture(result));
            } else {
                handler.handle(Future.failedFuture(dr.cause()));
            }
        });
        return this;
    }

    private Future<DatasetHelper> translate(DatasetHelper helper) {
        Promise<DatasetHelper> promise = Promise.promise();
        translationService.initializeTranslationProcess(helper, promise);
        return promise.future();
    }

    private void getHash(DatasetHelper helper, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(helper, getClass());
        String query = "SELECT ?hash ?record WHERE {<" + helper.catalogueUriRef() + "> <" + DCAT.record + "> ?record. ?record <" + DCTerms.identifier + "> \"" + helper.id() + "\"; <" + SPDX.checksum + ">/<" + SPDX.checksumValue + "> ?hash . }";
        connector.query(query, "application/sparql-results+json", ar -> {
            if (ar.succeeded()) {
                ResultSet set = ResultSetFactory.fromJSON(new ByteArrayInputStream(ar.result().body().getBytes()));
                if (set.hasNext()) {
                    QuerySolution solution = set.next();
                    RDFNode hash = solution.get("hash");
                    if (hash != null && hash.isLiteral()) {
                        log.debug("Hash available");
                        handler.handle(Future.succeededFuture(new JsonObject()
                                .put("success", true)
                                .put("hash", hash.asLiteral().toString())
                                .put("recordUriRef", solution.getResource("record").toString())));
                    } else {
                        log.debug("No old hash available");
                        handler.handle(Future.succeededFuture(new JsonObject().put("success", true)));
                    }
                } else {
                    log.debug("No old hash available");
                    handler.handle(Future.succeededFuture(new JsonObject().put("success", false)));
                }
            } else {
                log.error("Hash selection failed");
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private Future<JsonObject> store(DatasetHelper helper) {
        Promise<JsonObject> promise = Promise.promise();
        PiveauLogger log = PiveauLoggerFactory.getLogger(helper, getClass());
        log.trace("Store dataset");
        connector.putGraph(helper.graphName(), helper.model(), ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                if (response.statusCode() == 200) {
                    promise.complete(new JsonObject().put("status", "updated").put(HttpHeaders.LOCATION, helper.uriRef()));
                } else if (response.statusCode() == 201) {
                    promise.complete(new JsonObject()
                            .put("status", "created")
                            .put("dataset", helper.stringify(Lang.NTRIPLES))
                            .put(HttpHeaders.LOCATION, helper.uriRef()));
                } else {
                    log.error("Store dataset: {}", response.statusMessage());
                    promise.fail(response.statusMessage());
                }
            } else {
                log.error("Store dataset", ar.cause());
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    private void catalogue(DatasetHelper helper) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(helper, getClass());
        connector.addDatasetToCatalogue(helper.uriRef(), helper.recordUriRef(), helper.catalogueUriRef(), res -> {
            if (res.succeeded()) {
                HttpResponse<Buffer> response = res.result();
                if (response.statusCode() == 200) {
                    log.debug("Catalogue entries created in {}", helper.catalogueUriRef());
                }
            } else {
                log.error("Adding catalogue entries to " + helper.catalogueUriRef() + ":", res.cause());
            }
        });
    }

    private void index(DatasetHelper helper) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(helper, getClass());
        JsonObject indexMessage = Indexing.indexingDataset(helper.resource(), helper.catalogueId(), helper.sourceLang());
        indexService.addDatasetPut(indexMessage, ar -> {
            if (ar.failed()) {
                log.error("Indexing", ar.cause());
            }
        });
    }

    private void validate(DatasetHelper helper) {
        if (launcher.isPipeAvailable(validationConfig.getString("validationPipeName", ""))) {
            ObjectNode dataInfo = new ObjectMapper().createObjectNode().put("identifier", helper.id()).put("catalogue", helper.catalogueId());
            JsonObject configs = new JsonObject().put("validating-shacl", new JsonObject().put("skip", !"dcat-ap".equals(helper.sourceType())));
            launcher.runPipeWithData(validationConfig.getString("validationPipeName", ""), helper.stringify(Lang.TURTLE), RDFMimeTypes.TURTLE, dataInfo, configs);
        } else if (helper.sourceType().equals("dcat-ap")) {
            vertx.eventBus().send(ValidationServiceVerticle.ADDRESS, helper);
        }
    }

}
