package io.piveau.hub.services.distributions;

//import io.piveau.hub.converters.DatasetToIndexConverter;
import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.services.translation.TranslationService;
import io.piveau.hub.services.validation.ValidationServiceVerticle;
import io.piveau.hub.util.Constants;
import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.util.TSConnector;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.hub.util.rdf.SPDX;
import io.piveau.indexing.Indexing;
import io.piveau.utils.ConfigHelper;
import io.piveau.utils.JenaUtils;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpHeaders;
import org.apache.jena.arq.querybuilder.ConstructBuilder;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class DistributionsServiceImpl implements DistributionsService {


    private TSConnector connector;
    private JsonObject config;
    private Vertx vertx;
    private IndexService indexService;
    private TranslationService translationService;

    DistributionsServiceImpl(TSConnector connector, JsonObject config, Vertx vertx, Handler<AsyncResult<DistributionsService>> readyHandler) {
        this.vertx = vertx;
        this.connector = connector;
        this.config = config;

        this.indexService = IndexService.createProxy(vertx, IndexService.SERVICE_ADDRESS);
        this.translationService = TranslationService.createProxy(vertx, TranslationService.SERVICE_ADDRESS);
        readyHandler.handle(Future.succeededFuture(this));
    }


    @Override
    public DistributionsService getDistribution(String id, String consumes, Handler<AsyncResult<JsonObject>> handler) {

        String graphName = DCATAPUriSchema.applyFor(id).getDistributionUriRef();

        connector.getDistribution(graphName, consumes, ar -> {
            if (ar.succeeded()) {

                Model dist = JenaUtils.read(ar.result().getBytes(), consumes);

                if (!dist.isEmpty()) {
                    handler.handle(Future.succeededFuture(new JsonObject()
                            .put("status", "success")
                            .put("content", ar.result())

                    ));
                } else {
                    handler.handle(Future.succeededFuture(new JsonObject()
                            .put("status", "not found")
                            .put("content", "Distribution with id " + id + " not found")
                    ));
                }


            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }


    @Override
    public DistributionsService getDistributionByIdentifier(String identifier, String consumes, Handler<AsyncResult<JsonObject>> handler) {

        JsonObject responseObject = new JsonObject();

        Future<JsonObject> distributionUriFuture = Future.future();
        connector.getDistributionUriRefByIdentifier(identifier, distributionUriFuture);
        distributionUriFuture.compose(distributionUri -> {
            Future<String> distributionFuture = Future.future();
            if (distributionUri.getString("status", "").equals("not found")) {
                responseObject.mergeIn(distributionUri);
                distributionFuture.fail(distributionUri.getString("content", "failed getting distribution"));
                return distributionFuture;
            }
            connector.getDistribution(distributionUri.getString("distributionUriRef", ""), consumes, distributionFuture);
            return distributionFuture;

        }).setHandler(ar -> {
            if (ar.succeeded()) {

                Model dist = JenaUtils.read(ar.result().getBytes(), consumes);

                if (!dist.isEmpty()) {
                    handler.handle(Future.succeededFuture(new JsonObject()
                            .put("status", "success")
                            .put("content", ar.result())

                    ));
                } else {
                    handler.handle(Future.succeededFuture(new JsonObject()
                            .put("status", "not found")
                            .put("content", "Distribution with id " + identifier + " not found")
                    ));
                }


            } else {
                if (responseObject.containsKey("status")) {
                    handler.handle(Future.succeededFuture(responseObject));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }

            }

        });


        return this;
    }


    @Override
    public DistributionsService postDistribution(String distribution, String datasetId, String contentType, String catalogueId, Handler<AsyncResult<JsonObject>> handler) {
        Future<JsonObject> uriFuture = Future.future();
        connector.getDatasetUriRefs(datasetId, catalogueId, uriFuture);
        uriFuture.compose(uriRefs -> {
            Future<JsonObject> insertFuture = Future.future();
            String datasetUriRef = uriRefs.getString("datasetUriRef");
            insertDistribution(datasetId, catalogueId, distribution, DCATAPUriSchema.parseUriRef(datasetUriRef).getDatasetGraphName(), contentType, insertFuture);
            return insertFuture;
        }).setHandler(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(new JsonObject()
                        .put("status", "success")
                        .put("content", ar.result().getString("content", ""))
                        .put(HttpHeaders.LOCATION, ar.result().getString(HttpHeaders.LOCATION, ""))
                ));
            } else {
                try {
                    ReplyException s = (io.vertx.core.eventbus.ReplyException) ar.cause();
                    if (s.failureCode() == 409) {
                        handler.handle(Future.succeededFuture(new JsonObject()
                                .put("status", "already exists")
                                .put("content", ar.cause().getMessage())
                        ));
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }

                } catch (ClassCastException cce) {
                    handler.handle(Future.failedFuture(ar.cause()));
                } catch (Exception e) {
                    handler.handle(Future.failedFuture(ar.cause()));
                }

            }
        });
        return this;
    }

    //    @Override
    //    public DistributionsService postDistributionWithNormalizedDatasetId(String distribution, String normalizedDatasetId, String contentType, Handler<AsyncResult<JsonObject>> handler) {
    //        String datasetGraphName = DCATAPUriSchema.applyFor(normalizedDatasetId).datasetGraphName();
    //        Future<JsonObject> insertFuture = Future.future();
    //        insertDistribution(distribution, datasetGraphName, contentType, insertFuture);
    //        insertFuture.setHandler(ar -> {
    //            if (ar.succeeded()) {
    //                handler.handle(Future
    //                        .succeededFuture(
    //                                new JsonObject()
    //                                        .put("status", "success")
    //                                        .put(HttpHeaders.LOCATION, ar.result().getString(HttpHeaders.LOCATION,"")))
    //                );
    //            } else {
    //                handler.handle(Future.failedFuture(ar.cause()));
    //            }
    //        });
    //        return this;
    //    }

    @Override
    public DistributionsService putDistributionWithIdentifier(String distribution, String identifier, String contentType, Handler<AsyncResult<JsonObject>> handler) {
        JsonObject responseObject = new JsonObject();
        Future<JsonObject> distributionUriFuture = Future.future();
        connector.getDistributionUriRefByIdentifier(identifier, distributionUriFuture);

        distributionUriFuture.compose(distributionUriObject -> {
            Future<JsonObject> graphUriFuture = Future.future();
            if (distributionUriObject.getString("status", "").equals("not found")) {
                responseObject.mergeIn(distributionUriObject);
                graphUriFuture.fail(distributionUriObject.getString("content", "failed getting distribution"));
                return graphUriFuture;
            }
            responseObject.put("distributionUriRef", distributionUriObject.getString("distributionUriRef", ""));
            String distributionID = DCATAPUriSchema.parseUriRef(distributionUriObject.getString("distributionUriRef", "")).getId();
            connector.getDatasetUriRefForDistribution(distributionID, graphUriFuture);

            return graphUriFuture;

        }).compose(graphUriRefObject -> {
            Future<DatasetHelper> graphFuture = Future.future();
            if (graphUriRefObject.getString("status", "").equals("not found")) {
                responseObject.mergeIn(graphUriRefObject);
                graphFuture.fail(graphUriRefObject.getString("content", "failed PUTing distribution"));
                return graphFuture;
            }
            String datasetUriRef = DCATAPUriSchema.parseUriRef(graphUriRefObject.getString("identifier", "")).getDatasetGraphName();
            put(distribution, responseObject.getString("distributionUriRef", ""), datasetUriRef, contentType, responseObject, graphFuture);

            return graphFuture;
        }).setHandler(ar -> {
            DatasetHelper helper = ar.result();

            if (ar.succeeded()) {
                if (helper != null) {
                    index(helper);
                    translate(helper);
                }


                handler.handle(Future.succeededFuture(new JsonObject()
                        .put("status", "success")
                ));
            } else {
                if (responseObject.containsKey("status")) {
                    handler.handle(Future.succeededFuture(responseObject));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }

            }
        });
        return this;
    }


    @Override
    public DistributionsService putDistribution(String distribution, String distributionID, String contentType, Handler<AsyncResult<JsonObject>> handler) {
        String distributionUriRef = DCATAPUriSchema.applyFor(distributionID).getDistributionUriRef();
        Future<JsonObject> graphUriFuture = Future.future();
        connector.getDatasetUriRefForDistribution(distributionID, graphUriFuture);
        JsonObject responseObject = new JsonObject();


        graphUriFuture.compose(graphUriRef -> {
            Future<DatasetHelper> graphFuture = Future.future();
            if (graphUriRef.getString("status", "").equals("not found")) {
                responseObject.mergeIn(graphUriRef);
                graphFuture.fail(graphUriRef.getString("content", "failed PUTing distribution"));
                return graphFuture;
            }
            String datasetUriRef = DCATAPUriSchema.parseUriRef(graphUriRef.getString("identifier", "")).getDatasetGraphName();
            put(distribution, distributionUriRef, datasetUriRef, contentType, responseObject, graphFuture);

            return graphFuture;
        }).setHandler(ar -> {
            DatasetHelper helper = ar.result();

            if (ar.succeeded()) {
                if (helper != null) {
                    index(helper);
                    translate(helper);
                }


                handler.handle(Future.succeededFuture(new JsonObject()
                        .put("status", "success")
                ));
            } else {
                if (responseObject.containsKey("status")) {
                    handler.handle(Future.succeededFuture(responseObject));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }

            }
        });
        return this;
    }


    private void put(String distribution, String distributionUriRef, String datasetUriRef, String contentType, JsonObject responseObject, Handler<AsyncResult<DatasetHelper>> handler) {

        Future<String> graphFuture = Future.future();
        connector.getGraph(datasetUriRef, "application/n-triples", graphFuture);
        graphFuture.compose(graphModel -> {
            Future<DatasetHelper> helperFuture = Future.future();
            DatasetHelper.create(graphModel, "application/n-triples", helperFuture);
            return helperFuture;
        }).compose(helper -> {
            Future<String> withoutDistGraphFuture = Future.future();
            removeDistribution(helper, distributionUriRef, withoutDistGraphFuture);
            return withoutDistGraphFuture;
        }).compose(graphModel -> {
            Future<DatasetHelper> helperFuture = Future.future();
            DatasetHelper.create(graphModel, "application/n-triples", helperFuture);
            return helperFuture;
        }).compose(helper -> {
            Future<DatasetHelper> helperFuture = Future.future();
            helper.addDistribution(distribution, contentType, true, helperFuture);
            return helperFuture;
        }).compose(helper -> {
           /* //rename distribution
            helper.model().listResourcesWithProperty(RDF.type, DCAT.Distribution).forEachRemaining(resource -> {
                Resource renamed = ResourceUtils.renameResource(resource, distributionUriRef);
                if(!renamed.hasProperty(DCTerms.identifier)) {
                    renamed.addProperty(DCTerms.identifier, resource);
                }
            });
*/
            updateRecord(helper);

            Future<DatasetHelper> storeFuture = Future.future();
            store(helper, storeFuture);
            return storeFuture;

        }).setHandler(handler);
    }

    @Override
    public DistributionsService deleteDistribution(String id, Handler<AsyncResult<JsonObject>> handler) {

        Future<JsonObject> graphUriFuture = Future.future();
        connector.getDatasetUriRefForDistribution(id, graphUriFuture);
        JsonObject responseObject = new JsonObject();

        graphUriFuture.compose(graphUriRef -> {
            Future<String> graphFuture = Future.future();
            if (graphUriRef.getString("status", "").equals("not found")) {
                responseObject.mergeIn(graphUriRef);
                graphFuture.fail(graphUriRef.getString("content", "failed getting distribution"));
                return graphFuture;
            }
            connector.getGraph(DCATAPUriSchema.parseUriRef(graphUriRef.getString("identifier", "")).getDatasetGraphName(), "application/n-triples", graphFuture);
            return graphFuture;
        }).compose(graphModel -> {
            Future<DatasetHelper> helperFuture = Future.future();
            DatasetHelper.create(graphModel, "application/n-triples", helperFuture);
            return helperFuture;
        }).compose(helper -> {
            Future<String> withoutDistGraphFuture = Future.future();
            String distributionUriRef = DCATAPUriSchema.applyFor(id).getDistributionUriRef();
            removeDistribution(helper, distributionUriRef, withoutDistGraphFuture);
            return withoutDistGraphFuture;
        }).compose(graphModel -> {
            Future<DatasetHelper> helperFuture = Future.future();
            DatasetHelper.create(graphModel, "application/n-triples", helperFuture);
            return helperFuture;
        }).compose(helper -> {
            updateRecord(helper);

            Future<DatasetHelper> storeFuture = Future.future();
            store(helper, storeFuture);
            return storeFuture;
        }).setHandler(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(new JsonObject()
                        .put("status", "success")
                ));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }


    private void insertDistribution(String id, String catalogueID, String distribution, String datasetGraphName, String contentType, Handler<AsyncResult<JsonObject>> handler) {
        Future<String> graphFuture = Future.future();

        String identifier = getIdentifier(distribution, contentType);

        connector.getGraph(datasetGraphName, contentType, graphFuture);
        graphFuture.compose(graph -> {
            Future<DatasetHelper> helperFuture = Future.future();
            DatasetHelper.create(id, graph, contentType, null, catalogueID, helperFuture);
            return helperFuture;
        }).compose(helper -> {
            Future<DatasetHelper> helperFuture = Future.future();
            helper.addDistribution(distribution, contentType, false, helperFuture);
            return helperFuture;
        }).compose(helper -> {
            Future<DatasetHelper> storeFuture = Future.future();
            store(helper, storeFuture);
            return storeFuture;
        }).setHandler(ar -> {
            DatasetHelper helper = ar.result();


            if (ar.succeeded()) {
                String distributionUri = getDistributionURI(identifier, helper);

                index(helper);
                translate(helper);
                handler.handle(Future.succeededFuture(new JsonObject().put(HttpHeaders.LOCATION, distributionUri)));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private String getDistributionURI(String identifier, DatasetHelper helper) {

        for (ResIterator it = helper.model().listSubjectsWithProperty(RDF.type, DCAT.Distribution); it.hasNext(); ) {
            Resource res = it.next();
            Statement statement = res.getProperty(DCTerms.identifier);
            boolean found = false;
            if (statement != null) {
                RDFNode obj = statement.getObject();

                if (obj.isLiteral()) {
                    found = identifier.equals(obj.asLiteral().getString());
                } else if (obj.isURIResource()) {
                    found = identifier.equals(obj.asResource().getURI());
                }
            }
            if (found) {
                return res.getURI();
            }
        }

        return "";
    }


    private String getIdentifier(String distribution, String contentType) {

        Model model = JenaUtils.read(distribution.getBytes(), contentType);
        String identifier = "";

        ResIterator it = model.listSubjectsWithProperty(RDF.type, DCAT.Distribution);
        if (it.hasNext()) {
            Resource dist = it.next();

            identifier = JenaUtils.findIdentifier(dist);
            if (identifier == null && dist.isURIResource()) {
                identifier = dist.getURI();
            }
        }
        return identifier;
    }


    private void store(DatasetHelper helper, Handler<AsyncResult<DatasetHelper>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(helper, getClass());
        log.debug("Store dataset");
        connector.putGraph(helper.graphName(), helper.model(), ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                if (response.statusCode() == 200) {
                    handler.handle(Future.succeededFuture(helper));
                } else if (response.statusCode() == 201) {
                    handler.handle(Future.succeededFuture(helper));
                } else {
                    log.error("Store dataset: {}", response.statusMessage());
                    handler.handle(Future.failedFuture(response.statusMessage()));
                }
            } else {
                log.error("Store dataset", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }


    private void removeDistribution(DatasetHelper helper, String distributionUriRef, Handler<AsyncResult<String>> handler) {

        ConstructBuilder cb = new ConstructBuilder()
                .addConstruct("<" + distributionUriRef + ">", "?q", "?x")
                .addConstruct("?x", "?p", "?y")
                .addWhere("<" + distributionUriRef + ">", "?q", "?x")
                .addOptional("?x", "?p", "?y");

        try (QueryExecution qexec = QueryExecutionFactory.create(cb.build(), helper.model())) {
            Model result = qexec.execConstruct();
            Model diff = helper.model().difference(result);
            helper.model().getResource(helper.uriRef())
                    .listProperties(DCAT.distribution)
                    .filterKeep(statement -> statement.getResource().getURI().equals(distributionUriRef))
                    .forEachRemaining(diff::remove);
            handler.handle(Future.succeededFuture(JenaUtils.write(diff, Lang.NTRIPLES)));
        } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
        }
    }

    private void updateRecord(DatasetHelper helper) {

        Resource record = helper.recordResource();
        String hash = DigestUtils.md5Hex(JenaUtils.write(helper.model(), Lang.NTRIPLES));

        Resource checksum = record.getPropertyResourceValue(SPDX.checksum);
        checksum.removeAll(SPDX.checksumValue);
        checksum.addProperty(SPDX.checksumValue, hash);

        record.removeAll(DCTerms.modified);
        record.addProperty(DCTerms.modified, ZonedDateTime
                .now(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);
    }


    private void translate(DatasetHelper helper) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(helper, getClass());
        translationService.initializeTranslationProcess(helper, ar -> {
            if (ar.succeeded()) {
                log.debug("Requesting a new translation for model.");
            } else if (ar.failed()) {
                log.error("Dataset could not submitted to translation service.", ar.cause());
            }
        });
    }

    private void index(DatasetHelper helper) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(helper, getClass());
//        DatasetToIndexConverter datasetToIndexConverter = new DatasetToIndexConverter();
//        JsonObject indexMessage = datasetToIndexConverter.convert2(helper);
        JsonObject indexMessage = Indexing.indexingDataset(helper.resource(), helper.catalogueId(), helper.sourceLang());
        indexService.addDatasetPut(indexMessage, ar -> {
            if (ar.failed()) {
                log.error("Indexing", ar.cause());
            }
        });
    }

    private void validate(DatasetHelper helper) {
        if (ConfigHelper.forConfig(config).getJson(Constants.ENV_PIVEAU_HUB_VALIDATOR).getBoolean("enabled", false)) {
            vertx.eventBus().send(ValidationServiceVerticle.ADDRESS, helper.stringify(Lang.NTRIPLES));
        }
    }


}
