package io.piveau.hub.services.catalogues;

//import io.piveau.hub.converters.CatalogToIndexConverter;
import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.util.*;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.indexing.Indexing;
import io.piveau.utils.JenaUtils;
import io.piveau.utils.experimental.DCATAPUriRef;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCAT;

import java.io.ByteArrayInputStream;

public class CataloguesServiceImpl implements CataloguesService {

    private TSConnector connector;
    private IndexService indexService;

    CataloguesServiceImpl(TSConnector connector, Vertx vertx, Handler<AsyncResult<CataloguesService>> readyHandler) {
        this.connector = connector;
        this.indexService = IndexService.createProxy(vertx, IndexService.SERVICE_ADDRESS);
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public CataloguesService listCatalogues(String consumes, Integer limit, Integer offset, Handler<AsyncResult<JsonObject>> handler) {
        Handler<AsyncResult<HttpResponse<Buffer>>> asyncResultHandler = ar -> {
            if (ar.succeeded()) {
                //log.info(ar.result().bodyAsString());
                handler.handle(Future.succeededFuture(new JsonObject()
                        .put("contentType", consumes)
                        .put("status", "success")
                        .put("content", ar.result().bodyAsString())
                ));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        };
        connector.listCatalogs(consumes, limit, offset, asyncResultHandler);
        return this;
    }

    @Override
    public CataloguesService getCatalogue(String catalogueId, String consumes, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getCatalogueLogger(catalogueId,getClass());
        String graphName = DCATAPUriSchema.applyFor(catalogueId).getCatalogueGraphName();
        connector.getGraph(graphName, ar -> {
            if (ar.succeeded()) {
                connector.getDatasetsAndRecords(graphName, h -> {
                    if (h.succeeded()) {
                        Model model =ar.result();
                        Resource catalog = model.getResource(graphName);
                        h.result().get("dataset").forEach(resource -> {
                            catalog.addProperty(DCAT.dataset, resource);
                        });
                        h.result().get("record").forEach(resource -> {
                            catalog.addProperty(DCAT.record, resource);
                        });

                        String output =  JenaUtils.write(model,consumes);
                        log.debug("handle return");
                        handler.handle(Future.succeededFuture(new JsonObject()
                                .put("status", "success")
                                .put("content",output)
                        ));
                    } else {
                        handler.handle(Future.failedFuture(h.cause()));
                    }
                });

            } else {

                try{
                    ReplyException s = (io.vertx.core.eventbus.ReplyException)ar.cause();
                    if(s.failureCode()==404){
                        log.error("no cat: {}",s.failureType());
                        handler.handle(Future.succeededFuture(new JsonObject()
                                .put("status", "not found")
                        ));
                    }else{
                        handler.handle(Future.failedFuture(ar.cause()));
                    }



                }catch (ClassCastException cce){
                    log.error("casting is a no:",cce);
                    handler.handle(Future.failedFuture(ar.cause()));
                }catch(Exception e){
                    log.error("this does not work:",e);
                    handler.handle(Future.failedFuture(ar.cause()));
                }

            }
        });
        return this;
    }

    @Override
    public CataloguesService putCatalogue(String catalogueId, String catalogue, String contentType, String hash, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getCatalogueLogger(catalogueId,getClass());
        CatalogueHelper catalogueHelper = new CatalogueHelper(catalogueId, contentType, catalogue);
        // log.info("Puttin catalogue");
        Future<Void> createOrUpdateFuture = Future.future();

        connector.catalogueExists(catalogueHelper.uriRef(), er -> {
            if (er.succeeded()) {
                log.debug("Catalog already exists. lets update");
                connector.getDatasetsAndRecords(catalogueHelper.uriRef(), ar -> {
                    if (ar.succeeded()) {
                        catalogueHelper.model(ar.result(), createOrUpdateFuture);
                    } else {
                        createOrUpdateFuture.fail(ar.cause());
                    }
                });
            } else {
                log.debug("catalogue does not exist. Create new");
                catalogueHelper.model(createOrUpdateFuture);
            }
        });

        createOrUpdateFuture.compose(exist -> {
            Future<JsonObject> storeFuture = Future.future();
            store(catalogueHelper, storeFuture);
            return storeFuture;
        }).setHandler(ar -> {
            if (ar.succeeded()) {
                JsonObject indexMessage = Indexing.indexingCatalogue(catalogueHelper.getModel().getResource(catalogueHelper.uriRef()));
                indexService.addCatalog(indexMessage, h -> {
                    if (h.failed()) {
                        log.error(h.cause().getMessage());
                    }
                });

                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public CataloguesService postCatalogue(String catalogue, String contentType, Handler<AsyncResult<JsonObject>> handler) {
        handler.handle(Future.failedFuture("not implemented yet"));
        return this;
    }

    @Override
    public CataloguesService deleteCatalogue(String catalogueId, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getCatalogueLogger(catalogueId,getClass());

        DCATAPUriRef schema = DCATAPUriSchema.applyFor(catalogueId);
        String query = "SELECT ?dataset WHERE {GRAPH <" + schema.getCatalogueGraphName() + "> {<" + schema.getCatalogueUriRef() + "> <" + DCAT.dataset + "> ?dataset}}";
        connector.query(query, "application/json", ar -> {
            if (ar.succeeded()) {
                ResultSet resultSet = ResultSetFactory.fromJSON(new ByteArrayInputStream(ar.result().body().getBytes()));
                resultSet.forEachRemaining(querySolution -> {
                    Resource dataset = querySolution.getResource("dataset");
                    connector.deleteGraph(dataset.getURI(), dr -> {
                        if (dr.failed()) {
                            log.error("Delete dataset graph", dr.cause());
                        }
                    });
                    indexService.deleteDataset(DCATAPUriSchema.parseUriRef(dataset.getURI()).getId(), dd -> {
                        if (dd.failed()) {
                            log.error("Delete dataset index", dd.cause());
                        }
                    });
                });
                connector.deleteGraph(schema.getCatalogueGraphName(), gr -> {
                    if (gr.succeeded()) {
                        indexService.deleteCatalog(schema.getId(), dc -> {
                            if (dc.failed()) {
                                log.error("Delete catalogue index", dc.cause());
                            }else{
                                log.info("Catalogue deleted");
                            }
                            handler.handle(Future.succeededFuture());
                        });
                    } else {
                        handler.handle(Future.failedFuture(gr.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public CataloguesService existenceCheckCatalogue(String catalogueId, Handler<AsyncResult<JsonObject>> handler){
        connector.catalogueExists(DCATAPUriSchema.applyFor(catalogueId).getCatalogueUriRef(),ar->{
            if(ar.succeeded()){
                handler.handle(Future.succeededFuture());
            }else{
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    private void store(CatalogueHelper helper, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getCatalogueLogger(helper.getId(),getClass());
        log.trace("putting graph");
        connector.putGraph(helper.uriRef(), helper.getModel(), ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                if (response.statusCode() == 200) {
                    handler.handle(Future.succeededFuture(new JsonObject().put("status", "updated")));
                } else if (response.statusCode() == 201) {
                    handler.handle(Future.succeededFuture(new JsonObject().put("status", "created")));
                } else {
                    log.error("put catalogue: {}", response.statusMessage());
                    handler.handle(Future.failedFuture(response.statusMessage()));
                }
            } else {
                log.error("put catalogue", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
