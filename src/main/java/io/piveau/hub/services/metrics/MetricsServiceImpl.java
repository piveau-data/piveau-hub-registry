package io.piveau.hub.services.metrics;

import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.services.translation.TranslationService;
import io.piveau.hub.util.MetricHelper;
import io.piveau.hub.util.TSConnector;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.pipe.PipeLauncher;
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
import org.apache.http.HttpHeaders;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.riot.Lang;
import org.apache.jena.util.ResourceUtils;

public class MetricsServiceImpl implements MetricsService {

    private TSConnector connector;
    private JsonObject config;
    private Vertx vertx;
    private IndexService indexService;
    private TranslationService translationService;

    MetricsServiceImpl(TSConnector connector, JsonObject config, Vertx vertx, Handler<AsyncResult<MetricsService>> readyHandler) {
        this.vertx = vertx;
        this.connector = connector;
        this.config = config;
        this.indexService = IndexService.createProxy(vertx, IndexService.SERVICE_ADDRESS);
        this.translationService = TranslationService.createProxy(vertx, TranslationService.SERVICE_ADDRESS);
        readyHandler.handle(Future.succeededFuture(this));
    }


    @Override
    public MetricsService getMetric(String datasetId, String catalogueId, String consumes, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(datasetId, catalogueId, getClass());
        DCATAPUriRef dcatapUriSchema = DCATAPUriSchema.applyFor(datasetId);
        connector.getGraph(dcatapUriSchema.getMetricsUriRef(), consumes, ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(new JsonObject()
                        .put("status", "success")
                        .put("content", ar.result())
                ));
            } else {
                try {
                    ReplyException s = (ReplyException) ar.cause();
                    if (s.failureCode() == 404) {
                        log.error("no metric: {}", s.failureType());
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

    private void storeMetric(PiveauLogger log, MetricHelper helper, Handler<AsyncResult<JsonObject>> handler) {
        log.trace("Store metric");
        connector.putGraph(helper.metricGraphName(), helper.model(), ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                if (response.statusCode() == 200) {
                    handler.handle(Future.succeededFuture(new JsonObject().put("status", "updated").put(HttpHeaders.LOCATION, helper.metricRef())));
                } else if (response.statusCode() == 201) {
                    handler.handle(Future.succeededFuture(new JsonObject()
                            .put("status", "created")
                            .put("metric", helper.stringify(Lang.NTRIPLES))
                            .put(HttpHeaders.LOCATION, helper.metricRef())));
                } else {
                    log.error("Store metric: {}", response.statusMessage());
                    handler.handle(Future.failedFuture(response.statusMessage()));
                }
            } else {
                log.error("Store metric", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public MetricsService putMetric(String datasetId, String data, String contentType, String catalogueId, String hash, Handler<AsyncResult<JsonObject>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(datasetId, catalogueId, getClass());

        Dataset dataset = JenaUtils.readDataset(data.getBytes(), contentType);
        dataset.listNames().forEachRemaining(name -> log.info("name is " + name));

        MetricHelper.create(datasetId, data, contentType, hash, catalogueId, dr -> {
            if (dr.succeeded()) {
                MetricHelper metricHelper = dr.result();
                Property property=metricHelper.model().getProperty(metricHelper.getoldName());
                ResourceUtils.renameResource(property,metricHelper.metricRef());
                storeMetric(log, metricHelper, handler);
            } else {
                handler.handle(Future.failedFuture(dr.cause()));
            }
        });
        return this;
    }

/*
    @Override
    @Deprecated
    public MetricsService postMetric(String dataset, String contentType, Handler<AsyncResult<JsonObject>> handler) {
        handler.handle(Future.failedFuture("Not yet implemented"));
        return this;
    }
*/

    @Override
    public MetricsService deleteMetric(String datasetId, String catalogueId, Handler<AsyncResult<JsonObject>> handler) {
        //PiveauLogger log = PiveauLoggerFactory.getLogger(datasetId, catalogueId, getClass());
        DCATAPUriRef dcatapUriSchema = DCATAPUriSchema.applyFor(datasetId);
        connector.deleteGraph(dcatapUriSchema.getMetricsUriRef(), dr -> {
            if (dr.failed()) {
                handler.handle(Future.failedFuture(dr.cause()));
            }
            handler.handle(Future.succeededFuture(new JsonObject().put("status", "deleted")));
        });
        return this;
    }

}
