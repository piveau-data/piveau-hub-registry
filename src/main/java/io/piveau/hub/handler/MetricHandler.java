package io.piveau.hub.handler;

import io.piveau.hub.services.metrics.MetricsService;
import io.piveau.hub.util.Constants;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.hub.util.rdf.*;
import io.piveau.utils.JenaUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.http.HttpHeaders;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class MetricHandler {


    private MetricsService metricsService;

    public MetricHandler(Vertx vertx, String address) {
        metricsService = MetricsService.createProxy(vertx, address);
    }

    public void handleGetMetric(RoutingContext context) {
        String id = URLDecoder.decode(context.pathParam("id"), StandardCharsets.UTF_8);

        PiveauLogger log = PiveauLoggerFactory.getDatasetLogger(id, getClass());
        context.response().headers().forEach(head -> log.debug("Header - {}:{}", head.getKey(), head.getValue()));
        context.request().headers().forEach(head -> log.debug("Header - {}:{}", head.getKey(), head.getValue()));

        String catalogueId = context.queryParam("catalogue").isEmpty() ? null : context.queryParam("catalogue").get(0);
        String acceptType = context.getAcceptableContentType();

        Handler<AsyncResult<JsonObject>> har = getHandler(context, acceptType);

        metricsService.getMetric(id, catalogueId, acceptType, har);
    }


    private Handler<AsyncResult<JsonObject>> getHandler(RoutingContext context, String acceptType) {
        return ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                switch (result.getString("status")) {
                    case "success":
                        // JUST A WORKAROUND FOR NOW TO GET BETTER STRUCTURE FOR DEBUGGING
                        if (acceptType.equals(Lang.TURTLE.getHeaderString())) {
                            String payload = result.getString("content");
                            Model resultModel = ModelFactory.createDefaultModel();
                            RDFDataMgr.read(resultModel, new ByteArrayInputStream(payload.getBytes()), Lang.TURTLE);
                            resultModel.setNsPrefixes(SPDX.getNsMap());
                            resultModel.setNsPrefixes(DCATAP.getNsMap());
                            resultModel.setNsPrefixes(EDP.getNsMap());
                            resultModel.setNsPrefixes(EUVOC.getNsMap());
                            resultModel.setNsPrefixes(SHACL.getNsMap());
                            StringWriter out = new StringWriter();
                            RDFDataMgr.write(out, resultModel, RDFFormat.TURTLE);
                            context.response().putHeader("Content-Type", result.getString("contentType")).end(out.toString());
                        } else {
                            context.response().putHeader("Content-Type", result.getString("contentType")).end(result.getString("content"));
                        }
                        break;
                    case "not found":
                        context.response().setStatusCode(404).end();
                        break;
                    default:
                        context.response().setStatusCode(400).end();
                }
            } else {
                context.response().setStatusCode(500).end(ar.cause().getMessage());
            }
        };
    }


    public void handlePutMetric(RoutingContext context) {

        PiveauLoggerFactory.getLogger(getClass()).info("Handle Metric");
        String id = URLDecoder.decode(context.pathParam("id"), StandardCharsets.UTF_8);
        String catalogueId = context.queryParam("catalogue").get(0);
        PiveauLogger log = PiveauLoggerFactory.getLogger(id, catalogueId, getClass());

        String hash = context.queryParam("hash").size() > 0 ? context.queryParam("hash").get(0) : null;
        log.info("hash: {}", hash);
        String contentType = context.parsedHeaders().contentType().rawValue();
        log.info("received content type: {}", contentType);

        // Content-Type can look like: multipart/form-data; charset=utf-8; boundary=something, (see: https://tools.ietf.org/html/rfc7231#section-3.1.1.1) we need the first part
        String[] contentTypes = contentType.split(";");
        if (contentTypes.length > 0) contentType = contentTypes[0];

        if (!Constants.ALLOWED_CONTENT_TYPES.contains(contentType)) {
            context.response().setStatusCode(400).end("Content-Type header should have one of the following values: " + String.join(", ", Constants.ALLOWED_CONTENT_TYPES));
            log.info("returned 400");
            return;
        }


        String data = context.getBodyAsString();
        metricsService.putMetric(id, data, contentType, catalogueId, hash, ar -> {
            if (ar.succeeded()) {
                JsonObject status = ar.result();
                switch (status.getString("status")) {
                    case "created":
                        log.info("Respond with 201");
                        context.response().setStatusCode(201).putHeader(HttpHeaders.LOCATION, ar.result().getString(HttpHeaders.LOCATION, "")).end();
                        break;
                    case "updated":
                        log.info("Respond with 200");
                        context.response().setStatusCode(200).end();
                        break;
                    default:
                        // should not happen, succeeded path should only respond with 2xx codes
                        log.info("Respond with 400");
                        context.response().setStatusCode(400).end();
                }
            } else {
                if (ar.cause().getMessage() != null) {
                    switch (ar.cause().getMessage()) {
                        case "no metric":
                            log.info("Respond with 400");
                            context.response().setStatusCode(400).end("No metric with given id found.");
                            break;
                        case "skipped":
                            log.info("Respond with 304 - Metric is up to date");
                            context.response().setStatusCode(304).end("Metric is up to date");
                            break;
                        default:
                            log.error("Handling Error:", ar.cause());
                            context.response().setStatusCode(400).end(ar.cause().getMessage());
                    }
                } else {
                    log.error("Put metric", ar.cause());
                    context.response().setStatusCode(500).end();
                }
            }
        });
    }


    public void handleDeleteMetric(RoutingContext context) {
        String id = URLDecoder.decode(context.pathParam("id"), StandardCharsets.UTF_8);
        String catalogueId = context.queryParam("catalogue").get(0);
        metricsService.deleteMetric(id, catalogueId, ar -> {
            if (ar.succeeded()) {
                context.response().setStatusCode(200).end();
            } else {
                context.response().setStatusCode(500).setStatusMessage(ar.cause().getMessage()).end();
            }
        });
    }

/*    public void handlePostMetric(RoutingContext context) {
        throw new UnsupportedOperationException();
       *//* String contentType = context.parsedHeaders().contentType().rawValue();
        metricsService.postMetric(context.getBodyAsString(), contentType, ar -> {
            if (ar.succeeded()) {

            } else {

            }
        });*//*
    }*/


}
