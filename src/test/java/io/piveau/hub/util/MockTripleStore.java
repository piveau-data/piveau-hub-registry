package io.piveau.hub.util;

import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.utils.ConfigHelper;
import io.piveau.utils.JenaUtils;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.resultset.ResultsFormat;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

public class MockTripleStore extends AbstractVerticle {

    private Logger log = LoggerFactory.getLogger(getClass());

    private Dataset storeDataset;

    private static String dataEndpoint = "/data";

    public static String getDataEndpoint() {
        return dataEndpoint;
    }

    private static String queryEndpoint = "/query";

    public static String getQueryEndpoint() {
        return queryEndpoint;
    }

    private static String updateEndpoint = "/update";

    public static String getUpdateEndpoint() {
        return updateEndpoint;
    }

    public static JsonObject getTriplestoreConfig(Boolean withCatalog) {
        return new JsonObject()
                .put("host", "http://localhost")
                .put("port", 9096)
                .put("data_endpoint", getDataEndpoint())
                .put("query_endpoint", getQueryEndpoint())
                .put("update_endpoint", getUpdateEndpoint())
                .put("create_catalog", withCatalog);
    }

    @Override
    public void start(Promise<Void> startPromise) {
        PiveauLoggerFactory.getLogger(getClass()).trace("Starting piveau hub...");

        storeDataset = DatasetFactory.create();
        log.debug("Store reset!");

        JsonObject config = ConfigHelper.forConfig(config()).forceJsonObject(Constants.ENV_PIVEAU_HUB_TRIPLESTORE_CONFIG);

        if (config.getBoolean("create_catalog", false)) {
            addGraph();
        }

        Router router = Router.router(vertx);
        router.get(queryEndpoint).handler(this::query);

        router.put(dataEndpoint).handler(BodyHandler.create()).handler(this::putData);
        router.get(dataEndpoint).handler(this::getData);
        router.delete(dataEndpoint).handler(this::deleteData);

        router.get(updateEndpoint).handler(this::update);

        vertx.createHttpServer().requestHandler(router).listen(config.getInteger("port", 8890), ar -> {
            if (ar.succeeded()) {
                startPromise.complete();
            } else {
                startPromise.fail(ar.cause());
            }
        });
    }

    private void query(RoutingContext context) {
        //PiveauLoggerFactory.getLogger(getClass()).info("Got query");
        if (context.queryParam("query").isEmpty() || context.queryParam("query") == null || context.queryParam("query").get(0).isEmpty()) {
            PiveauLoggerFactory.getLogger(getClass()).warn("Param 'query' is missing or empty");
            context.response().setStatusCode(400).end("Param 'query' is missing or empty");
            return;
        }
        PiveauLoggerFactory.getLogger(getClass()).debug("query: {}", context.queryParam("query").get(0));
        String accept = null;
        try {
            accept = context.parsedHeaders().accept().get(0).rawValue();
        } catch (Exception e) {
            PiveauLoggerFactory.getLogger(getClass()).error("no accept header");
        }

        Query query = QueryFactory.create();
        try {
            //PiveauLoggerFactory.getLogger(getClass()).info("query is not empty");
            query = QueryFactory.create(context.queryParam("query").get(0));
        } catch (Exception e) {
            PiveauLoggerFactory.getLogger(getClass()).error("convert query error:", e);
        }

        try (QueryExecution qexec = QueryExecutionFactory.create(query, storeDataset)) {
            Model model;
            switch (query.getQueryType()) {
                case Query.QueryTypeSelect:
                    PiveauLoggerFactory.getLogger(getClass()).trace("Select query");
                    try {
                        ResultSet rset = qexec.execSelect();
                        ByteArrayOutputStream os = new ByteArrayOutputStream();
                        ResultsFormat fmt;
                        if (accept != null) {

                            fmt = (ResultsFormat.lookup(accept.split("/")[1]) != null)
                                    ? ResultsFormat.lookup(accept.split("/")[1])
                                    : ResultsFormat.FMT_RS_JSON;
                        } else {
                            fmt = ResultsFormat.FMT_RS_JSON;
                        }

                        ResultSetFormatter.output(os, rset, fmt);
                        context.response().setStatusCode(200).end(os.toString());
                    } catch (Exception e) {
                        PiveauLoggerFactory.getLogger(getClass()).error("Select query error:", e);
                    }
                    break;
                case Query.QueryTypeConstruct:
                    model = qexec.execConstruct();
                    context.response().setStatusCode(200).end(JenaUtils.write(model, context.getAcceptableContentType()));
                    break;
                case Query.QueryTypeAsk:
                    String answer = String.valueOf(qexec.execAsk());
                    context.response().setStatusCode(200).end(answer);
                case Query.QueryTypeDescribe:
                    model = qexec.execDescribe();
                    context.response().setStatusCode(200).end(JenaUtils.write(model, context.getAcceptableContentType()));
                    break;
                default:
                    qexec.abort();
                    context.response().setStatusCode(400).end("query error");
            }

        } catch (Exception e) {
            context.response().setStatusCode(500).end(e.getMessage());
        }
    }


    private void update(RoutingContext context) {
        PiveauLoggerFactory.getLogger(getClass()).trace("Got update");
        if (context.queryParam("query").isEmpty() || context.queryParam("query").get(0).isEmpty()) {
            context.response().setStatusCode(400).end("Param 'query' is missing or empty");
            return;
        }
        try {
            UpdateRequest updateRequest = UpdateFactory.create(context.queryParam("query").get(0));
            UpdateAction.execute(updateRequest, storeDataset);
        } catch (Exception e) {
            PiveauLoggerFactory.getLogger(getClass()).error("Update error", e);
        }
        context.response().setStatusCode(200).end();

    }

    private void getData(RoutingContext context) {

        if (context.queryParam("graph").isEmpty() || context.queryParam("graph").get(0).isEmpty()) {
            context.response().setStatusCode(400).end("Param 'graph' is missing or empty");
            return;
        }
        String graphUri = context.queryParam("graph").get(0);

        Model model = storeDataset.getNamedModel(graphUri);
        if (model == null || model.isEmpty()) {
            context.response().setStatusCode(404).end();
            return;
        }

        context.response().setStatusCode(200).end(JenaUtils.write(model, context.getAcceptableContentType()));

    }

    private void putData(RoutingContext context) {

        if (context.queryParam("graph").isEmpty() || context.queryParam("graph").get(0).isEmpty()) {
            context.response().setStatusCode(400).end("Param 'graph' is missing or empty");
            return;
        }
        String graphUri = context.queryParam("graph").get(0);

        String contentType = context.parsedHeaders().contentType().rawValue();
        String[] contentTypes = contentType.split(";");
        if (contentTypes.length > 0) contentType = contentTypes[0];


        // PiveauLoggerFactory.getLogger(getClass()).info("Body: {}",context.getBodyAsString() );
        Model model = JenaUtils.read(context.getBodyAsString().getBytes(), contentType);

        if (storeDataset.containsNamedModel(graphUri)) {
            storeDataset.replaceNamedModel(graphUri, model);
            //PiveauLoggerFactory.getLogger(getClass()).info("replaced in ds");
            context.response().setStatusCode(200).end();
        } else {
            storeDataset.addNamedModel(graphUri, model);
            //PiveauLoggerFactory.getLogger(getClass()).info("added model to ds");
            context.response().setStatusCode(201).end();
        }
    }

    private void deleteData(RoutingContext context) {
        if (context.queryParam("graph").isEmpty() || context.queryParam("graph").get(0).isEmpty()) {
            context.response().setStatusCode(400).end("Param 'graph' is missing or empty");
            return;
        }
        String graphUri = context.queryParam("graph").get(0);
        if (!storeDataset.containsNamedModel(graphUri)) {
            context.response().setStatusCode(404).end();
            return;
        }
        storeDataset.removeNamedModel(graphUri);
        context.response().setStatusCode(200).end();
    }


    private void addGraph() {
        FileSystem fileSystem = vertx.fileSystem();
        Buffer buffer = fileSystem.readFileBlocking("misc/example_catalog.ttl");
        String graphUri = DCATAPUriSchema.applyFor("test-catalog").getCatalogueGraphName();

        Model model = JenaUtils.read(buffer.getBytes(), "text/turtle");

        if (storeDataset.containsNamedModel(graphUri)) {
            storeDataset.replaceNamedModel(graphUri, model);
        } else {
            storeDataset.addNamedModel(graphUri, model);
        }
    }

}
