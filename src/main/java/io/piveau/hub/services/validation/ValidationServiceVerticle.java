package io.piveau.hub.services.validation;

import io.piveau.hub.util.Constants;
import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.util.TSConnector;
import io.piveau.hub.util.ValidationConnector;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.hub.util.rdf.DQV;
import io.piveau.hub.util.rdf.OA;
import io.piveau.hub.util.rdf.SHACL;
import io.piveau.utils.ConfigHelper;
import io.piveau.utils.JenaUtils;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;

public class ValidationServiceVerticle extends AbstractVerticle {

    public static final String ADDRESS = "io.piveau.hub.validation.queue";

    private ValidationConnector validation;
    private TSConnector connector;

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.eventBus().consumer(ADDRESS, this::handleValidation);
        WebClient client = WebClient.create(vertx);

        JsonObject validatorConfig = ConfigHelper.forConfig(config()).getJson(Constants.ENV_PIVEAU_HUB_VALIDATOR);
        validation = new ValidationConnector(client, validatorConfig.getString("url"));

        CircuitBreaker breaker = CircuitBreaker.create("virt-breaker", vertx, new CircuitBreakerOptions().setMaxRetries(2))
                .retryPolicy(count -> count * 1000L);
        connector = TSConnector.create(client, breaker, ConfigHelper.forConfig(config()).getJson(Constants.ENV_PIVEAU_HUB_TRIPLESTORE_CONFIG));

        startPromise.complete();
    }

    private void handleValidation(Message<DatasetHelper> message) {
        DatasetHelper helper = message.body();
        PiveauLogger log = PiveauLoggerFactory.getLogger(helper,getClass());

        Model dataset = JenaUtils.extractResource(helper.resource());
        Future<Model> future = validation.validate(dataset);
        future.compose(vr -> {
            Promise<HttpResponse<Buffer>> storeFuture = Promise.promise();

            Resource qa = vr.createResource(DQV.QualityAnnotation);
            vr.listSubjectsWithProperty(RDF.type, SHACL.ValidationReport).forEachRemaining(s -> qa.addProperty(OA.hasBody, s));

            connector.putGraph(helper.validationGraphName(), vr, storeFuture);
            return storeFuture.future();
        }).compose(response -> {
            Promise<HttpResponse<Buffer>> recordPromise = Promise.promise();
            String query = "WITH <" + helper.graphName() + "> DELETE { <" + helper.recordUriRef() + "> <" + DQV.hasQualityAnnotation
                    + "> ?o } INSERT { <" + helper.recordUriRef() + "> <" + DQV.hasQualityAnnotation + "> <" + helper.validationUriRef() + "> }";

            connector.update(query, null, recordPromise);
            return recordPromise.future();
        }).setHandler(rr -> {
            if (rr.succeeded()) {
                log.info("Validation finished for Dataset");
            } else {
                log.error("Validation failed for Dataset", rr.cause());
            }
        });
    }

}
