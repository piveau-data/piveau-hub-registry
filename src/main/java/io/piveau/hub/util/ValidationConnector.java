package io.piveau.hub.util;

import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.utils.JenaUtils;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;


public class ValidationConnector {
    private WebClient client;
    private String uri;

    public ValidationConnector(WebClient client, String validator) {
        this.client = client;
        uri = validator + "/validation/report";
    }

    public Future<Model> validate(Model model) {
        HttpRequest<Buffer> req = client.postAbs(uri);
        req.putHeader("Content-Type", "application/n-triples");
        Future<Model> modelFuture = Future.future();
        PiveauLogger log = PiveauLoggerFactory.getLogger(getClass());
        try {
            String body = JenaUtils.write(model, Lang.NTRIPLES);
            req.sendBuffer(Buffer.buffer(body), ar -> {
                if (ar.succeeded()) {
                    log.info("Validation successful");
                    modelFuture.complete(JenaUtils.read(ar.result().body().getBytes(), "text/turtle"));
                } else {
                    log.error("Validation failed", ar.cause());
                    modelFuture.fail(ar.cause());
                }
            });
        } catch (Exception e) {
            modelFuture.fail(e);
        }

        return modelFuture;
    }

}
