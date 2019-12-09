package io.piveau.hub.util.rdf;

import io.piveau.hub.util.Constants;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.SKOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class VocabularyManager {

    private static final Logger log = LoggerFactory.getLogger(VocabularyManager.class);

    public static Model model = ModelFactory.createDefaultModel();

    public static WebClient client;

    public static void init(JsonObject config, Vertx vertx, Handler<AsyncResult<Void>> handler) {
        client = WebClient.create(vertx);
        if (config.getBoolean(Constants.ENV_PIVEAU_HUB_LOAD_VOCABULARIES, true)) {
            loadVocabularies(vertx, config.getBoolean(Constants.ENV_PIVEAU_HUB_LOAD_VOCABULARIES_FETCH, false), handler);
        } else {
            log.debug("Loading vocabularies disabled.");
            handler.handle(Future.succeededFuture());
        }
    }

    private static void loadVocabularies(Vertx vertx, boolean fetch, Handler<AsyncResult<Void>> handler) {
        List<Future> vocFutures = new ArrayList<>();

        if (fetch) {
            vocFutures.add(fetchVocabulary(vertx, "http://publications.europa.eu/resource/authority/language", "vocabularies/languages-skos.rdf"));
            vocFutures.add(fetchVocabulary(vertx, "http://publications.europa.eu/resource/authority/data-theme", "vocabularies/data-theme-skos-ap-act.rdf"));
            vocFutures.add(fetchVocabulary(vertx, "http://publications.europa.eu/resource/authority/file-type", "vocabularies/filetypes-skos.rdf"));
            vocFutures.add(fetchVocabulary(vertx, "http://publications.europa.eu/resource/authority/country", "vocabularies/countries-skos-ap-act.rdf"));
            vocFutures.add(fetchVocabulary(vertx, "http://publications.europa.eu/resource/authority/continent", "vocabularies/continents-skos-ap-act.rdf"));
            vocFutures.add(fetchVocabulary(vertx, "http://publications.europa.eu/resource/authority/corporate-body", "vocabularies/corporatebodies-skos-ap-act.rdf"));
            vocFutures.add(fetchVocabulary(vertx, "http://publications.europa.eu/resource/authority/licence", "vocabularies/licences-skos.rdf"));
        } else {
            vocFutures.add(loadVocabulary(vertx, "vocabularies/languages-skos.rdf"));
            vocFutures.add(loadVocabulary(vertx, "vocabularies/data-theme-skos-ap-act.rdf"));
            vocFutures.add(loadVocabulary(vertx, "vocabularies/filetypes-skos.rdf"));
            vocFutures.add(loadVocabulary(vertx, "vocabularies/countries-skos-ap-act.rdf"));
            vocFutures.add(loadVocabulary(vertx, "vocabularies/continents-skos-ap-act.rdf"));
            vocFutures.add(loadVocabulary(vertx, "vocabularies/corporatebodies-skos-ap-act.rdf"));
            vocFutures.add(loadVocabulary(vertx, "vocabularies/licences-skos.rdf"));
        }
        vocFutures.add(loadVocabulary(vertx, "vocabularies/piveau-licences-skos.rdf"));

        CompositeFuture.join(vocFutures).setHandler(ar -> {
            if (ar.succeeded()) {
                log.debug("Loading vocabularies finished.");
                handler.handle(Future.succeededFuture());
            } else {
                log.error("Loading vocabularies failed.", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private static Future<Void> loadVocabulary(Vertx vertx, String resource) {
        Promise<Void> promise = Promise.promise();
        vertx.fileSystem().readFile(resource, ar -> {
            if (ar.succeeded()) {
                vertx.executeBlocking(parse -> {
                    parseRDF(new ByteArrayInputStream(ar.result().getBytes()));
                    parse.complete();
                }, res -> {
                    if (res.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(res.cause());
                    }
                });
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    private static Future<Void> fetchVocabulary(Vertx vertx, String uri, String resource) {
        Promise<Void> fetchPromise = Promise.promise();
        readVocabulary(uri).setHandler(read -> {
            if (read.succeeded()) {
                // iterate through concepts
                List<Future> conceptFutures = model
                        .listSubjectsWithProperty(SKOS.inScheme, model.createResource(uri))
                        .mapWith(r -> (Future)readVocabulary(r.getURI()))
                        .toList();
                CompositeFuture.join(conceptFutures).setHandler(concepts -> {
                    if (concepts.succeeded()) {
                        log.debug("{} - {} concepts", uri, conceptFutures.size());
                        fetchPromise.complete();
                    } else {
                        log.warn("Fetch concept from remote failed (using local fallback)", concepts.cause());
                        loadVocabulary(vertx, resource).setHandler(load -> {
                            if (load.succeeded()) {
                                fetchPromise.complete();
                            } else {
                                fetchPromise.fail(load.cause());
                            }
                        });
                    }
                });
            } else {
                log.warn("Fetch concept scheme from remote failed (using local fallback)", read.cause());
                loadVocabulary(vertx, resource).setHandler(load -> {
                    if (load.succeeded()) {
                        fetchPromise.complete();
                    } else {
                        fetchPromise.fail(load.cause());
                    }
                });
            }
        });
        return fetchPromise.future();
    }

    private static Future<Void> readVocabulary(String uri) {
        Promise<Void> readPromise = Promise.promise();
        client.getAbs(uri).expect(ResponsePredicate.SC_OK).send(ar -> {
            if (ar.succeeded()) {
                parseRDF(new ByteArrayInputStream(ar.result().body().getBytes()));
                readPromise.complete();
            } else {
                readPromise.fail(ar.cause());
            }
        });
        return readPromise.future();
    }

    private static synchronized void parseRDF(InputStream stream) {
        RDFDataMgr.read(model, stream, Lang.RDFXML);
    }

}
