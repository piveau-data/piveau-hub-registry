package io.piveau.hub.verticles;

import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.util.*;
import io.piveau.hub.util.Constants;

import io.piveau.indexing.Indexing;
import io.piveau.rdf.RDFMimeTypes;
import io.piveau.utils.ConfigHelper;
import io.piveau.utils.JenaUtils;
import io.piveau.utils.experimental.DCATAPUriRef;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.cli.Argument;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.shell.ShellService;
import io.vertx.ext.shell.ShellServiceOptions;
import io.vertx.ext.shell.command.CommandBuilder;
import io.vertx.ext.shell.command.CommandProcess;
import io.vertx.ext.shell.command.CommandRegistry;
import io.vertx.ext.shell.term.HttpTermOptions;
import io.vertx.ext.shell.term.TelnetTermOptions;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.DCAT;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ShellVerticle extends AbstractVerticle {

    private IndexService indexService;

    private TSConnector connector;

    @Override
    public void start(Promise<Void> promise) {

        CircuitBreaker breaker = CircuitBreaker.create("shell-breaker", vertx,
                new CircuitBreakerOptions().setMaxRetries(2))
                .retryPolicy(retryCount -> retryCount * 1000L);

        WebClient client = WebClient.create(vertx);

        JsonObject conf = ConfigHelper.forConfig(config()).forceJsonObject(Constants.ENV_PIVEAU_HUB_TRIPLESTORE_CONFIG);
        connector = TSConnector.create(client, null, conf);

        JsonObject cliConfig = ConfigHelper.forConfig(config()).forceJsonObject(Constants.ENV_PIVEAU_HUB_SEARCH_CLI_CONFIG);
        Integer cliPort = cliConfig.getInteger("port", 8085);
        String cliType = cliConfig.getString("type", "http");

        if (cliType.equals("http")) {
            ShellServiceOptions shellServiceOptions = new ShellServiceOptions()
                    .setWelcomeMessage(" Welcome to piveau-hub CLI!")
                    .setTelnetOptions(new TelnetTermOptions()
                            .setHost("0.0.0.0")
                            .setPort(5000))
                    .setHttpOptions(new HttpTermOptions()
                            .setHost("0.0.0.0")
                            .setPort(cliPort))
                    .setSessionTimeout(21600000);

            ShellService service = ShellService.create(vertx, shellServiceOptions);
            service.start(ar -> {
                if (ar.succeeded()) {
                    indexService = IndexService.createProxy(vertx, IndexService.SERVICE_ADDRESS);

                    CommandRegistry.getShared(vertx).registerCommand(simskiReindex().build(vertx));

                    promise.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
        }
    }

    private CommandBuilder simskiReindex() {
        CommandBuilder commandBuilder = CommandBuilder.command(
                CLI.create("syncIndex").addArgument(
                        new Argument()
                                .setArgName("catalogueIds")
                                .setRequired(false)
                                .setDescription("The ids of the catalogues to re-index."))
                        .addOption(
                                new Option()
                                        .setArgName("exclude")
                                        .setShortName("e")
                                        .setLongName("exclude")
                                        .setDefaultValue("")
                                        .setDescription("Exclude a list of catalogues"))
                        .addOption(
                                new Option()
                                        .setArgName("partitionSize")
                                        .setShortName("p")
                                        .setLongName("partitionSize")
                                        .setDefaultValue("1000")
                                        .setDescription("Page size for partitioning datasets."))
                        .addOption(new Option().setHelp(true).setFlag(true).setArgName("help").setShortName("h").setLongName("help"))
                        .addOption(new Option().setFlag(true).setArgName("verbose").setShortName("v").setLongName("verbose")));

        commandBuilder.processHandler(process -> {
            process.session().put("syncIndexCounter", new AtomicInteger());

            CommandLine commandLine = process.commandLine();
            if (commandLine.allArguments().isEmpty()) {
                process.write("Reindexing all catalogues\n");
                listCatalogues(ar -> {
                    if (ar.succeeded()) {
                        syncCatalogues(ar.result(), process);
                    } else {
                        process.write("Reindexing failed: " + ar.cause().getMessage() + "\n");
                        process.end();
                    }
                });
            } else {
                List<String> catalogueIds = commandLine.allArguments();
                syncCatalogues(catalogueIds.stream().map(id -> io.piveau.utils.experimental.DCATAPUriSchema.applyFor(id).getCatalogueUriRef()).collect(Collectors.toList()), process);
            }
        });
        return commandBuilder;
    }

    private void syncCatalogues(List<String> uriRefs, CommandProcess process) {
        Instant start = Instant.now();
        String exclude = process.commandLine().getOptionValue("exclude");
        List<String> excludeCatalogues = exclude.isBlank() ? Collections.emptyList() : Arrays.stream(exclude.split(",")).map(s -> io.piveau.utils.experimental.DCATAPUriSchema.applyFor(s).getCatalogueUriRef()).collect(Collectors.toList());
        uriRefs.removeAll(excludeCatalogues);
        if (!uriRefs.isEmpty()) {
            Promise<Void> promise = Promise.promise();
            reduceCatalogues(uriRefs, process, promise);
            promise.future().setHandler(v -> {
                process.write("Reindexing all catalogues finished. Overall duration " + Duration.between(start, Instant.now()) + "\n");
                process.end();
            });
        } else {
            process.write("No catalogues for indexing.\n");
            process.end();
        }
    }

    private void reduceCatalogues(List<String> catalogues, CommandProcess process, Promise<Void> promise) {
        String catalogue = catalogues.remove(0);
        DCATAPUriRef schema = DCATAPUriSchema.parseUriRef(catalogue);
        process.write("Start indexing " + schema.getId() + "\n");
        syncCatalogue(schema, process, ic -> {
            if (ic.succeeded()) {
                process.write("Reindex of " + schema.getId() + " finished. Duration " + ic.result().toString() + "\n");
            } else {
                process.write("Reindex of " + schema.getId() + " failed: " + ic.cause().getMessage() + "\n");
            }
            if (catalogues.isEmpty()) {
                promise.complete();
            } else {
                reduceCatalogues(catalogues, process, promise);
            }
        });
    }

    private void syncCatalogue(DCATAPUriRef catalogueRef, CommandProcess process, Handler<AsyncResult<Duration>> handler) {
        Instant start = Instant.now();

        int chunk = Integer.parseInt(process.commandLine().getOptionValue("partitionSize"));
        boolean verbose = process.commandLine().isFlagEnabled("verbose");

        String queryCatalogue = "CONSTRUCT WHERE {GRAPH <" + catalogueRef.getCatalogueGraphName() + "> {?s ?p ?o filter (?p != dcat:record)}}";
        constructGraph(queryCatalogue, ModelFactory.createDefaultModel(), 0, ar -> {
            if (ar.succeeded()) {
                Model catalogueModel = ar.result();
                Promise<Void> cataloguePromise = Promise.promise();
                indexService.addCatalog(Indexing.indexingCatalogue(catalogueModel.getResource(catalogueRef.getCatalogueUriRef())), cr -> {
                    if (cr.succeeded()) {
                        process.write("Indexing metadata of catalogue " + catalogueRef.getId() + " was successful.\n");
                        cataloguePromise.complete();
                    } else {
                        process.write("Indexing metadata of catalogue " + catalogueRef.getId() + " failed: " + cr.cause().getMessage() + "\n");
                        cataloguePromise.fail(cr.cause());
                    }
                });
                cataloguePromise.future().compose(v -> {
                    Promise<Void> promise = Promise.promise();
                    List<RDFNode> datasets = catalogueModel.listObjectsOfProperty(DCAT.dataset).toList();
                    process.write("Start indexing " + datasets.size() + " datasets...\n");
                    List<List<RDFNode>> partitions = Lists.partition(datasets, chunk);
                    if (verbose) {
                        process.write("Index datasets in " + partitions.size() + " partitions\n");
                    }
                    reducePartitions(catalogueRef, partitions, process, promise);
                    return promise.future();
                }).setHandler(v -> {
                    if (v.succeeded()) {
                        process.write("\nIndexing datasets finished.\n");
                        getIndexDatasetIds(catalogueRef.getId(), il -> {
                            if (il.succeeded()) {
                                Set<String> indexList = il.result();
                                Set<String> storeList = catalogueModel.listObjectsOfProperty(DCAT.dataset).toSet().stream().map(node -> DCATAPUriSchema.parseUriRef(node.asResource().getURI()).getId()).collect(Collectors.toSet());
                                indexList.removeAll(storeList);
                                process.write("Number of obsolete datasets in " + catalogueRef.getId() + " index: " + indexList.size() + "\n");
                                indexList.forEach(id -> {
                                    indexService.deleteDataset(id, dr -> {
                                        if (dr.failed()) {
                                            process.write(dr.cause().getMessage());
                                        }
                                    });
                                });
                                handler.handle(Future.succeededFuture(Duration.between(start, Instant.now())));
                            } else {
                                process.write(il.cause().getMessage());
                                handler.handle(Future.failedFuture(il.cause()));
                            }
                        });
                    } else {
                        handler.handle(Future.failedFuture(v.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void reducePartitions(DCATAPUriRef catalogueSchema, List<List<RDFNode>> partitions, CommandProcess process, Promise<Void> promise) {
        boolean verbose = process.commandLine().isFlagEnabled("verbose");

        if (partitions.isEmpty()) {
            if (verbose) {
                process.write("No more partitions\n");
            }
            promise.complete();
        } else {
            List<Future> futures = new ArrayList<>();
            List<RDFNode> partition = partitions.get(0);
            partition.forEach(node -> {
                if (node.isURIResource()) {
                    if (verbose) {
                        process.write("Index dataset " + node.asResource().getURI() + "\n");
                    }
                    Promise<Void> datasetPromise = Promise.promise();
                    futures.add(datasetPromise.future());
                    String queryDataset = "CONSTRUCT WHERE {GRAPH <" + node.asResource().getURI() + "> {?s ?p ?o }}";
                    constructGraph(queryDataset, ModelFactory.createDefaultModel(), 0, dr -> {
                        if (dr.succeeded()) {
                            if (verbose) {
                                process.write("Dataset " + node.asResource().getURI() + " fetched successfully\n");
                            }
                            Model dataset = dr.result();
                            try {
                                JsonObject indexInfo = Indexing.indexingDataset(dataset.getResource(node.asResource().getURI()), catalogueSchema.getId(), "de");
                                if (!indexInfo.isEmpty()) {
                                    if (verbose) {
                                        process.write("Index info for " + node.asResource().getURI() + " generated successfully\n");
                                    }
                                    indexService.addDatasetPut(indexInfo, ir -> {
                                        if (ir.failed()) {
                                            process.write("\nSent indexed dataset " + node.asResource().getURI() + " failed: " + indexInfo.encodePrettily() + " - " + ir.cause() + "\n");
                                            datasetPromise.fail(ir.cause());
                                        } else {
                                            if (verbose) {
                                                process.write("Dataset " + node.asResource().getURI() + " indexed successfully\n");
                                            }
                                            datasetPromise.complete();
                                        }
                                    });
                                } else {
                                    if (verbose) {
                                        process.write("Index info for " + node.asResource().getURI() + " was empty!\n");
                                    }
                                    datasetPromise.fail("Index info for " + node.asResource().getURI() + " was empty!");
                                }
                            } catch (Exception e) {
                                if (verbose) {
                                    process.write("Indexing dataset " + node.asResource().getURI() + " failed (" + e.getMessage() + "): " + JenaUtils.write(dataset, Lang.TURTLE) + "\n");
                                } else {
                                    process.write("\nIndexing dataset " + node.asResource().getURI() + " failed: " + e.getMessage() + "\n");
                                }
                                datasetPromise.fail(e);
                            }
                        } else {
                            process.write("\nFailed to fetch " + node.asResource().getURI() + ": " + dr.cause().getMessage() + "\n");
                            datasetPromise.fail(dr.cause());
                        }
                    });
                }
            });
            CompositeFuture.join(futures).setHandler(cf -> {
                if (!verbose) {
                    AtomicInteger counter = process.session().get("syncIndexCounter");
                    process.write("\rIndexed " + counter.addAndGet(partition.size()));
                }
                reducePartitions(catalogueSchema, partitions.subList(1, partitions.size()), process, promise);
            });
        }
    }

    private void constructGraph(String query, Model model, int offset, Handler<AsyncResult<Model>> handler) {
        String page = query + " OFFSET " + offset + " LIMIT 5000";
        connector.query(page, RDFMimeTypes.NTRIPLES, ar -> {
            if (ar.succeeded()) {
                String body = ar.result().bodyAsString();
                if (!body.contains("# Empty NT")) {
                    model.add(JenaUtils.read(body.getBytes(), RDFMimeTypes.NTRIPLES));
                    constructGraph(query, model, offset + 500, handler);
                } else {
                    handler.handle(Future.succeededFuture(model));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void listCatalogues(Handler<AsyncResult<List<String>>> handler) {
        String query = "SELECT ?c WHERE {?c a dcat:Catalog}";
        connector.query(query, "application/json", ar -> {
            if (ar.succeeded()) {
                List<String> catalogues = new ArrayList<>();
                ResultSet set = ResultSetFactory.fromJSON(new ByteArrayInputStream(ar.result().body().getBytes()));
                if (set.hasNext()) {
                    set.forEachRemaining(qs -> catalogues.add(qs.getResource("c").getURI()));
                }
                handler.handle(Future.succeededFuture(catalogues));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void getIndexDatasetIds(String catalogueId, Handler<AsyncResult<Set<String>>> handler) {
        String address = config().getString(Constants.ENV_PIVEAU_HUB_ELASTICSEARCH_ADDRESS, "http://elasticsearch:9200");
        WebClient client = WebClient.create(vertx);
        HttpRequest<Buffer> request = client.getAbs(address + "/dataset/_search")
                .addQueryParam("q", "catalog.id:" + catalogueId)
                .addQueryParam("_source_includes", "id")
                .addQueryParam("size", "250000");

        request.send(ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result().bodyAsJsonObject();
                JsonArray hits = result.getJsonObject("hits", new JsonObject()).getJsonArray("hits", new JsonArray());
                Set<String> ids = hits.stream().map(obj -> ((JsonObject) obj).getJsonObject("_source").getString("id")).collect(Collectors.toSet());
                handler.handle(Future.succeededFuture(ids));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

}
