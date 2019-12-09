package io.piveau.hub.services.translation;

import io.piveau.hub.services.index.IndexService;
import io.piveau.hub.util.Constants;
import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.util.TSConnector;
import io.piveau.hub.util.rdf.EDP;
import io.piveau.indexing.Indexing;
import io.piveau.rdf.LanguageTag;
import io.piveau.utils.ConfigHelper;
import io.piveau.utils.Piveau;
import io.piveau.utils.PiveauContext;
import io.piveau.utils.experimental.DCATAPUriRef;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import kotlin.Triple;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class TranslationServiceImpl implements TranslationService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JsonObject config;
    private final TSConnector tsConnector;
    private final IndexService indexService;

    private WebClient client;

    private PiveauContext moduleContext;

    TranslationServiceImpl(Vertx vertx, WebClient client, JsonObject config, TSConnector tsConnector,
                           Handler<AsyncResult<TranslationService>> readyHandler) {
        this.client = client;
        this.config = config;
        this.tsConnector = tsConnector;
        this.indexService = IndexService.createProxy(vertx, IndexService.SERVICE_ADDRESS);

        moduleContext = Piveau.createPiveauContext("hub", "Translation");

        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public TranslationService initializeTranslationProcess(DatasetHelper helper, Handler<AsyncResult<DatasetHelper>> asyncHandler) {
        PiveauContext resourceContext = moduleContext.extend(helper.id());

        addCatalogRecordDetailsBeforeTranslation(helper);

        final JsonObject requestBody = buildJsonFromResource(helper);

        sendRequestToTranslationServiceMiddleware(requestBody).setHandler(response -> {
            if (response.succeeded()) {
                resourceContext.log().info("Translation initialized.");
                asyncHandler.handle(Future.succeededFuture(helper));
            } else {
                resourceContext.log().error("Translation initialization failed.", response.cause());
                asyncHandler.handle(Future.failedFuture(response.cause()));
            }
        });

        return this;
    }

    @Override
    public TranslationService receiveTranslation(JsonObject translation, Handler<AsyncResult<JsonObject>> asyncHandler) {
        PiveauContext resourceContext = moduleContext.extend(translation.getString("id"));
        resourceContext.log().debug("Incoming translation: {}", translation.toString());
        DCATAPUriRef uriRef = DCATAPUriSchema.applyFor(translation.getString("id"));
        tsConnector.getGraph(uriRef.getDatasetGraphName(), ar -> {
            if (ar.succeeded()) {

                String originalLanguage = translation.getString("original_language");
                JsonObject translations = translation.getJsonObject("translation");

                Model model = ar.result();
                Resource resource = model.getResource(uriRef.getDatasetUriRef());
                addTranslationsToModel(resource, translations, originalLanguage);
                // Updating catalog record with translation informations
                Resource record = model.getResource(uriRef.getRecordUriRef());
                addCatalogRecordDetailsAfterTranslation(record, originalLanguage);

                // Write model back to store and index
                sendTranslationToStore(uriRef, model);
                sendTranslationToIndex(resource, originalLanguage);

                resourceContext.log().info("Translation stored");
                asyncHandler.handle(Future.succeededFuture(new JsonObject().put("status", "success")));
            } else {
                resourceContext.log().error("Translation received: " + ar.cause().getMessage(), ar.cause());
                asyncHandler.handle(Future.failedFuture(ar.cause()));
            }
        });

        return this;
    }

    private void addTranslationsToModel(Resource resource, JsonObject translations, String originalLanguage) {
        removeOldTranslations(resource, originalLanguage);
        for (String language : translations.fieldNames()) {
            String languageTag = this.buildLanguageTag(originalLanguage, language);
            JsonObject attributes = translations.getJsonObject(language);
            for (String attribute : attributes.fieldNames()) {
                if ("title".equals(attribute)) {
                    resource.addProperty(DCTerms.title, attributes.getString(attribute), languageTag);
                } else if ("description".equals(attribute)) {
                    resource.addProperty(DCTerms.description, attributes.getString(attribute), languageTag);
                } else {
                    String distributionId = attribute.substring(0, attribute.length() - 4);
                    String distributionUriRef = DCATAPUriSchema.applyFor(distributionId).getDistributionUriRef();
                    Resource distribution = resource.getModel().getResource(distributionUriRef);
                    if (attribute.endsWith("titl")) {
                        distribution.addProperty(DCTerms.title, attributes.getString(attribute), languageTag);
                    } else if (attribute.endsWith("desc")) {
                        distribution.addProperty(DCTerms.description, attributes.getString(attribute), languageTag);
                    }
                }
            }
        }
    }

    private void removeOldTranslations(Resource resource, String originalLanguage) {
        resetLiteral(resource, DCTerms.title, originalLanguage);
        resetLiteral(resource, DCTerms.description, originalLanguage);

        resource.listProperties(DCAT.distribution)
                .filterKeep(statement -> statement.getObject().isURIResource())
                .mapWith(Statement::getResource)
                .forEachRemaining(distribution -> {
                    resetLiteral(distribution, DCTerms.title, originalLanguage);
                    resetLiteral(distribution, DCTerms.description, originalLanguage);
                });
    }

    private void addCatalogRecordDetailsBeforeTranslation(DatasetHelper helper) {
        Resource record = helper.recordResource();

        record.removeAll(EDP.edpTranslationReceived);
        record.removeAll(EDP.edpTranslationIssued);
        record.addProperty(EDP.edpTranslationIssued, ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS).format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);
        record.removeAll(EDP.edpTranslationStatus);
        record.addProperty(EDP.edpTranslationStatus, EDP.edpTranslationInProcess);
    }

    private void addCatalogRecordDetailsAfterTranslation(Resource record, String originalLanguage) {
        record.removeAll(EDP.edpTranslationReceived);
        record.addProperty(EDP.edpTranslationReceived, ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS).format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);
        record.removeAll(EDP.edpOriginalLanguage);
        record.addProperty(EDP.edpOriginalLanguage, generateLanguageUri(originalLanguage));
        record.removeAll(EDP.edpTranslationStatus);
        record.addProperty(EDP.edpTranslationStatus, EDP.edpTranslationCompleted);

        record.removeAll(DCTerms.modified);
        record.addProperty(DCTerms.modified, ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS).format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);
    }

    private void sendTranslationToStore(DCATAPUriRef uriRef, Model model) {
        tsConnector.putGraph(uriRef.getDatasetGraphName(), model, ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                if (response.statusCode() == 200) {
                    log.debug("Dataset updated with translation information in store successful.");
                } else if (response.statusCode() == 201) {
                    log.debug("Translation created successful.");
                } else {
                    log.error("Put dataset: {}", response.statusMessage());
                }
            } else {
                log.error("Put dataset", ar.cause());
            }
        });
    }

    private void sendTranslationToIndex(Resource resource, String language) {
        tsConnector.query("SELECT ?c WHERE { ?c <http://www.w3.org/ns/dcat#:dataset> <" + resource.getURI() + "> }", "application/json", ar -> {
            if (ar.succeeded()) {
                ResultSet set = ResultSetFactory.fromJSON(new ByteArrayInputStream(ar.result().body().getBytes()));
                if (set.hasNext()) {
                    QuerySolution solution = set.next();
                    RDFNode c = solution.get("c");
                    String catalogueId = DCATAPUriSchema.parseUriRef(c.asResource().getURI()).getId();
                    indexService.addDatasetPut(Indexing.indexingDataset(resource, catalogueId, language), ir -> {
                        if (ir.succeeded()) {
                            log.debug("Successfully send to Index Service");
                        } else {
                            log.error("Dataset could not send to IndexService", ir.cause());
                        }
                    });
                }
            } else {
                log.error("Detecting catalogue id", ar.cause());
            }
        });
    }

    private Future<HttpResponse<Buffer>> sendRequestToTranslationServiceMiddleware(JsonObject requestBody) {
        Promise<HttpResponse<Buffer>> promise = Promise.promise();
        String translationServiceUrl = ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_TRANSLATION_SERVICE).getString("translation_service_url");
        client.postAbs(translationServiceUrl)
                .putHeader("Content-Type", "application/json")
                .sendJsonObject(requestBody, promise);
        return promise.future();
    }

    private JsonObject buildJsonFromResource(DatasetHelper helper) {
        JsonObject requestBody = new JsonObject();

        // Adding original language
        final String originalLanguage = getOriginalLanguage(helper.resource(), helper.sourceLang());

        requestBody.put("original_language", originalLanguage);
        log.debug("Identified original language: " + originalLanguage);

        // Adding languages for translation request
        final Set<String> set = getTranslationLanguages(originalLanguage);
        requestBody.put("languages", new JsonArray(new ArrayList<>(set)));

        // Adding callback parameters
        requestBody.put("callback", getCallbackParameters(helper));

        // Adding data dict to translate
        requestBody.put("data_dict", getDataDict(helper.resource(), originalLanguage));

        return requestBody;
    }

    private JsonObject getDataDict(Resource resource, String originalLanguage) {
        JsonObject dataDict = new JsonObject()
                .put("title", getOriginalText(resource, DCTerms.title, originalLanguage))
                .put("description", getOriginalText(resource, DCTerms.description, originalLanguage));

        resource.listProperties(DCAT.distribution)
                .filterKeep(statement -> statement.getObject().isURIResource())
                .mapWith(Statement::getResource)
                .forEachRemaining(distribution -> {
                    String distId = DCATAPUriSchema.parseUriRef(distribution.getURI()).getId();
                    dataDict.put(distId + "titl", getOriginalText(distribution, DCTerms.title, originalLanguage));
                    dataDict.put(distId + "desc", getOriginalText(distribution, DCTerms.description, originalLanguage));
                });

        return dataDict;
    }

    private JsonObject getCallbackParameters(DatasetHelper helper) {
        JsonObject callbackParameters = new JsonObject();
        String callbackUrl = ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_TRANSLATION_SERVICE).getString("callback_url");
        String callbackAuth = this.config.getString(Constants.ENV_PIVEAU_HUB_API_KEY);

        callbackParameters.put("url", callbackUrl);
        callbackParameters.put("method", "POST");
        JsonObject payload = new JsonObject();

        payload.put("id", DCATAPUriSchema.parseUriRef(helper.uriRef()).getId());

        callbackParameters.put("payload", payload);
        JsonObject headers = new JsonObject();
        headers.put("Authorization", callbackAuth);
        callbackParameters.put("headers", headers);
        return callbackParameters;
    }

    private String getOriginalLanguage(Resource resource, String defaultLang) {

        List<String> available = resource.listProperties(DCTerms.title)
                .filterKeep(statement -> statement.getObject().isLiteral())
                .mapWith(statement -> LanguageTag.parseLangTag(statement.getLanguage(), defaultLang))
                .filterKeep(triple -> !triple.getFirst())
                .mapWith(Triple::getSecond)
                .toList();

        if (!available.isEmpty()) {
            return available.contains(defaultLang) ? defaultLang : available.get(0);
        } else {
            return defaultLang;
        }
    }

    private String getOriginalText(Resource resource, Property text, String language) {
        Statement statement = resource.getProperty(text, language);
        if (statement == null) {
            statement = resource.listProperties(text)
                    .filterDrop(stmt -> stmt.getLanguage().isBlank())
                    .toList()
                    .stream()
                    .findFirst()
                    .orElse(null);
        }
        if (statement != null) {
            return statement.getString();
        } else{
            return "";
        }
    }

    private Set<String> getTranslationLanguages(String originalLanguage) {
        Set<String> translationLanguages = getAcceptedLanguages();
        translationLanguages.remove(originalLanguage);
        return translationLanguages;
    }

    private Set<String> getAcceptedLanguages() {
        List acceptedLanguages = ConfigHelper.forConfig(config).forceJsonObject(Constants.ENV_PIVEAU_TRANSLATION_SERVICE).getJsonArray("accepted_languages").getList();
        return new HashSet<String>(acceptedLanguages);
    }

    private String buildLanguageTag(String originalLanguage, String targetLanguage) {
        if (targetLanguage.equals("nb"))
            targetLanguage = "no";
        return targetLanguage + "-t-" + originalLanguage + "-t0-mtec";
    }

    private Resource generateLanguageUri(String languageCode) {
        String string_uri;
        switch (languageCode) {
            case "en":
                string_uri = "http://publications.europa.eu/resource/authority/language/ENG";
                break;
            case "bg":
                string_uri = "http://publications.europa.eu/resource/authority/language/BUL";
                break;
            case "hr":
                string_uri = "http://publications.europa.eu/resource/authority/language/HRV";
                break;
            case "cs":
                string_uri = "http://publications.europa.eu/resource/authority/language/CES";
                break;
            case "da":
                string_uri = "http://publications.europa.eu/resource/authority/language/DAN";
                break;
            case "nl":
                string_uri = "http://publications.europa.eu/resource/authority/language/NLD";
                break;
            case "et":
                string_uri = "http://publications.europa.eu/resource/authority/language/EST";
                break;
            case "fi":
                string_uri = "http://publications.europa.eu/resource/authority/language/FIN";
                break;
            case "fr":
                string_uri = "http://publications.europa.eu/resource/authority/language/FRA";
                break;
            case "el":
                string_uri = "http://publications.europa.eu/resource/authority/language/ELL";
                break;
            case "hu":
                string_uri = "http://publications.europa.eu/resource/authority/language/HUN";
                break;
            case "ga":
                string_uri = "http://publications.europa.eu/resource/authority/language/GLE";
                break;
            case "it":
                string_uri = "http://publications.europa.eu/resource/authority/language/ITA";
                break;
            case "lv":
                string_uri = "http://publications.europa.eu/resource/authority/language/LAV";
                break;
            case "lt":
                string_uri = "http://publications.europa.eu/resource/authority/language/LIT";
                break;
            case "mt":
                string_uri = "http://publications.europa.eu/resource/authority/language/MLT";
                break;
            case "pl":
                string_uri = "http://publications.europa.eu/resource/authority/language/POL";
                break;
            case "pt":
                string_uri = "http://publications.europa.eu/resource/authority/language/POR";
                break;
            case "ro":
                string_uri = "http://publications.europa.eu/resource/authority/language/RON";
                break;
            case "sk":
                string_uri = "http://publications.europa.eu/resource/authority/language/SLK";
                break;
            case "sl":
                string_uri = "http://publications.europa.eu/resource/authority/language/SLV";
                break;
            case "es":
                string_uri = "http://publications.europa.eu/resource/authority/language/SPA";
                break;
            case "sv":
                string_uri = "http://publications.europa.eu/resource/authority/language/SWE";
                break;
            case "nb":
                string_uri = "http://publications.europa.eu/resource/authority/language/NOB";
                break;
            case "de":
                string_uri = "http://publications.europa.eu/resource/authority/language/DEU";
                break;
            default:
                string_uri = "http://publications.europa.eu/resource/authority/language/ENG";
                break;
        }
        return ResourceFactory.createResource(string_uri);
    }

    private void resetLiteral(Resource resource, Property property, String language) {
        String originalValue = getOriginalText(resource, property, language);
        resource.removeAll(property);
        resource.addProperty(property, originalValue, language);
    }

}

