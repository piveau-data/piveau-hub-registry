package io.piveau.hub.dataobjects;

import io.piveau.hub.util.DataUploadConnector;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.utils.JenaUtils;
import io.piveau.utils.experimental.DCATAPUriRef;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.piveau.vocabularies.vocabulary.SPDX;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.json.JsonObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.util.ResourceUtils;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

@DataObject
public class DatasetHelper {
    private String id;
    private String hash;
    private String catalogueId;

    private String sourceType;
    private String sourceLang;

    private DCATAPUriRef uriSchema;

    private Model model;

    public DatasetHelper(JsonObject json) {
        id = json.getString("id");
        hash = json.getString("hash");
        catalogueId = json.getString("catalogueId");
        sourceType = json.getString("sourceType");
        sourceLang = json.getString("sourceLang");
        String content = json.getString("model");
        if (content != null) {
            model = JenaUtils.read(content.getBytes(), Lang.NTRIPLES.getContentType().toString());
        }
        if (id != null) {
            uriSchema = DCATAPUriSchema.applyFor(id);
        }
    }

    public JsonObject toJson() {
        return new JsonObject()
                .put("id", id)
                .put("hash", hash)
                .put("catalogueId", catalogueId)
                .put("sourceType", sourceType)
                .put("sourceLang", sourceLang)
                .put("model", JenaUtils.write(model, Lang.NTRIPLES));
    }

    private DatasetHelper(String content, String contentType, Handler<AsyncResult<DatasetHelper>> handler) {
        try {
            model = JenaUtils.read(content.getBytes(), contentType);
            hash = JenaUtils.canonicalHash(model);
            extractId();
            handler.handle(Future.succeededFuture(this));
        } catch(Exception e) {
            handler.handle(Future.failedFuture(e));
        }
    }

    private DatasetHelper(String id, String content, String contentType, String hash, String catalogueId, Handler<AsyncResult<DatasetHelper>> handler) {
        this(content, contentType, ar -> {
            if(ar.succeeded()) {
                DatasetHelper helper = ar.result();
                helper.id = id;

                if(helper.uriSchema == null) {
                    helper.uriSchema = DCATAPUriSchema.applyFor(id);
                }

                helper.catalogueId = catalogueId;
                if(hash != null && !hash.isEmpty()) {
                    helper.hash = hash;
                }
                handler.handle(Future.succeededFuture(helper));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });

    }

    public static void create(String content, String contentType, Handler<AsyncResult<DatasetHelper>> handler) {
        new DatasetHelper(content, contentType, handler);
    }

    public static void create(String id, String content, String contentType, String hash, String catalogueId, Handler<AsyncResult<DatasetHelper>> handler) {
        new DatasetHelper(id, content, contentType, hash, catalogueId, handler);
    }

    public String id() {
        return id;
    }

    public String catalogueId() {
        return catalogueId;
    }

    public String hash() {
        return hash;
    }

    public String graphName() {
        return uriSchema.getDatasetGraphName();
    }

    public String uriRef() {
        return uriSchema.getDatasetUriRef();
    }

    public String recordUriRef() {
        return uriSchema.getRecordUriRef();
    }

    public String catalogueUriRef() {
        return DCATAPUriSchema.applyFor(catalogueId).getCatalogueUriRef();
    }

    public String catalogueGraphName() {
        return DCATAPUriSchema.applyFor(catalogueId).getCatalogueGraphName();
    }

    public String validationUriRef() {
        return uriSchema.getValidationUriRef();
    }

    public String validationGraphName() {
        return uriSchema.getValidationGraphName();
    }

    public Resource resource() {
        return model.getResource(uriRef());
    }

    public Resource recordResource() {
        return model.getResource(recordUriRef());
    }

    public void sourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String sourceType() {
        return sourceType;
    }

    public void sourceLang(String sourceLang) {
        this.sourceLang = sourceLang;
    }

    public String sourceLang() {
        return sourceLang;
    }

    public Model model() {
        return model;
    }

    public String stringify(Lang lang) {
        return JenaUtils.write(model, lang);
    }

    public void update(Model oldModel, String recordUriRef) {
        uriSchema = DCATAPUriSchema.applyFor(DCATAPUriSchema.parseUriRef(recordUriRef).getId());

        Model recordModel = JenaUtils.extractResource(oldModel.getResource(recordUriRef));
        Resource record = recordModel.getResource(recordUriRef);
        updateRecord(record, hash);

        Map<String, Resource> dist = savedDistributionIds(oldModel);
        renameReferences(dist);

        extractTranslations(oldModel);

        model.add(recordModel);
    }

    public void init(String normalized) {
        uriSchema = DCATAPUriSchema.applyFor(normalized);
        renameReferences(Collections.emptyMap());
        initRecord(model.createResource(recordUriRef(), DCAT.CatalogRecord));
    }

    public void setAccessURLs(DataUploadConnector dataUploadConnector) {
        model.listResourcesWithProperty(RDF.type, DCAT.Distribution).forEachRemaining(resource -> {
            String distId = DCATAPUriSchema.parseUriRef(resource.getURI()).getId();
            if(!resource.hasProperty(DCAT.accessURL)) {
                resource.addProperty(DCAT.accessURL, model.createResource(dataUploadConnector.getDataURL(distId)));
            }
        });
    }

    public void addDistribution(String content, String contentType, Boolean allowUpdate, Handler<AsyncResult<DatasetHelper>> handler) {
        PiveauLogger log = PiveauLoggerFactory.getLogger(this, getClass());
        try {
            Model distModel = JenaUtils.read(content.getBytes(), contentType);

            Map<String, Resource> distIds = savedDistributionIds(model);
            if(!allowUpdate) {
                String distid = getDistributionIdentifier(content, contentType);
                if(distIds.get(distid) != null) {

                    handler.handle(Future.failedFuture(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 409, "Distribution already exists in Dataset. Use PUT for updating")));
                    return;
                }
            }
            model.add(distModel);

            //Get all distributions in the dist model and add them as DCAT:distribution reference to the Dataset
            distModel.listSubjectsWithProperty(RDF.type).forEachRemaining(s -> {
                s.listProperties(RDF.type).forEachRemaining(p -> {
                    if(p.getObject().equals(DCAT.Distribution)) {
                        model.listSubjectsWithProperty(RDF.type, DCAT.Dataset)
                                .forEachRemaining(ds ->
                                        ds.addProperty(DCAT.distribution, p.getSubject())
                                );
                    }

                });
            });

            renameReferences(distIds);

            hash = DigestUtils.md5Hex(JenaUtils.write(model, Lang.NTRIPLES));
            updateRecord(recordResource(), hash);
            handler.handle(Future.succeededFuture(this));
        } catch(Exception e) {
            handler.handle(Future.failedFuture(e));
        }
    }


    private void initRecord(Resource record) {
        record.addProperty(FOAF.primaryTopic, record.getModel().createResource(uriRef()));
        record.addProperty(DCTerms.created, ZonedDateTime
                .now(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);

        record.addProperty(DCTerms.modified, ZonedDateTime
                .now(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);

        record.addProperty(DCTerms.identifier, id);

        Resource checksum = record.getModel().createResource(SPDX.Checksum);
        checksum.addProperty(SPDX.algorithm, SPDX.checksumAlgorithm_md5);
        checksum.addProperty(SPDX.checksumValue, hash);
        record.addProperty(SPDX.checksum, checksum);
    }

    private void updateRecord(Resource record, String hash) {
        Resource checksum = record.getPropertyResourceValue(SPDX.checksum);
        checksum.removeAll(SPDX.checksumValue);
        checksum.addProperty(SPDX.checksumValue, hash);

        record.removeAll(DCTerms.modified);
        record.addProperty(DCTerms.modified, ZonedDateTime
                .now(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_DATE_TIME), XSDDatatype.XSDdateTime);
    }

    private void renameReferences(Map<String, Resource> identDist) {
        //rename datasets
        model.listSubjectsWithProperty(RDF.type, DCAT.Dataset).forEachRemaining(ds -> ResourceUtils.renameResource(ds, uriRef()));

        //rename distributions
        model.listResourcesWithProperty(RDF.type, DCAT.Distribution).forEachRemaining(resource -> {

            //set the id to lookup in the identDist map
            String id;
            if (resource.hasProperty(DCTerms.identifier)) {
                id = resource.getProperty(DCTerms.identifier).getLiteral().toString();
            } else if (resource.isURIResource()) {
                id = resource.getURI();
            } else if (resource.hasProperty(DCTerms.title)) {
                id = resource.getProperty(DCTerms.title).getLiteral().toString();
            } else {
                id = resource.getProperty(DCAT.accessURL).getResource().getURI();
            }

            //if the Distribution in the new model has an {@link DCTerms#identifier identifier} that was already present in the old model,
            //      give the new Distribution the same id/uriRef as the old distribution
            //else give it a new uriRef
            //
            if(identDist.containsKey(id)) {
                Resource renamed = ResourceUtils.renameResource(resource, identDist.get(id).getURI());
                renamed.addProperty(DCTerms.identifier, id);
            } else {

                String newID = UUID.randomUUID().toString();

                Resource renamed = ResourceUtils.renameResource(resource, DCATAPUriSchema.applyFor(newID).getDistributionUriRef());
                // if there is no {@link DCTerms#identifier identifier}, add a the new one we can use on the next update
                if(!renamed.hasProperty(DCTerms.identifier)) {
                    renamed.addProperty(DCTerms.identifier, id);
                }
            }
        });
    }

    /**
     * This method collects all
     *
     * @param oldModel the old Model, from which the
     * @return A mapping from the distribution identifier to the distribution uri which looks like:
     * Map< Distribution {@link DCTerms#identifier identifier} as string, Map < Distribution Uri, Distribution {@link DCTerms#identifier identifier} as Statement>>
     */
    private Map<String, Resource> savedDistributionIds(Model oldModel) {
        Map<String, Resource> saved = new HashMap<>();
        oldModel.listResourcesWithProperty(RDF.type, DCAT.Distribution).forEachRemaining(dist -> dist.listProperties(DCTerms.identifier).forEachRemaining(id -> {
            if (id.getObject().isLiteral()) {
                saved.put(id.getLiteral().toString(), dist);
            } else if (id.getObject().isResource()) {
                saved.put(id.getResource().getURI(), dist);
            }
        }));
        return saved;
    }

    private void extractId() {
        ResIterator it = model.listSubjectsWithProperty(RDF.type, DCAT.CatalogRecord);
        if(it.hasNext()) {
            Resource record = it.next();
            if(record.getProperty(DCTerms.identifier) != null) {
                id = record.getProperty(DCTerms.identifier).getObject().asLiteral().getString();
            }
        }

        it = model.listSubjectsWithProperty(RDF.type, DCAT.Dataset);
        if(it.hasNext()) {
            Resource dataset = it.next();
            if(dataset.isURIResource()) {
                DCATAPUriRef tmp = DCATAPUriSchema.parseUriRef(dataset.getURI());
                if(tmp != null) {
                    uriSchema = DCATAPUriSchema.applyFor(tmp.getId());
                }
            }
        }
    }

    private String getDistributionIdentifier(String distribution, String contentType) {

        Model model = JenaUtils.read(distribution.getBytes(), contentType);
        String identifier = "";

        ResIterator it = model.listSubjectsWithProperty(RDF.type, DCAT.Distribution);
        if(it.hasNext()) {
            Resource dist = it.next();

            identifier = JenaUtils.findIdentifier(dist);
            if(identifier == null && dist.isURIResource()) {
                identifier = dist.getURI();
            }
        }
        return identifier;
    }


    private void extractTranslations(Model oldModel) {
        extractTranslationsFromResource(oldModel.getResource(uriRef()), resource(), DCTerms.title);
        extractTranslationsFromResource(oldModel.getResource(uriRef()), resource(), DCTerms.description);

        model.listResourcesWithProperty(RDF.type, DCAT.Distribution).forEachRemaining(dist -> {
            Resource oldDist = oldModel.getResource(dist.getURI());
            if (oldDist != null) {
                extractTranslationsFromResource(oldDist, dist, DCTerms.title);
                extractTranslationsFromResource(oldDist, dist, DCTerms.description);
            }
        });
    }

    private void extractTranslationsFromResource(Resource oldResource, Resource newResource, Property property) {
        oldResource.listProperties(property).filterKeep(pred -> pred.getLanguage().contains("mtec")).forEachRemaining(stm -> newResource.addLiteral(property, stm.getLiteral()));
    }

    private void extractDistributions(Resource oldResource, Resource newResource, Model translationModel) {
        StmtIterator iterator = oldResource.listProperties(DCAT.distribution);
        while(iterator.hasNext()) {
            Statement statement = iterator.nextStatement();
            if(statement.getSubject().isResource()) {
                Resource distribution = statement.getResource();
                String distributionUri = distribution.getURI();
                Resource newDistribution = translationModel.createResource(distributionUri);
                newResource.addProperty(DCAT.distribution, newDistribution);
                this.extractTranslationsFromResource(distribution, newDistribution, DCTerms.title);
                this.extractTranslationsFromResource(distribution, newDistribution, DCTerms.description);
            }
        }
    }
}
