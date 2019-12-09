package io.piveau.hub.converters;

import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.util.rdf.*;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.RDF;

/**
 * Converts a Dataset Jena Model to JSON for the Search Index
 * ToDo Just a first version, should be more generic
 */
public class DatasetToIndexConverter {

    private PropertyParser propertyParser = new PropertyParser();

    public JsonObject convert(Model model, String uriRef, String catalogueId) {
        ResIterator it = model.listResourcesWithProperty(RDF.type, DCAT.CatalogRecord);
        if (it.hasNext()) {
            return convert(model.getResource(uriRef), it.next(), catalogueId);
        } else {
            return new JsonObject();
        }
    }

    public JsonObject convert2(DatasetHelper helper) {
        return convert(helper.resource(), helper.recordResource(), helper.catalogueId());
    }

    private JsonObject convert(Resource res, Resource recordRes, String catalogueId) {
        JsonObject result = new JsonObject();

        JsonObject translationMeta = new JsonObject();

        String normalizedId = DCATAPUriSchema.parseUriRef(res.getURI()).getId();
        result.put("id", normalizedId);
        result.put("idName", normalizedId);

        JsonArray messages =  new JsonArray();

        try {
            JsonObject description = propertyParser.getMTECDctDescription(res);
            result.put("description", description.getJsonObject("payload"));
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            JsonObject title = propertyParser.getMTECDctTitle(res);
            result.put("title", title.getJsonObject("payload"));
            JsonObject transInfo = propertyParser.getEdpTransInfo(recordRes);

            if(!title.getJsonObject("meta").isEmpty() && !transInfo.isEmpty()) {
                translationMeta.put("details", title.getJsonObject("meta"));

                JsonArray fullAvailable = new JsonArray();
                JsonObject details = translationMeta.getJsonObject("details");
                if (details != null) {
                    for (String language :  details.fieldNames()) {
                        // Todo This is not 100% correct
                        fullAvailable.add(language);
                        JsonObject langObject =  details.getJsonObject(language);
                        if(langObject != null && langObject.getBoolean("machine_translated", false)) {
                            langObject.mergeIn(transInfo);
                            langObject.remove("status");
                        }
                    }
                }
                translationMeta.put("status", transInfo.getString("status"));
                translationMeta.put("full_available_languages", fullAvailable);
            }
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        JsonArray categories = propertyParser.getDcatTheme(res);
        if (!categories.isEmpty()) {
            result.put("categories", categories);
        }

        try {
            JsonArray languages = propertyParser.getDctLanguage(res);
            result.put("languages", languages);
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            JsonArray contactPoint = propertyParser.getDcatContactPoint(res);
            result.put("contact_points", contactPoint);
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            JsonArray distributions = propertyParser.getDcatDistributions(res);
            result.put("distributions", distributions);
        } catch (PropertyNotAvailableException e) {
            result.put("distributions", new JsonArray());
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            JsonArray keywords = propertyParser.getDcatKeyword(res);
            result.put("keywords", keywords);
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            JsonObject spatial = propertyParser.getDctSpatialForDataset(res);
            result.put("spatial", spatial);
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            String accessRight = propertyParser.getDctAccssRights(res);
            result.put("access_right", accessRight);
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            JsonArray conformsTo = propertyParser.getDctConformsTo(res);
            result.put("conforms_to", conformsTo);
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            JsonArray documentation = propertyParser.getFoafPage(res);
            result.put("documentations", documentation);
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            String releaseDate = propertyParser.getDctIssued(res);
            result.put("release_date", releaseDate);
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            String modDate = propertyParser.getDctModified(res);
            result.put("modification_date", modDate);
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            JsonObject publisher = propertyParser.getDctPublisher(res);
            result.put("publisher", publisher);
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            JsonArray landingPage = propertyParser.getDcatLandingPage(res);
            result.put("landing_page", landingPage);
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        try {
            JsonArray provenance = propertyParser.getDctProvenance(res);
            result.put("provenances", provenance);
        } catch (PropertyNotAvailableException e) {
            if(e.getMessage() != null) {
                messages.add(e.getMessage());
            }
        }

        result.put("catalog", new JsonObject().put("id", catalogueId));

        if(translationMeta.getJsonObject("details") != null && !translationMeta.getJsonObject("details").isEmpty()) {
            result.put("translation_meta", translationMeta);
        }

        return result;
    }

}
