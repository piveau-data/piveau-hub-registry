package io.piveau.hub.converters;

import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.hub.util.rdf.DateTimeUtil;
import io.piveau.hub.util.rdf.PropertyNotAvailableException;
import io.piveau.hub.util.rdf.PropertyParser;
import io.piveau.utils.JenaUtils;
import io.piveau.utils.experimental.DCATAPUriSchema;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogToIndexConverter {




    private PropertyParser propertyParser;
    private DateTimeUtil dateTimeUtil = new DateTimeUtil();

    public CatalogToIndexConverter() {
        this.propertyParser = new PropertyParser();
    }

    public JsonObject convert(Model model, String id) {
        PiveauLogger LOGGER = PiveauLoggerFactory.getCatalogueLogger(id,getClass());
        JsonObject result = new JsonObject();
        Resource res = model.getResource(DCATAPUriSchema.applyFor(id).getCatalogueUriRef());
        result.put("id", JenaUtils.normalize(id));
        result.put("idName", JenaUtils.normalize(id));

        try {
            JsonObject title = propertyParser.getDctTitle(res);
            result.put("title", title);
        } catch (PropertyNotAvailableException e) {
            LOGGER.debug(e.getMessage());
        }

        try {
            JsonObject description = propertyParser.getDctDescription(res);
            result.put("description", description);
        } catch (PropertyNotAvailableException e) {
            LOGGER.debug(e.getMessage());
        }

        try {
            JsonObject publisher = propertyParser.getDctPublisher(res);
            result.put("publisher", publisher);
        } catch (PropertyNotAvailableException e) {
            LOGGER.debug(e.getMessage());
        }

        try {
            JsonArray languages = propertyParser.getDctLanguage(res);
            result.put("languages", languages);
        } catch (PropertyNotAvailableException e) {
            LOGGER.debug(e.getMessage());
        }

        try {
            JsonObject country = propertyParser.getDctSpatial(res);
            result.put("country", country);
        } catch (PropertyNotAvailableException e) {
            LOGGER.debug(e.getMessage());
        }

        result.put("issued", dateTimeUtil.now());
        result.put("modified", dateTimeUtil.now());

        return result;
    }

}
