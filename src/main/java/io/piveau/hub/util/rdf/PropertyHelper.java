package io.piveau.hub.util.rdf;

import io.piveau.hub.converters.DatasetToIndexConverter;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;

public class PropertyHelper {

    private DateTimeUtil dateTimeUtil;



    public PropertyHelper() {
        this.dateTimeUtil = new DateTimeUtil();
    }

    public String getSingleLiteral(Resource resource, Property property) throws PropertyNotAvailableException {
        String result;
        try {
            if(resource.getProperty(property) == null) {
                throw new PropertyNotAvailableException(property.toString() + " is not set");
            }
            if(!resource.getProperty(property).getObject().isLiteral()) {
                throw new PropertyNotAvailableException(property.toString() + " is not a Literal");
            }
            result = resource.getProperty(property).getLiteral().getString();
        } catch (RuntimeException e) {
            throw new PropertyNotAvailableException(e.getMessage());
        }
        return result;
    }

    public String getStringFromLiteralOrResource(Resource resource, Property property) throws PropertyNotAvailableException {
        try {
            Statement stmt = resource.getProperty(property);
            if(stmt == null) {
                throw new PropertyNotAvailableException(property.toString() + " is not set");
            }
            else if(stmt.getObject().isLiteral()) {
                return stmt.getLiteral().getString();
            }
            else if(stmt.getObject().isResource() && !stmt.getObject().isAnon()) {
                return stmt.getResource().getURI();
            } else {
                throw new PropertyNotAvailableException(property.toString() + " is neither Literal or Resource");
            }
        } catch (RuntimeException e) {
            throw new PropertyNotAvailableException(e.getMessage());
        }
    }


    public List<String> getSingleLiterals(Resource resource, Property property) {
        List<String> result = new ArrayList<>();
        StmtIterator stmtIterator = resource.listProperties(property);
        while(stmtIterator.hasNext()) {
            Statement stmt = stmtIterator.nextStatement();
            if(stmt.getObject().isLiteral()){
                result.add(stmt.getLiteral().getString());
            }
        }
        return result;
    }


    public JsonObject getMultilingualLiteral(Resource resource, Property property) throws PropertyNotAvailableException {
        JsonObject result = new JsonObject();
        PiveauLogger LOGGER = PiveauLoggerFactory.getLogger(getClass());

        try {
            StmtIterator stmtIterator = resource.listProperties(property);
            if(!stmtIterator.hasNext()) {
                throw new PropertyNotAvailableException(property.toString() + " not set.");
            }
            while(stmtIterator.hasNext()) {
                Statement stmt = stmtIterator.nextStatement();
                if(!stmt.getObject().isLiteral()) {
                    LOGGER.warn("Found value for " + property.toString() + ", which is not a literal.");
                } else {
                    Literal literal = stmt.getLiteral();
                    String lang = literal.getLanguage();
                    if(!lang.isEmpty()) {
                        result.put(lang, literal.getString());
                    } else {
                        result.put("en", literal.getString());
                    }
                }
            }
        } catch (RuntimeException e) {
            throw new PropertyNotAvailableException(e.getMessage());
        }
        if(result.isEmpty()) {
            throw new PropertyNotAvailableException(property.toString() + " not set.");
        }

        return result;
    }

    public JsonObject getMTECMultilingualLiteral(Resource resource, Property property) throws PropertyNotAvailableException {

        PiveauLogger LOGGER = PiveauLoggerFactory.getLogger(getClass());

        JsonObject result = new JsonObject();
        result.put("payload", new JsonObject());
        result.put("meta", new JsonObject());

        try {
            StmtIterator stmtIterator = resource.listProperties(property);
            if(!stmtIterator.hasNext()) {
                throw new PropertyNotAvailableException(property.toString() + " not set.");
            }
            while(stmtIterator.hasNext()) {
                Statement stmt = stmtIterator.nextStatement();
                if(!stmt.getObject().isLiteral()) {
                    LOGGER.warn("Found value for " + property.toString() + ", which is not a literal.");
                } else {
                    Literal literal = stmt.getLiteral();
                    String lang = literal.getLanguage();
                    if(!lang.isEmpty()) {
                        if(lang.contains("mtec")) {
                            JsonObject meta = parseMTECString(lang);
                            result.getJsonObject("payload").put(meta.getString("lang"), literal.getString());
                            result.getJsonObject("meta").put(meta.getString("lang"),
                                    new JsonObject().
                                            put("machine_translated", true).
                                            put("original_language", meta.getString("original")));
                        } else {
                            result.getJsonObject("payload").put(lang, literal.getString());
                            result.getJsonObject("meta").put(lang, new JsonObject().put("machine_translated", false));
                        }
                    } else {
                        result.getJsonObject("payload").put("en", literal.getString());
                        result.getJsonObject("meta").put("en", new JsonObject().put("machine_translated", false));
                    }
                }
            }
        } catch (RuntimeException e) {
            throw new PropertyNotAvailableException(e.getMessage());
        }
        if( result.getJsonObject("payload").isEmpty()) {
            throw new PropertyNotAvailableException(property.toString() + " not set.");
        }

        return result;
    }

    private JsonObject parseMTECString(String string) {
        if(string.length() == 13) {
            String lang = string.substring(0,2);
            String original = string.substring(6,8);
            return new JsonObject().put("original", original).put("lang", lang);
        } else {
            String lang = string.substring(0,2);
            String original = string.substring(5,7);
            return new JsonObject().put("original", original).put("lang", lang);
        }
    }

    public String getEnglishLiteral(Resource resource, Property property) throws PropertyNotAvailableException {
        PiveauLogger LOGGER = PiveauLoggerFactory.getLogger(getClass());

        try {
            StmtIterator stmtIterator = resource.listProperties(property);
            if(!stmtIterator.hasNext()) {
                throw new PropertyNotAvailableException(property.toString() + " not set.");
            }
            while(stmtIterator.hasNext()) {
                Statement stmt = stmtIterator.nextStatement();
                if(!stmt.getObject().isLiteral()) {
                    LOGGER.warn("Found value for " + property.toString() + ", which is not a literal.");
                } else {
                    Literal literal = stmt.getLiteral();
                    String lang = literal.getLanguage();
                    if(lang.equals("en")) {
                        return literal.getString();
                    }
                }
            }
        } catch (RuntimeException e) {
            throw new PropertyNotAvailableException(e.getMessage());
        }
        return null;
    }


    public void getProperty(Resource res, Property prop, Handler<Statement> handler) {
        if(res.getProperty(prop) != null    ) {
            handler.handle(res.getProperty(prop));
        }
    }

    public Resource getPropertyAsResource(Resource resource, Property property) throws PropertyNotAvailableException  {
        Statement stmt = resource.getProperty(property);
        if(stmt != null) {
            if(stmt.getObject().isResource()) {
                return stmt.getResource();
            } else {
                throw new PropertyNotAvailableException(property.toString() + " is not a resource");
            }
        } else {
            throw new PropertyNotAvailableException(property.toString() + " is not defined");
        }
    }

    public Resource getStatementAsResource(Statement statement) throws PropertyNotAvailableException  {
        if(statement.getObject().isResource()) {
            return statement.getResource();
        } else {
            throw new PropertyNotAvailableException(statement.toString() + " is not a resource");
        }
    }

    public StmtIterator getPropertiesAsStmtIterator(Resource resource, Property property) throws PropertyNotAvailableException  {
        StmtIterator stmtIterator = resource.listProperties(property);
        if(stmtIterator.hasNext()) {
            return stmtIterator;
        } else {
            throw new PropertyNotAvailableException(property.toString() + " is not set");
        }
    }

    public JsonArray getURLsFromResource(Resource resource, Property property)  throws PropertyNotAvailableException  {
        JsonArray result = new JsonArray();
        StmtIterator stmtIterator = getPropertiesAsStmtIterator(resource, property);
        while(stmtIterator.hasNext()) {
            Statement stmt = stmtIterator.nextStatement();
            RDFNode node = stmt.getObject();
            if(node.isResource() && !node.isAnon()) {
                result.add(node.asResource().getURI());
            }
        }
        if(result.isEmpty()) {
            throw new PropertyNotAvailableException(property.toString() + " is not set");
        }
        return result;
    }

    public Resource getResourceFromVocabulary(Resource resource) {
        return VocabularyManager.model.containsResource(resource) ? VocabularyManager.model.getResource(resource.getURI()) : null;
    }

    public JsonObject guessTitleAndResource(RDFNode node, String customLabel)  {
        JsonObject result = new JsonObject();
        String label = customLabel != null ? customLabel : "title";

        if(node.isResource()) {
            String title = null;
            Resource res = node.asResource();

            Statement stmt = res.getProperty(RDFS.label);
            if(stmt != null && stmt.getObject().isLiteral()) {
                title = stmt.getLiteral().getString();
            } else {
                stmt = res.getProperty(EUVOC.skosPrefLabel);
                if(stmt != null && stmt.getObject().isLiteral()) {
                    title = stmt.getLiteral().getString();
                }
            }

            if(title != null) {
                result.put(label, title);
                result.put("resource", res.isAnon() ? "" : res.getURI());
            }

        } else if(node.isLiteral()) {
            result.put(label, node.asLiteral().getString());
            result.put("resource", "");
        }

        return result;
    }


    public String getDateTime(Resource resource, Property property) throws PropertyNotAvailableException  {
        String rawDateTime = getSingleLiteral(resource, property);
        String formattedDate = dateTimeUtil.parse(rawDateTime);
        if(formattedDate != null) {
            return formattedDate;
        } else {
            throw new PropertyNotAvailableException("Could not parse date " + rawDateTime);
        }
    }


    public String generateID(String name) {
        //normalize
        name = Normalizer.normalize(name, Normalizer.Form.NFKD);
        //remove all '%'
        //replace non-word-characters with '-'
        //then combine multiple '-' into one
        return name.replaceAll("%", "").replaceAll("\\W", "-").replaceAll("-+", "-").toLowerCase();
    }

    public String extractLabelFromURI(String uri){
        int idx = uri.lastIndexOf("/");
        if (idx != -1 && idx < (uri.length() - 1)) {
            return uri.substring(idx + 1).trim();
        } else {
            return null;
        }
    }

}
