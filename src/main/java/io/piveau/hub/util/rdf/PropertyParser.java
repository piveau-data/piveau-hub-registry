package io.piveau.hub.util.rdf;

import io.piveau.utils.JenaUtils;
import io.piveau.vocabularies.Concept;
import io.piveau.vocabularies.ConceptSchemes;
import io.piveau.vocabularies.DataTheme;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;

import java.util.List;


/**
 * Parser for retrieving DCAT-AP properties for humans and plain machines
 * For details refer to DCAT-AP specs version 1.2
 */
public class PropertyParser {

    private PropertyHelper propertyHelper = new PropertyHelper();

    /**
     * dct:description
     * Range: rdfs:Literal
     * Used in: Dataset, Distribution, Catalogue
     *
     * @param resource
     * @return
     */
    public JsonObject getDctDescription(Resource resource) throws PropertyNotAvailableException {
        return propertyHelper.getMultilingualLiteral(resource, DCATAP.dctDescription);
    }

    /**
     * dct:description
     * Range: rdfs:Literal
     * Used in: Dataset, Distribution, Catalogue
     *
     * @param resource
     * @return
     */
    public JsonObject getMTECDctDescription(Resource resource) throws PropertyNotAvailableException {
        return propertyHelper.getMTECMultilingualLiteral(resource, DCATAP.dctDescription);
    }

    /**
     * dct:title
     * Range: rdfs:Literal
     * Used in: Dataset, Distribution, Catalogue
     *
     * @param resource
     * @return
     */
    public JsonObject getDctTitle(Resource resource) throws PropertyNotAvailableException {
        return propertyHelper.getMultilingualLiteral(resource, DCATAP.dctTitle);
    }

    /**
     * dct:title
     * Range: rdfs:Literal
     * Used in: Dataset, Distribution, Catalogue
     *
     * @param resource
     * @return
     */
    public JsonObject getMTECDctTitle(Resource resource) throws PropertyNotAvailableException {
        return propertyHelper.getMTECMultilingualLiteral(resource, DCATAP.dctTitle);
    }


    /**
     * dcat:contactPoint
     * Range: vcard:Kind
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonArray getDcatContactPoint(Resource resource) throws PropertyNotAvailableException {
        JsonArray result = new JsonArray();
        StmtIterator stmtIterator = propertyHelper.getPropertiesAsStmtIterator(resource, DCATAP.dcatContactPoint);

        while (stmtIterator.hasNext()) {
            JsonObject contactPointResult = new JsonObject();
            Statement stmn = stmtIterator.nextStatement();
            Resource contactPoint = propertyHelper.getStatementAsResource(stmn);

            try {
                Resource type = propertyHelper.getPropertyAsResource(contactPoint, DCATAP.rdfType);
                contactPointResult.put("type", type.getLocalName());
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            try {
                String name = propertyHelper.getSingleLiteral(contactPoint, DCATAP.vcardFn);
                contactPointResult.put("name", name);
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            try {
                Resource email = propertyHelper.getPropertyAsResource(contactPoint, DCATAP.vcardHasEmail);
                contactPointResult.put("email", email.getURI());
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            if (!contactPointResult.isEmpty()) {
                result.add(contactPointResult);
            }

        }
        if (result.isEmpty()) {
            throw new PropertyNotAvailableException(DCATAP.dcatContactPoint.toString() + " is not properly set");
        }
        return result;
    }

    /**
     * dcat:distribution
     * Range: dcat:Distribution
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonArray getDcatDistributions(Resource resource) throws PropertyNotAvailableException {
        JsonArray result = new JsonArray();
        StmtIterator stmtIterator = propertyHelper.getPropertiesAsStmtIterator(resource, DCATAP.dcatDistribution);
        while (stmtIterator.hasNext()) {
            JsonObject distroResult = new JsonObject();
            Statement stmn = stmtIterator.nextStatement();
            Resource distribution = propertyHelper.getStatementAsResource(stmn);

            try {
                distroResult.put("id", propertyHelper.extractLabelFromURI(distribution.getURI()));
            } catch (RuntimeException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            try {
                distroResult.put("title", this.getMTECDctTitle(distribution).getJsonObject("payload"));
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            try {
                distroResult.put("description", this.getMTECDctDescription(distribution).getJsonObject("payload"));
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            try {
                distroResult.put("format", this.getDctFormat(distribution));
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            try {
                distroResult.put("access_url", this.getDcatAccessUrl(distribution));
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            try {
                distroResult.put("licence", this.getDctLicense(distribution));
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            try {
                distroResult.put("download_urls", this.getDcatDownloadUrl(distribution));
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            try {
                distroResult.put("media_type", this.getDcatMediaType(distribution));
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            if (!distroResult.isEmpty()) {
                result.add(distroResult);
            }
        }

        if (result.isEmpty()) {
            throw new PropertyNotAvailableException(DCATAP.dcatDistribution.toString() + " is not properly set");
        }

        return result;
    }


    /**
     * dcat:keyword
     * Range: rdfs:Literal
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonArray getDcatKeyword(Resource resource) throws PropertyNotAvailableException {
        JsonArray result = new JsonArray();
        List<String> keywords = propertyHelper.getSingleLiterals(resource, DCATAP.dcatKeyword);
        if (!keywords.isEmpty()) {
            keywords.forEach(keyword -> {
                result.add(new JsonObject()
                        .put("id", JenaUtils.normalize(keyword))
                        .put("title", keyword));
            });
        }
        if (result.isEmpty()) {
            throw new PropertyNotAvailableException(DCATAP.dcatKeyword.toString() + " is not properly set");
        }
        return result;
    }

    /**
     * dct:publisher
     * Range: foaf:Agent
     * Used in: Dataset, Catalogue
     *
     * @param resource
     * @return
     */
    public JsonObject getDctPublisher(Resource resource) throws PropertyNotAvailableException {
        JsonObject result = new JsonObject();
        Resource publisher = propertyHelper.getPropertyAsResource(resource, DCATAP.dctPublisher);
        Resource vocResource = propertyHelper.getResourceFromVocabulary(publisher);

        if (vocResource == null) {
            try {
                Resource type = propertyHelper.getPropertyAsResource(publisher, DCATAP.rdfType);
                result.put("type", type.getLocalName());
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            try {
                String name = propertyHelper.getSingleLiteral(publisher, DCATAP.foafName);
                result.put("name", name);
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            //ToDo Change search service to homepage here
            try {
                Resource homepage = propertyHelper.getPropertyAsResource(publisher, DCATAP.foafHomepage);
                result.put("email", homepage.getURI());
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }
        } else {
            result.put("type", "Organisation");

            try {
                String name = propertyHelper.getEnglishLiteral(vocResource, SKOS.prefLabel);
                result.put("name", name);
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            try {
                Resource homepage = propertyHelper.getPropertyAsResource(vocResource, DCATAP.foafHomepage);
                result.put("email", homepage.getURI());
            } catch (PropertyNotAvailableException e) {
                //throw new PropertyNotAvailableException(e.getMessage());
            }

            result.put("resource", vocResource.getURI());
        }

        if (result.isEmpty()) {
            throw new PropertyNotAvailableException(DCATAP.dctPublisher.toString() + " is not properly set");
        }
        return result;
    }

    /**
     * dcat:theme
     * Range: skos:Concept
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonArray getDcatTheme(Resource resource) {
        JsonArray result = new JsonArray();
        resource.listProperties(DCATAP.dcatTheme).forEachRemaining(stmt -> {
            JsonObject themeResult = (JsonObject) stmt.getObject().visitWith(new RDFVisitor() {
                @Override
                public Object visitBlank(Resource resource, AnonId anonId) {
                    JsonObject result = new JsonObject();
                    if (ConceptSchemes.isA(resource, SKOS.Concept)) {
                        Statement statement = resource.getProperty(DCTerms.identifier);
                        if (statement.getObject().isLiteral()) {
                            Concept concept = DataTheme.INSTANCE.getConcept(statement.getString());
                            if (concept != null) {
                                result.put("id", concept.getIdentifier()).put("title", concept.label("en"));
                            }
                        }
                    }
                    return result;
                }

                @Override
                public Object visitURI(Resource resource, String s) {
                    JsonObject result = new JsonObject();
                    if (DataTheme.INSTANCE.isConcept(resource)) {
                        Concept theme = DataTheme.INSTANCE.getConcept(resource);
                        result.put("id", theme.getIdentifier()).put("title", theme.label("en"));
                    }
                    return result;
                }

                @Override
                public Object visitLiteral(Literal literal) {
                    JsonObject result = new JsonObject();
                    Concept theme = DataTheme.INSTANCE.getConcept(literal.getLexicalForm());
                    if (theme != null) {
                        result.put("id", theme.getIdentifier()).put("title", theme.label("en"));
                    }
                    return result;
                }
            });
            if (!themeResult.isEmpty()) {
                result.add(themeResult);
            }
        });
        return result;
    }

    /**
     * dct:accessRights
     * Range: dct:RightsStatement
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public String getDctAccssRights(Resource resource) throws PropertyNotAvailableException {
        return propertyHelper.getStringFromLiteralOrResource(resource, DCATAP.dctAccessRights);
    }

    /**
     * dct:conformsTo
     * Range: dct:Standard
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonArray getDctConformsTo(Resource resource) throws PropertyNotAvailableException {
        JsonArray result = new JsonArray();
        StmtIterator stmtIterator = propertyHelper.getPropertiesAsStmtIterator(resource, DCATAP.dctConformsTo);
        while (stmtIterator.hasNext()) {
            Statement stmt = stmtIterator.nextStatement();
            JsonObject item = propertyHelper.guessTitleAndResource(stmt.getObject(), null);
            if (!item.isEmpty()) {
                result.add(item);
            }
        }
        if (result.isEmpty()) {
            throw new PropertyNotAvailableException(DCATAP.dctConformsTo.toString() + " is not properly set");
        }
        return result;
    }

    /**
     * foaf:Page
     * Range: foaf:Document
     * Used in: Dataset, Distribution
     *
     * @param resource
     * @return
     */
    public JsonArray getFoafPage(Resource resource) throws PropertyNotAvailableException {
        JsonArray result = new JsonArray();
        StmtIterator stmtIterator = propertyHelper.getPropertiesAsStmtIterator(resource, DCATAP.foafPage);
        while (stmtIterator.hasNext()) {
            Statement statement = stmtIterator.nextStatement();
            if (statement.getObject().isResource() && !statement.getObject().isAnon()) {
                result.add(statement.getResource().getURI());
            }
        }

        if (result.isEmpty()) {
            throw new PropertyNotAvailableException(DCATAP.foafPage.toString() + " is not properly set");
        }
        return result;
    }

    /**
     * dct:accrualPeriodicity
     * Range: dct:Frequency
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getDctAccrualPeriodicity(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dct:hasVersion
     * Range: dcat:Dataset
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getDctHasVersion(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dct:identifier
     * Range: rdfs:Literal
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getDctIdentifier(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dct:isVersionOf
     * Range: dcat:Dataset
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getDctIsVersionOf(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dcat:LandingPage
     * Range: foaf:Document
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonArray getDcatLandingPage(Resource resource) throws PropertyNotAvailableException {
        return propertyHelper.getURLsFromResource(resource, DCATAP.dcatLandingPage);
    }

    /**
     * dct:Language
     * Range: dct:LinguisticSystem
     * Used in: Dataset, Distribution, Catalogue
     *
     * @param resource
     * @return
     */
    public JsonArray getDctLanguage(Resource resource) throws PropertyNotAvailableException {
        JsonArray result = new JsonArray();
        try {
            StmtIterator stmtIterator = resource.listProperties(DCATAP.dctLanguage);
            if (!stmtIterator.hasNext()) {
                throw new PropertyNotAvailableException(DCATAP.dctLanguage.toString() + " not set");
            }
            while (stmtIterator.hasNext()) {
                Statement stmt = stmtIterator.nextStatement();
                if (stmt.getObject().isResource()) {
                    Resource res = propertyHelper.getResourceFromVocabulary(stmt.getResource());
                    StmtIterator stms = res.listProperties(EUVOC.atOpMappedCode);
                    while (stms.hasNext()) {
                        RDFNode rdfNode = stms.nextStatement().getObject();
                        String h = rdfNode.asResource().getProperty(EUVOC.dcSource).getString();
                        if (h.equals("iso-639-1")) {
                            result.add(rdfNode.asResource().getProperty(EUVOC.atLegacyCode).getString());
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            throw new PropertyNotAvailableException(e.getMessage());
        }
        if (result.isEmpty()) {
            throw new PropertyNotAvailableException("No valid " + DCATAP.dctLanguage.toString() + " is set");
        }
        return result;
    }

    /**
     * adms:identifier
     * Range: adms:Identifier
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getAdmsIdentifier(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dct:provenance
     * Range: dct:ProvenanceStatement
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonArray getDctProvenance(Resource resource) throws PropertyNotAvailableException {
        JsonArray result = new JsonArray();
        StmtIterator stmtIterator = propertyHelper.getPropertiesAsStmtIterator(resource, DCATAP.dctProvenance);
        while (stmtIterator.hasNext()) {
            Statement stmt = stmtIterator.nextStatement();
            JsonObject item = propertyHelper.guessTitleAndResource(stmt.getObject(), "label");
            if (!item.isEmpty()) {
                result.add(item);
            }
        }
        if (result.isEmpty()) {
            throw new PropertyNotAvailableException(DCATAP.dctProvenance.toString() + " is not properly set");
        }
        return result;
    }

    /**
     * dct:relation
     * Range: rdfs:Resource
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getDctRelation(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dct:issued
     * Range: rdfs:Literal (xsd:date, xsd:dateTime
     * Used in: Dataset, Distribution, Catalgue
     *
     * @param resource
     * @return
     */
    public String getDctIssued(Resource resource) throws PropertyNotAvailableException {
        return propertyHelper.getDateTime(resource, DCATAP.dctIssued);
    }

    /**
     * dct:modified
     * Range: rdfs:Literal (xsd:date, xsd:dateTime
     * Used in: Dataset, Distribution, Catalogue
     *
     * @param resource
     * @return
     */
    public String getDctModified(Resource resource) throws PropertyNotAvailableException {
        return propertyHelper.getDateTime(resource, DCATAP.dctModified);
    }

    /**
     * adms:sample
     * Range: dcat:Distribution
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getAdmsSample(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dct:source
     * Range: dcat:Dataset
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getDctSource(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dct:spatial
     * Range: dct:Location
     * Used in: Dataset, Catalogue
     *
     * @param resource
     * @return
     */
    public JsonObject getDctSpatial(Resource resource) throws PropertyNotAvailableException {
        JsonObject result = new JsonObject();
        Resource spatialResource = propertyHelper.getPropertyAsResource(resource, DCATAP.dctSpatial);

        // Todo Temporary Hack
        if (spatialResource.getURI().equals("http://publications.europa.eu/resource/authority/continent/EUROPE")) {
            result.put("title", "Europe");
            result.put("id", "eu");
            return result;
        }

        // Todo Temporary Hack
        try {
            if (spatialResource.getURI().contains("place")) {
                String tempUri = spatialResource.getURI();
                tempUri = tempUri.substring(tempUri.length() - 7, tempUri.length() - 4);
                spatialResource = ModelFactory.createDefaultModel().createResource("http://publications.europa.eu/resource/authority/country/" + tempUri);
            }
        } catch (RuntimeException e) {
            throw new PropertyNotAvailableException(e.getMessage());
        }

        Resource spatial = propertyHelper.getResourceFromVocabulary(spatialResource);
        if (spatial == null) {
            throw new PropertyNotAvailableException("Spatial not in vocabulary.");
        }
        //result.put("id", propertyHelper.getSingleLiteral(spatial, EUVOC.dcIdentifier));

        StmtIterator stmtIterator = propertyHelper.getPropertiesAsStmtIterator(spatial, EUVOC.skosPrefLabel);
        while (stmtIterator.hasNext()) {
            Statement stmt = stmtIterator.nextStatement();
            Literal literal = stmt.getLiteral();
            if (literal.getLanguage().equals("en")) {
                result.put("title", literal.getString());
            }
        }

        stmtIterator = propertyHelper.getPropertiesAsStmtIterator(spatial, EUVOC.euvocXlNotation);
        while (stmtIterator.hasNext()) {
            Statement stmt = stmtIterator.nextStatement();
            Resource notation = stmt.getResource();
            Resource notationType = notation.getPropertyResourceValue(DCATAP.dctType);
            if (notationType.equals(EUVOC.EuvocAlpha2)) {
                result.put("id", notation.getProperty(EUVOC.euvocXlCodification).getString().toLowerCase());
            }
        }

        return result;
    }

    /**
     * dct:spatial
     * Range: dct:Location
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getDctSpatialForDataset(Resource resource) throws PropertyNotAvailableException {
        Resource spatialResource = propertyHelper.getPropertyAsResource(resource, DCATAP.dctSpatial);
        StmtIterator stmtIterator = propertyHelper.getPropertiesAsStmtIterator(spatialResource, EDP.locnGeometry);
        GeoParser geoParser = new GeoParser();
        while (stmtIterator.hasNext()) {
            Statement statement = stmtIterator.nextStatement();
            if (statement.getObject().isLiteral()) {
                Literal literal = statement.getLiteral();
                String datatype = literal.getDatatypeURI();
                if (datatype.equals("https://www.iana.org/assignments/media-types/application/vnd.geo+json")) {
                    try {
                        return new JsonObject(literal.getString());
                    } catch (RuntimeException e) {
                        throw new PropertyNotAvailableException(e.getMessage());
                    }
                } else if (datatype.equals("http://www.openlinksw.com/schemas/virtrdf#Geometry")) {
                    try {
                        return geoParser.WKTtoGeoJSON(literal.getString());
                    } catch (GeoParsingException e) {
                        throw new PropertyNotAvailableException(e.getMessage());
                    }
                } else if (datatype.equals("http://www.opengis.net/ont/geosparql#gmlLiteral")) {
                    try {
                        return geoParser.GML3toGeoJSON(literal.getString());
                    } catch (GeoParsingException e) {
                        throw new PropertyNotAvailableException(e.getMessage());
                    }
                } else {
                    throw new PropertyNotAvailableException("No supported format in: " + EDP.locnGeometry.toString());
                }
            } else {
                throw new PropertyNotAvailableException("No supported format in: " + EDP.locnGeometry.toString());
            }
        }
        throw new PropertyNotAvailableException(EDP.locnGeometry.toString() + " is not set");
    }

    /**
     * dct:temporal
     * Range: dct:PeriodOfTime
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getDctTemporal(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dct:type
     * Range: skos:Concept
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getDctType(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * owl:versionInfo
     * Range: rdfs:Literal
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getOwlVersionInfo(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * adms:versionNotes
     * Range: rdfs:Literal
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getAdmsVersionNotes(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dcat:accessURL
     * Range: rdfs:Resource
     * Used in: Distribution
     *
     * @param resource
     * @return
     */
    public String getDcatAccessUrl(Resource resource) throws PropertyNotAvailableException {
        Resource res = propertyHelper.getPropertyAsResource(resource, DCATAP.dcatAccessURL);
        return res.getURI();
    }

    /**
     * dct:format
     * Range: dct:MediaTypeOrExtent
     * Used in: Distribution
     *
     * @param resource
     * @return
     */
    public JsonObject getDctFormat(Resource resource) throws PropertyNotAvailableException {
        JsonObject result = new JsonObject();
        Statement stmt = resource.getProperty(DCATAP.dctFormat);
        try {
            if (stmt != null) {
                RDFNode node = stmt.getObject();
                if (node.isResource()) {
                    Resource res = propertyHelper.getResourceFromVocabulary(node.asResource());
                    if (res == null) {
                        try {
                            String format = propertyHelper.getSingleLiteral(node.asResource(), RDFS.label);
                            result.put("id", propertyHelper.generateID(format));
                            result.put("title", format);
                        } catch (PropertyNotAvailableException e) {
                            String format = propertyHelper.extractLabelFromURI(node.asResource().getURI());
                            result.put("id", propertyHelper.generateID(format));
                            result.put("title", format);
                        }
                    } else {
                        String format = propertyHelper.getSingleLiteral(res, EUVOC.dcIdentifier);
                        result.put("id", propertyHelper.generateID(format));
                        result.put("title", format);
                    }
                }
                if (node.isLiteral()) {
                    String format = node.asLiteral().getString();
                    result.put("id", propertyHelper.generateID(format));
                    result.put("title", format);
                }
            } else {
                throw new PropertyNotAvailableException(DCATAP.dctFormat.toString() + " is not set");
            }
        } catch (RuntimeException e) {
            throw new PropertyNotAvailableException(e.getMessage());
        }
        return result;
    }

    /**
     * dct:license
     * Range: dct:LicenseDocument
     * Used in: Distribution, Catalogue
     *
     * @param resource
     * @return
     */
    public JsonObject getDctLicense(Resource resource) throws PropertyNotAvailableException {
        JsonObject result = new JsonObject();
        Statement stmt = resource.getProperty(DCATAP.dctLicense);
        try {
            if (stmt != null) {
                RDFNode node = stmt.getObject();
                if (node.isResource()) {
                    Resource res = propertyHelper.getResourceFromVocabulary(node.asResource());
                    if (res == null) {

                        try {
                            result.put("id", propertyHelper.getSingleLiteral(node.asResource(), EUVOC.dcIdentifier));
                            result.put("title", propertyHelper.getSingleLiteral(node.asResource(), EUVOC.skosAltLabel));
                            result.put("description", propertyHelper.getSingleLiteral(node.asResource(), EUVOC.skosPrefLabel));
                            result.putNull("la_url");
                            result.put("resource", propertyHelper.getPropertyAsResource(node.asResource(), EUVOC.skosExactMatch).getURI());
                        } catch (PropertyNotAvailableException e) {
                            String licence = propertyHelper.extractLabelFromURI(node.asResource().getURI());
                            result.put("id", propertyHelper.generateID(licence));
                            result.put("title", licence);
                            result.put("description", licence);
                            result.putNull("la_url");
                            result.put("resource", node.asResource().getURI());
                        }

                    } else {

                        Statement stmtSameAs = res.getProperty(OWL.sameAs);
                        if (stmtSameAs != null) {
                            res = propertyHelper.getResourceFromVocabulary(stmtSameAs.getResource());
                        }

                        String id = propertyHelper.getSingleLiteral(res, EUVOC.dcIdentifier);
                        String title = propertyHelper.getEnglishLiteral(res, EUVOC.skosAltLabel);
                        String description = propertyHelper.getEnglishLiteral(res, EUVOC.skosPrefLabel);
                        try {
                            Resource url = propertyHelper.getPropertyAsResource(res, EUVOC.skosExactMatch);
                            result.put("resource", url.getURI());
                        } catch (PropertyNotAvailableException e) {
                            result.putNull("resource");
                        }

                        try {
                            Resource resLA = propertyHelper.getPropertyAsResource(res, EDP.edpLicensingAssistant);
                            result.put("la_url", resLA.getURI());
                        } catch (PropertyNotAvailableException e) {
                            result.putNull("in_la");
                        }

                        result.put("id", id);
                        if (title != null) {
                            result.put("title", title);
                        } else {
                            result.put("title", id);
                        }

                        if (description != null) {
                            result.put("description", description);
                        } else {
                            result.put("description", id);
                        }

                    }

                }
                if (node.isLiteral()) {
                    result.put("id", propertyHelper.generateID(node.toString()));
                    result.put("title", node.toString());
                    result.put("description", node.toString());
                    result.putNull("la_url");
                    result.put("resource", "");
                }
            }

        } catch (RuntimeException e) {
            throw new PropertyNotAvailableException(e.getMessage());
        }

        if (result.isEmpty()) {
            throw new PropertyNotAvailableException("No valid " + DCATAP.dctLicense.toString() + " is set");
        }

        return result;
    }

    /**
     * dcat:byteSize
     * Range: rdfs:Literal (xsd:decimal)
     * Used in: Distribution
     *
     * @param resource
     * @return
     */
    public JsonObject getDcatByteSize(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * spdx:checksum
     * Range: spdx:Checksum
     * Used in: Distribution
     *
     * @param resource
     * @return
     */
    public JsonObject getSpdxChecksum(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dcat:downloadURL
     * Range: rdfs:Resource
     * Used in: Distribution
     *
     * @param resource
     * @return
     */
    public JsonArray getDcatDownloadUrl(Resource resource) throws PropertyNotAvailableException {
        return propertyHelper.getURLsFromResource(resource, DCATAP.dcatDownloadURL);
    }

    /**
     * dcat:mediaType
     * Range: dct:MediaTypeOrExtent
     * Used in: Distribution
     *
     * @param resource
     * @return
     */
    public String getDcatMediaType(Resource resource) throws PropertyNotAvailableException {
        return propertyHelper.getStringFromLiteralOrResource(resource, DCATAP.dcatMediaType);
    }

    /**
     * dct:rights
     * Range: dct:RightsStatement
     * Used in: Distribution, Catalogue
     *
     * @param resource
     * @return
     */
    public JsonObject getDctRights(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * adms:status
     * Range: skos:Concept
     * Used in: Distribution
     * <p>
     * Todo Unclear how to parse this property
     *
     * @param resource
     * @return
     */
    public JsonObject getAdmsStatus(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * foaf:homepage
     * Range: foaf:Document
     * Used in: Catalogue
     *
     * @param resource
     * @return
     */
    public JsonObject getFoafHomepage(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dcat:ThemeTaxonomy
     * Range: skos:ConceptTheme
     * Used in: Catalogue
     *
     * @param resource
     * @return
     */
    public JsonObject getDcatThemeTaxonomy(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dct:hasPart
     * Range: dcat:Catalog
     * Used in: Catalogue
     *
     * @param resource
     * @return
     */
    public JsonObject getDctHasPart(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * dct:isPartOf
     * Range: dcat:Catalog
     * Used in: Catalogue
     *
     * @param resource
     * @return
     */
    public JsonObject getDctIsPartOf(Resource resource) throws PropertyNotAvailableException {
        throw new PropertyNotAvailableException();
    }

    /**
     * edp:trans
     * Used in: Dataset
     *
     * @param resource
     * @return
     */
    public JsonObject getEdpTransInfo(Resource resource) throws PropertyNotAvailableException {
        JsonObject result = new JsonObject();

        try {
            result.put("received", propertyHelper.getDateTime(resource, EDP.edpTranslationReceived));
        } catch (PropertyNotAvailableException e) {
            //throw new PropertyNotAvailableException(EDP.edpTranslationReceived.toString() + " is not set.");
        }

        try {
            result.put("issued", propertyHelper.getDateTime(resource, EDP.edpTranslationIssued));
        } catch (PropertyNotAvailableException e) {
            //throw new PropertyNotAvailableException(EDP.edpTranslationIssued.toString() + " is not set.");
        }

        //ToDo Get Original Language (not needed yet)

        try {
            Resource status = propertyHelper.getPropertyAsResource(resource, EDP.edpTranslationStatus);
            if (status.equals(EDP.edpTranslationCompleted)) {
                result.put("status", "completed");
            } else if (status.equals(EDP.edpTranslationInProcess)) {
                result.put("status", "processing");
            } else {
                result.put("status", "unknown");
            }
        } catch (PropertyNotAvailableException e) {
            //throw new PropertyNotAvailableException(EDP.edpTranslationStatus.toString() + " is not set.");
        }


        return result;
    }


}
