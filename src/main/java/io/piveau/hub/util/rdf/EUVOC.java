package io.piveau.hub.util.rdf;

import io.vertx.core.json.JsonObject;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import java.util.HashMap;
import java.util.Map;

/**
 * Jena Classes and Resources for managing EuroVoc
 * This is partly a dublicate from Jena build-in classes, but on purpose!
 * @Todo be completed
 *
 */
public class EUVOC {

    private static final Model m = ModelFactory.createDefaultModel();

    public static final String AT_NS = "http://publications.europa.eu/ontology/authority/";
    public static final String EUVOC_NS = "http://publications.europa.eu/ontology/euvoc#";
    public static final String DC_NS = "http://purl.org/dc/elements/1.1/";
    public static final String SKOS_NS = "http://www.w3.org/2004/02/skos/core#";

    public static Map<String, String> getNsMap() {
        Map<String, String> map = new HashMap<>();
        map.put("at", AT_NS);
        map.put("euvoc", EUVOC_NS);
        map.put("dc", DC_NS);
        map.put("skos", SKOS_NS);
        return map;
    }

    //Classes
    public static final Resource EuvocAlpha2 = m.createResource("http://publications.europa.eu/resource/authority/notation-type/ISO_3166_1_ALPHA_2");

    //Properties
    public static final Property atOpMappedCode = m.createProperty(AT_NS + "op-mapped-code");
    public static final Property atLegacyCode = m.createProperty(AT_NS + "legacy-code");
    public static final Property euvocXlNotation = m.createProperty(EUVOC_NS + "xlNotation");
    public static final Property euvocXlCodification = m.createProperty(EUVOC_NS + "xlCodification");
    public static final Property dcSource = m.createProperty(DC_NS + "source");
    public static final Property dcIdentifier = m.createProperty(DC_NS + "identifier");
    public static final Property skosPrefLabel = m.createProperty(SKOS_NS + "prefLabel");
    public static final Property skosAltLabel = m.createProperty(SKOS_NS + "altLabel");
    public static final Property skosExactMatch = m.createProperty(SKOS_NS + "exactMatch");

}
