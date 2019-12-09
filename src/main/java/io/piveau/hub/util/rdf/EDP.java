package io.piveau.hub.util.rdf;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import java.util.HashMap;
import java.util.Map;

/**
 * Jena Classes and Resources for managing EDP
 * This is partly a dublicate from Jena build-in classes, but on purpose!
 * @Todo be completed
 *
 */
public class EDP {

    private static final Model m = ModelFactory.createDefaultModel();

    public static final String EDP_NS = "https://europeandataportal.eu/voc#";
    public static final String LOCN_NS = "http://www.w3.org/ns/locn#";

    public static Map<String, String> getNsMap() {
        Map<String, String> map = new HashMap<>();
        map.put("edp", EDP_NS);
        return map;
    }

    //Classes
    public static final Resource edpTranslationInProcess = m.createResource(EDP_NS + "TransInProcess");
    public static final Resource edpTranslationCompleted = m.createResource(EDP_NS + "TransCompleted");

    //Properties
    public static final Property edpLicensingAssistant = m.createProperty(EDP_NS + "licensingAssistant");
    public static final Property edpTranslationIssued = m.createProperty(EDP_NS + "transIssued");
    public static final Property edpTranslationReceived = m.createProperty(EDP_NS + "transReceived");
    public static final Property edpTranslationStatus = m.createProperty(EDP_NS + "transStatus");
    public static final Property edpOriginalLanguage = m.createProperty(EDP_NS + "originalLanguage");

    public static final Property locnGeometry = m.createProperty(LOCN_NS + "geometry");


}
