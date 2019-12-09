package io.piveau.hub.util.rdf;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;

import java.util.HashMap;
import java.util.Map;

public class OA {


    private static final Model m = ModelFactory.createDefaultModel();

    public static final String OA_NS = "http://www.w3.org/ns/oa#";

    public static Map<String, String> getNsMap() {
        Map<String, String> map = new HashMap<>();
        map.put("oa", OA_NS);
        return map;
    }

    //properties
    public static final Property hasBody = m.createProperty(OA_NS + "hasBody");

}
