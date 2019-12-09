package io.piveau.hub.util.rdf;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import java.util.HashMap;
import java.util.Map;

public class SHACL {
    private static final Model m = ModelFactory.createDefaultModel();
    public static final String NS = "http://www.w3.org/ns/shacl#";
    public static final Resource NAMESPACE;
    public static final Resource ValidationReport;
    public static final Resource ValidationResult;
    public static final Resource Violation;
    public static final Property result;
    public static final Property resultMessage;
    public static final Property resultSeverity;
    public static final Property sourceConstraintComponent;


    public static Map<String, String> getNsMap() {
        Map<String, String> map = new HashMap<>();
        map.put("sh", NS);

        return map;
    }

    public static String getURI() {
        return "http://www.w3.org/ns/shacl#";
    }

    static {
        NAMESPACE = m.createResource("http://www.w3.org/ns/dqv#");
        ValidationReport = m.createResource("http://www.w3.org/ns/shacl#ValidationReport");
        ValidationResult = m.createResource("http://www.w3.org/ns/shacl#ValidationResult");
        Violation = m.createResource("http://www.w3.org/ns/shacl#Violation");
        result = m.createProperty("http://www.w3.org/ns/shacl#result");
        resultMessage = m.createProperty("http://www.w3.org/ns/shacl#resultMessage");
        resultSeverity = m.createProperty("http://www.w3.org/ns/shacl#resultSeverity");
        sourceConstraintComponent = m.createProperty("http://www.w3.org/ns/shacl#sourceConstraintComponent");
    }
}
