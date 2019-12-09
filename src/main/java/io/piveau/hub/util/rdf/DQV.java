package io.piveau.hub.util.rdf;


import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import java.util.HashMap;
import java.util.Map;

/**
 * Vocabulary definition for the <a href="https://www.w3.org/TR/vocab-dqv/">Data Quality vocabulary</a>.
 *
 * @see <a href="https://github.com/w3c/dwbp/blob/gh-pages/dqv.ttl">Turtle specification</a>
 */

public class DQV {

    private static final Model m = ModelFactory.createDefaultModel();

    public static final String DQV_NS = "http://www.w3.org/ns/dqv#";

    public static Map<String, String> getNsMap() {
        Map<String, String> map = new HashMap<>();
        map.put("dqv", DQV_NS);

        return map;
    }

    //classes
    public static final Resource Category = m.createResource(DQV_NS + "Category");
    public static final Resource Dimension = m.createResource(DQV_NS + "Dimension");
    public static final Resource Metric = m.createResource(DQV_NS + "Metric");
    public static final Resource QualityAnnotation = m.createResource(DQV_NS + "QualityAnnotation");
    public static final Resource QualityCertificate = m.createResource(DQV_NS + "QualityCertificate");
    public static final Resource QualityMeasurement = m.createResource(DQV_NS + "QualityMeasurement");
    public static final Resource QualityMeasurementDataset = m.createResource(DQV_NS + "QualityMeasurementDataset");
    public static final Resource QualityMetadata = m.createResource(DQV_NS + "QualityMetadata");
    public static final Resource QualityPolicy = m.createResource(DQV_NS + "QualityPolicy");
    public static final Resource UserQualityFeedback = m.createResource(DQV_NS + "UserQualityFeedback");

    //properties
    public static final Property computedOn = m.createProperty(DQV_NS + "computedOn");
    public static final Property expectedDataType = m.createProperty(DQV_NS + "expectedDataType");
    public static final Property inCategory = m.createProperty(DQV_NS + "inCategory");
    public static final Property inDimension = m.createProperty(DQV_NS + "inDimension");
    public static final Property isMeasurementOf = m.createProperty(DQV_NS + "isMeasurementOf");
    public static final Property hasQualityAnnotation = m.createProperty(DQV_NS + "hasQualityAnnotation");
    public static final Property hasQualityMeasurement = m.createProperty(DQV_NS + "hasQualityMeasurement");
    public static final Property hasQualityMetadata = m.createProperty(DQV_NS + "hasQualityMetadata");
    public static final Property value = m.createProperty(DQV_NS + "value");
}
