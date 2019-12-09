package io.piveau.hub.util.rdf;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import java.util.HashMap;
import java.util.Map;

public class SPDX {
    private static final Model m = ModelFactory.createDefaultModel();

    public static final String SPDX_NS = "https://spdx.org/rdf/terms/#";

    public static Map<String, String> getNsMap() {
        Map<String, String> map = new HashMap<>();
        map.put("spdx", SPDX_NS);

        return map;
    }


    //classes
    public static final Resource Checksum = m.createResource(SPDX_NS + "Checksum");
    public static final Resource checksumAlgorithm_md5 = m.createResource(SPDX_NS + "checksumAlgorithm_md5");

    //properties
    public static final Property checksum = m.createProperty(SPDX_NS + "checksum");
    public static final Property algorithm = m.createProperty(SPDX_NS + "algorithm");
    public static final Property checksumValue = m.createProperty(SPDX_NS + "checksumValue");


}
