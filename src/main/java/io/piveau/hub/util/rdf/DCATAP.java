package io.piveau.hub.util.rdf;

import io.vertx.core.json.JsonObject;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import java.util.HashMap;
import java.util.Map;

/**
 * Jena Classes and Resources for managing DCAT-AP
 * This is partly a dublicate from Jena build-in classes, but on purpose!
 * @Todo be completed
 *
 */
public class DCATAP {

    private static final Model m = ModelFactory.createDefaultModel();

    public static final String DCAT_NS = "http://www.w3.org/ns/dcat#";
    public static final String DCT_NS = "http://purl.org/dc/terms/";
    public static final String RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    public static final String FOAF_NS = "http://xmlns.com/foaf/0.1/";
    public static final String VCARD_NS = "http://www.w3.org/2006/vcard/ns#";



    public static Map<String, String> getNsMap() {
        Map<String, String> map = new HashMap<>();
        map.put("dcat", DCAT_NS);
        map.put("dct", DCT_NS);
        map.put("rdf", RDF_NS);
        map.put("foaf", FOAF_NS);
        map.put("vcard", VCARD_NS);
        return map;
    }

    //Classes
    public static final Resource DcatDistribution = m.createResource(DCAT_NS + "Distribution");
    public static final Resource DcatDataset = m.createResource(DCAT_NS + "Dataset");
    public static final Resource DcatCatalog = m.createResource(DCAT_NS + "Catalog");
    public static final Resource FoafAgent = m.createResource(FOAF_NS + "Agent");

    //Properties
    public static final Property dctTitle = m.createProperty(DCT_NS + "title");
    public static final Property dctDescription = m.createProperty(DCT_NS + "description");
    public static final Property dctPublisher = m.createProperty(DCT_NS + "publisher");
    public static final Property dctLanguage = m.createProperty(DCT_NS + "language");
    public static final Property dctFormat = m.createProperty(DCT_NS + "format");
    public static final Property dctIssued = m.createProperty(DCT_NS + "issued");
    public static final Property dctModified = m.createProperty(DCT_NS + "modified");
    public static final Property dctSpatial = m.createProperty(DCT_NS + "spatial");
    public static final Property dctLicense = m.createProperty(DCT_NS + "license");
    public static final Property dctProvenance = m.createProperty(DCT_NS + "provenance");
    public static final Property dcatDownloadURL = m.createProperty(DCAT_NS + "downloadURL");
    public static final Property dcatMediaType = m.createProperty(DCAT_NS + "mediaType");
    public static final Property dctAccessRights = m.createProperty(DCT_NS + "accessRights");
    public static final Property dctConformsTo = m.createProperty(DCT_NS + "conformsTo");
    public static final Property dctType = m.createProperty(DCT_NS + "type");
    public static final Property dcatAccessURL = m.createProperty(DCAT_NS + "accessURL");
    public static final Property dcatContactPoint = m.createProperty(DCAT_NS + "contactPoint");
    public static final Property dcatDataset = m.createProperty(DCAT_NS + "dataset");
    public static final Property dcatDistribution = m.createProperty(DCAT_NS + "distribution");
    public static final Property dcatLandingPage = m.createProperty(DCAT_NS + "landingPage");
    public static final Property dcatKeyword = m.createProperty(DCAT_NS + "keyword");
    public static final Property dcatTheme = m.createProperty(DCAT_NS + "theme");
    public static final Property rdfType = m.createProperty(RDF_NS + "type");
    public static final Property rdfResource = m.createProperty(RDF_NS + "resource");
    public static final Property foafName = m.createProperty(FOAF_NS + "name");
    public static final Property foafHomepage = m.createProperty(FOAF_NS + "homepage");
    public static final Property foafPage = m.createProperty(FOAF_NS + "page");
    public static final Property vcardHasEmail = m.createProperty(VCARD_NS + "hasEmail");
    public static final Property vcardHasName = m.createProperty(VCARD_NS + "hasName");
    public static final Property vcardFn = m.createProperty(VCARD_NS + "fn");

    // ToDo Workaround, themes should be resolved with Linked Data
    public static Map<String, JsonObject> themeMapping() {
        Map<String, JsonObject> map = new HashMap<>();
        map.put("http://publications.europa.eu/resource/authority/data-theme/ENVI",
            new JsonObject().put("title", "Environment").put("id", "envi"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/AGRI",
            new JsonObject().put("title", "Agriculture, fisheries, forestry and food").put("id", "agri"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/ECON",
            new JsonObject().put("title", "Economy and finance").put("id", "econ"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/EDUC",
            new JsonObject().put("title", "Education, culture and sport").put("id", "educ"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/ENER",
            new JsonObject().put("title", "Energy").put("id", "ener"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/GOVE",
            new JsonObject().put("title", "Government and public sector").put("id", "gove"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/HEAL",
            new JsonObject().put("title", "Health").put("id", "heal"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/INTR",
            new JsonObject().put("title", "International issues").put("id", "intr"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/JUST",
            new JsonObject().put("title", "Justice, legal system and public safety").put("id", "just"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/OP_DATPRO",
            new JsonObject().put("title", "Provisional data").put("id", "od_datpro"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/REGI",
            new JsonObject().put("title", "Regions and cities").put("id", "regi"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/SOCI",
            new JsonObject().put("title", "Population and society").put("id", "soci"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/TECH",
            new JsonObject().put("title", "Science and technology").put("id", "tech"));
        map.put("http://publications.europa.eu/resource/authority/data-theme/TRAN",
            new JsonObject().put("title", "Transport").put("id", "tran"));
        return map;
    }


}
