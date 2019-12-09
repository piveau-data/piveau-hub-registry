package io.piveau.hub;

import io.piveau.hub.util.rdf.GeoParser;
import io.piveau.hub.util.rdf.GeoParsingException;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Testing geo parsing")
@ExtendWith(VertxExtension.class)
class GeoParserTest {

    @Test
    @DisplayName("Parsing WKT")
    void testWKTtoGeoJSON(Vertx vertx, VertxTestContext testContext) {
        GeoParser geoParser = new GeoParser();
        String match1 = "{\n" +
                "  \"type\" : \"Polygon\",\n" +
                "  \"coordinates\" : [ [ [ 13.66, 52.89 ], [ 13.83, 52.89 ], [ 13.83, 52.79 ], [ 13.66, 52.79 ], [ 13.66, 52.89 ] ] ]\n" +
                "}";

        String input1 = "POLYGON((13.66 52.89,13.83 52.89,13.83 52.79, 13.66 52.79,13.66 52.89))";

        String match2 = "{\n" +
                "    \"type\": \"Point\", \n" +
                "    \"coordinates\": [50, 50]\n" +
                "}";
        String input2 = "Point(50 50)";
        try {
            JsonObject result1 = geoParser.WKTtoGeoJSON(input1);
            testContext.verify(() -> assertEquals(result1, new JsonObject(match1)));
        } catch (GeoParsingException e) {
            testContext.failNow(e);
        }

        try {
            JsonObject result2 = geoParser.WKTtoGeoJSON(input2);
            testContext.verify(() -> assertEquals(result2, new JsonObject(match2)));
            testContext.completeNow();
        } catch (GeoParsingException e) {
            testContext.failNow(e);
        }
    }

    @Test
    @DisplayName("Parsing GML3")
    void testGML3toGeoJSON(Vertx vertx, VertxTestContext testContext) {
        GeoParser geoParser = new GeoParser();
        String match1 = "{\n" +
                "  \"type\" : \"Polygon\",\n" +
                "  \"coordinates\" : [ [ [ 12.915, 53.1485 ], [ 12.915, 53.1985 ], [ 12.9983, 53.1985 ], [ 12.9983, 53.1485 ], [ 12.915, 53.1485 ] ] ]\n" +
                "}";
        String input1 = "<gml:Envelope srsName=\"http://www.opengis.net/def/EPSG/0/4326\"><gml:lowerCorner>53.1485 12.915</gml:lowerCorner><gml:upperCorner>53.1985 12.9983</gml:upperCorner></gml:Envelope>";
        try {
            JsonObject result1 = geoParser.GML3toGeoJSON(input1);
            testContext.verify(() -> assertEquals(result1, new JsonObject(match1)));
            testContext.completeNow();
        } catch (GeoParsingException e) {
            testContext.failNow(e);
        }
    }
}
