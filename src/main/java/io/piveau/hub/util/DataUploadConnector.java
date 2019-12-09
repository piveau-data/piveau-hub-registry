package io.piveau.hub.util;

import io.piveau.hub.dataobjects.DatasetHelper;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.piveau.hub.util.rdf.PropertyNotAvailableException;
import io.piveau.hub.util.rdf.PropertyParser;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import org.apache.jena.rdf.model.Resource;

import java.util.UUID;

public class DataUploadConnector {

    PiveauLogger LOGGER = PiveauLoggerFactory.getLogger(getClass());

    private WebClient client;
    private String url;
    private String serviceUrl;
    private String apiKey;

    public static DataUploadConnector create(WebClient client, JsonObject config) {
        return new DataUploadConnector(client, config);
    }

    private DataUploadConnector(WebClient client, JsonObject config) {
        this.client = client;
        this.url = config.getString("url");
        this.serviceUrl = config.getString("service_url");
        this.apiKey = config.getString("api_key");
    }

    public String getDataURL(String distributionID) {
        return url + "/v1/data/" + distributionID;
    }

    public JsonArray getResponse(DatasetHelper helper) {
        Resource resource = helper.resource();

        PropertyParser propertyParser = new PropertyParser();
        try {
            JsonArray result = propertyParser.getDcatDistributions(resource);
            JsonArray response = prepareResponse(result);
            prepareDataService(response);
            return response;
        } catch (PropertyNotAvailableException e) {
            return null;
        }
    }

    private JsonArray prepareResponse(JsonArray distributions) {
        JsonArray result = new JsonArray();
        distributions.iterator().forEachRemaining(o -> {
            JsonObject dist = (JsonObject) o;
            String token = UUID.randomUUID().toString();
            JsonObject returnDist = new JsonObject();
            if(dist.containsKey("id")) {
                returnDist.put("id", dist.getValue("id"));
            }
            if(dist.containsKey("title")) {
                returnDist.put("title", dist.getValue("title"));
            }
            if(dist.containsKey("format")) {
                returnDist.put("format", dist.getValue("format"));
            }
            if(dist.containsKey("access_url")) {
                returnDist.put("access_url", dist.getValue("access_url"));
                returnDist.put("upload_url", dist.getValue("access_url") + "?token=" + token);
                returnDist.put("upload_token", token);
            }
            if(returnDist.containsKey("access_url") && returnDist.getValue("access_url").toString().contains(this.url)) {
                result.add(returnDist);
            }
        });

        return result;
    }

    private void prepareDataService(JsonArray distributions) {
        JsonArray payload = new JsonArray();
        distributions.iterator().forEachRemaining(o -> {
            JsonObject file = new JsonObject();
            JsonObject dist = (JsonObject) o;
            file.put("id", dist.getValue("id"));
            file.put("token", dist.getValue("upload_token"));
            payload.add(file);
        });
        HttpRequest<Buffer> request = client.putAbs(this.serviceUrl + "/v1/data")
                .putHeader("Authorization", this.apiKey)
                .putHeader("Content-Type", "application/json");

        request.sendJson(payload, ar-> {
            if (ar.succeeded()) {
                if (ar.result().statusCode() == 200) {
                    LOGGER.info("Successful requested Data Upload Service");
                } else {
                    LOGGER.error("Error when requested Data Upload Service " + ar.result().bodyAsString());
                }
            } else {
                LOGGER.error("Error when requested Data Upload Service " + ar.cause());
            }
        });
    }
}
