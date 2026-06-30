package com.processdataquality.praeclarus.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Calls a local Ollama server over HTTP using the JDK's built-in HttpClient.
 * The endpoint and model are fixed at construction; the prompts and output
 * schema are supplied per call.
 *
 * @author Sean Dewantoro
 * @date 23/6/26
 */
public class OllamaClient implements LlmClient {

    // local CPU inference is slow, so keep the timeout generous
    private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(5);

    private final String baseUrl;
    private final String model;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    public OllamaClient(String baseUrl, String model) {
        this.baseUrl = baseUrl;
        this.model = model;
    }

    @Override
    public String generate(String systemPrompt, String userPrompt, JsonNode format)
            throws IOException, InterruptedException {

        ObjectNode body = mapper.createObjectNode();
        body.put("model", model);
        body.put("stream", false);
        body.put("think", false);
        body.putObject("options").put("seed", 42);
        body.put("system", systemPrompt);
        if (format != null) {
            body.set("format", format);
        }
        body.put("prompt", userPrompt);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/generate"))
                .header("Content-Type", "application/json")
                .timeout(REQUEST_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        // For /api/generate the model's (schema-conforming) JSON arrives as text
        // in the "response" field.
        return mapper.readTree(response.body()).get("response").asText();
    }
}
