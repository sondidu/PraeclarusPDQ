package com.processdataquality.praeclarus.llm;

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
 * The model, system prompt and output schema are fixed for now; only the prompt
 * varies per call.
 *
 * @author Sean Dewantoro
 * @date 23/6/26
 */
public class OllamaClient implements LlmClient {

    private static final String OLLAMA_BASE = "http://172.19.41.216:11434"; // this is a private ip btw
    private static final String MODEL = "llama3.2:3b";
    private static final String SYSTEM_PROMPT =
            "You will receive a food meal. Respond following the desired JSON output schema.";

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String generate(String prompt) throws IOException, InterruptedException {

        // Constrain the model to this JSON schema (Ollama structured outputs).
        ObjectNode format = mapper.createObjectNode();
        format.put("type", "object");
        ObjectNode properties = format.putObject("properties");
        properties.putObject("meal").put("type", "string");
        ObjectNode recipe = properties.putObject("recipe");
        recipe.put("type", "array");
        recipe.putObject("items").put("type", "string");
        properties.putObject("time_to_make_in_seconds").put("type", "number");
        format.putArray("required").add("meal").add("recipe").add("time_to_make_in_seconds");

        ObjectNode body = mapper.createObjectNode();
        body.put("model", MODEL);
        body.put("stream", false);
        body.put("system", SYSTEM_PROMPT);
        body.set("format", format);
        body.put("prompt", prompt);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(OLLAMA_BASE + "/api/generate"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofMinutes(2))
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        // For /api/generate the model's (schema-conforming) JSON arrives as text
        // in the "response" field.
        return mapper.readTree(response.body()).get("response").asText();
    }

    // This is temporary and will eventually be deleted
    public static void main(String[] args) throws IOException, InterruptedException {
        OllamaClient client = new OllamaClient();
        System.out.println(client.generate("Make me fried rice please"));
    }
}
