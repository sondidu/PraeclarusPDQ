package com.processdataquality.praeclarus.llm;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/**
 * A minimal boundary for calling a Large Language Model. An implementation owns
 * the transport and request/response shape of one provider; callers supply the
 * prompts and the desired output schema, and receive the model's raw response.
 *
 * @author Sean Dewantoro
 * @date 23/6/26
 */
public interface LlmClient {

    /**
     * Sends a system prompt and user prompt to the model and returns its raw
     * response. When {@code format} is supplied, the model is asked to constrain
     * its output to that JSON schema, so the returned string is JSON text the
     * caller is expected to parse.
     *
     * @param systemPrompt the system / instruction prompt
     * @param userPrompt   the user content to act on
     * @param format       a JSON schema constraining the output, or null for free-form
     * @return the model's raw response
     * @throws IOException          if the call to the model fails
     * @throws InterruptedException if the call is interrupted while waiting
     */
    String generate(String systemPrompt, String userPrompt, JsonNode format)
            throws IOException, InterruptedException;
}
