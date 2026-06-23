package com.processdataquality.praeclarus.llm;

import java.io.IOException;

/**
 * A minimal boundary for calling a Large Language Model. An implementation owns
 * the transport and request/response shape of one provider; callers only hand
 * over a prompt and receive the model's raw response back.
 *
 * @author Sean Dewantoro
 * @date 23/6/26
 */
public interface LlmClient {

    /**
     * Sends a prompt to the model and returns its raw response. When the
     * implementation asks the model for structured output, the returned string
     * is the JSON text the caller is expected to parse.
     *
     * @param prompt the prompt to send to the model
     * @return the model's raw response
     * @throws IOException          if the call to the model fails
     * @throws InterruptedException if the call is interrupted while waiting
     */
    String generate(String prompt) throws IOException, InterruptedException;
}
