package com.processdataquality.praeclarus.action;

import com.processdataquality.praeclarus.annotation.Plugin;
import com.processdataquality.praeclarus.exception.InvalidOptionValueException;
import com.processdataquality.praeclarus.llm.LlmClient;
import com.processdataquality.praeclarus.llm.OllamaClient;
import com.processdataquality.praeclarus.option.ColumnNameListOption;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.util.List;

/**
 * @author Sean Dewantoro
 * @date 23/6/2026
 */
@Plugin(
    name = "LlmEnrich",
    author = "Sean Dewantoro",
    version = "1.0",
    synopsis = "Reads a free-text column, sends each cell to an LLM, and appends "
            + "the extracted value as a new column (originals untouched)."
)
public class LlmEnrich extends AbstractAction {

    private static final String SYSTEM_PROMPT =
            "You are a clinical information extraction assistant. Given the text of a "
                    + "clinical note, extract the single primary diagnosis and respond with "
                    + "a JSON object of the form {\"diagnosis\": \"...\"}.";

    private final ObjectMapper mapper = new ObjectMapper();

    public LlmEnrich() {
        super();
        getOptions().addDefault(new ColumnNameListOption("Text column"));
        getOptions().addDefault("Output column name", "extracted_diagnosis");
        getOptions().addDefault("Endpoint URL", "http://localhost:11434"); // LAN IP of the Ollama host
        getOptions().addDefault("Model name", "llama3.2:3b");
    }

    @Override
    public Table run(List<Table> inputList) throws InvalidOptionValueException {
        if (inputList.size() != 1) {
            throw new IllegalArgumentException("This action requires one table as input.");
        }
        Table input = inputList.remove(0);

        String textColName = getSelectedColumnNameValue("Text column");
        if (textColName == null || textColName.isEmpty()
                || !input.columnNames().contains(textColName)) {
            throw new InvalidOptionValueException("Select a valid text column to enrich.");
        }
        String outColName = getOptions().get("Output column name").asString();
        String endpoint = getOptions().get("Endpoint URL").asString();
        String model = getOptions().get("Model name").asString();

        // One JSON schema (single field) reused for every row's structured output.
        ObjectNode format = mapper.createObjectNode();
        format.put("type", "object");
        format.putObject("properties").putObject("diagnosis").put("type", "string");
        format.putArray("required").add("diagnosis");

        LlmClient llmClient = new OllamaClient(endpoint, model);
        StringColumn outCol = StringColumn.create(outColName);

        for (Row row : input) {
            String text = row.getString(textColName);
            if (text == null || text.trim().isEmpty()) {
                outCol.append("");                      // don't call the LLM on nothing
                continue;
            }
            try {
                String json = llmClient.generate(SYSTEM_PROMPT, text, format);
                outCol.append(mapper.readTree(json).path("diagnosis").asText(""));
            } catch (Exception e) {
                System.out.println("LlmEnrich: skipping row " + row.getRowNumber()
                        + " — " + e.getMessage());
                outCol.append("");
            }
        }

        Table result = input.copy();
        if (result.columnNames().contains(outColName)) {
            return result.replaceColumn(outCol);
        }
        return result.addColumns(outCol);
    }
}
