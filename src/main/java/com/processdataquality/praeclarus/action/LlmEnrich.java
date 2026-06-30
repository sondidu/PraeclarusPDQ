package com.processdataquality.praeclarus.action;

import com.processdataquality.praeclarus.annotation.Plugin;
import com.processdataquality.praeclarus.exception.InvalidOptionValueException;
import com.processdataquality.praeclarus.llm.LlmClient;
import com.processdataquality.praeclarus.llm.OllamaClient;
import com.processdataquality.praeclarus.option.ColumnNameListOption;
import com.processdataquality.praeclarus.option.MultiLineOption;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Sean Dewantoro
 * @date 23/6/2026
 */
@Plugin(
    name = "LlmEnrich",
    author = "Sean Dewantoro",
    version = "1.0",
    synopsis = "Reads a free-text column, sends each cell to an LLM, and appends the "
            + "extracted fields as new columns (originals untouched)."
)
public class LlmEnrich extends AbstractAction {

    // The fields extracted from each note. This single list drives the output JSON
    // schema, the appended columns, and the parse loop, so they can never drift.
    // Their meaning is described to the model in SYSTEM_PROMPT below.
    private static final List<String> FIELDS = List.of(
            "primary_diagnosis", "admission_date", "discharge_date",
            "discharge_location", "procedures", "medications_on_discharge");

    // Default instructions shown in the (editable) "System prompt" option. Markdown,
    // since models follow it more reliably than a flat string. The field list here
    // mirrors FIELDS above; keep the two in sync if you add or rename a field.
    private static final String SYSTEM_PROMPT =
            "# Role\n" +
            "You are a clinical information-extraction assistant. You are given the full text\n" +
            "of one de-identified MIMIC-III clinical note (free text) and must extract a fixed\n" +
            "set of fields as structured data.\n" +
            "\n" +
            "# Rules\n" +
            "- Return **only** a single JSON object conforming to the provided schema.\n" +
            "- Extract **only what is explicitly stated**. Do not infer, guess, or fabricate.\n" +
            "  If a field is not present in the note, return an empty string \"\".\n" +
            "- Keep values short and canonical — a clinical term or phrase, not a sentence\n" +
            "  (e.g. \"Sepsis\", not \"The patient was admitted with sepsis\").\n" +
            "- Preserve clinical terminology and abbreviations as written.\n" +
            "\n" +
            "# Handling de-identified text\n" +
            "MIMIC-III masks PHI with bracketed placeholders like [**2150-8-10**] (dates) or\n" +
            "[**Known lastname 1234**] (names/IDs).\n" +
            "- Treat a bracketed date as the note's real date — use it and normalize to YYYY-MM-DD.\n" +
            "- Treat bracketed names/IDs/locations as redacted — never extract them as values.\n" +
            "\n" +
            "# Dates\n" +
            "- Output dates as YYYY-MM-DD, exactly as written in the note. Do not shift or reconcile.\n" +
            "\n" +
            "# Fields to extract\n" +
            "- primary_diagnosis: principal diagnosis for this admission (short clinical term).\n" +
            "- admission_date: admission date (YYYY-MM-DD) if stated, else \"\".\n" +
            "- discharge_date: discharge date (YYYY-MM-DD) if stated, else \"\".\n" +
            "- discharge_location: location the patient was discharged to (e.g. \"Home\", \"Rehab\", \"SNF\"; \"Expired\" if the patient died).\n" +
            "- procedures: major procedures/operations performed (may be several, in one string).\n" +
            "- medications_on_discharge: discharge medications (may be several, in one string).";

    private final ObjectMapper mapper = new ObjectMapper();

    public LlmEnrich() {
        super();
        getOptions().addDefault(new ColumnNameListOption("Text column"));
        getOptions().addDefault(new MultiLineOption("System prompt", SYSTEM_PROMPT));
        getOptions().addDefault("Endpoint URL", "http://localhost:11434"); // or a LAN IP if Ollama runs elsewhere
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
        String systemPrompt = getOptions().get("System prompt").asString();
        String endpoint = getOptions().get("Endpoint URL").asString();
        String model = getOptions().get("Model name").asString();

        // One JSON schema, built once from FIELDS and reused for every row: an object
        // with one required string property per field, so every key always comes back.
        ObjectNode format = mapper.createObjectNode();
        format.put("type", "object");
        ObjectNode properties = format.putObject("properties");
        ArrayNode required = format.putArray("required");
        for (String field : FIELDS) {
            properties.putObject(field).put("type", "string");
            required.add(field);
        }

        // One output column per field, kept in field order.
        Map<String, StringColumn> outCols = new LinkedHashMap<>();
        for (String field : FIELDS) {
            outCols.put(field, StringColumn.create(field));
        }

        LlmClient llmClient = new OllamaClient(endpoint, model);

        for (Row row : input) {
            String text = row.getString(textColName);
            if (text == null || text.trim().isEmpty()) {
                for (StringColumn col : outCols.values()) {
                    col.append("");                         // don't call the LLM on nothing
                }
                continue;
            }
            try {
                String json = llmClient.generate(systemPrompt, "Clinical note:\n\n" + text, format);
                JsonNode parsed = mapper.readTree(json);
                for (Map.Entry<String, StringColumn> entry : outCols.entrySet()) {
                    entry.getValue().append(parsed.path(entry.getKey()).asText(""));
                }
            } catch (Exception e) {
                System.out.println("LlmEnrich: skipping row " + row.getRowNumber()
                        + " — " + e.getMessage());
                for (StringColumn col : outCols.values()) {
                    col.append("");
                }
            }
        }

        Table result = input.copy();
        for (StringColumn col : outCols.values()) {
            if (result.columnNames().contains(col.name())) {
                result.replaceColumn(col);
            } else {
                result.addColumns(col);
            }
        }
        return result;
    }
}
