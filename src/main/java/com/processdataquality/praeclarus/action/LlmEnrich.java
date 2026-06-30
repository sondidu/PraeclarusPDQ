package com.processdataquality.praeclarus.action;

import com.processdataquality.praeclarus.annotation.Plugin;
import com.processdataquality.praeclarus.exception.InvalidOptionValueException;
import com.processdataquality.praeclarus.llm.LlmClient;
import com.processdataquality.praeclarus.llm.OllamaClient;
import com.processdataquality.praeclarus.option.ColumnNameListOption;
import com.processdataquality.praeclarus.option.MultiLineOption;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Sean Dewantoro
 * @date 23/6/2026
 */
@Plugin(
    name = "LlmEnrich",
    author = "Sean Dewantoro",
    version = "1.0",
    synopsis = "Reads a free-text column, sends each cell to an LLM, and appends the "
            + "extracted label/value pairs as new columns (originals untouched)."
)
public class LlmEnrich extends AbstractAction {

    // Default instructions shown in the (editable) "System prompt" option. Markdown,
    // since models follow it more reliably than a flat string. The model returns an open
    // list of {column_name, value} pairs, named after the note's own labels, so one prompt
    // handles every note category (discharge summary, echo, radiology) without a fixed
    // field list. The schema below is just a fixed envelope around that variable list.
    private static final String SYSTEM_PROMPT =
            "# Role\n" +
            "You are a clinical information-extraction assistant. You are given the full text of one\n" +
            "de-identified MIMIC-III clinical note and must extract its information as a flat list of\n" +
            "(column_name, value) pairs.\n" +
            "\n" +
            "# Output\n" +
            "- Return only: { \"fields\": [ { \"column_name\": \"...\", \"value\": \"...\" }, ... ] }.\n" +
            "- value is always a string. Extract only what is explicitly stated — do not infer or\n" +
            "  fabricate. Omit a field entirely rather than emitting an empty or guessed value.\n" +
            "\n" +
            "# These notes label themselves — reuse their labels\n" +
            "Most information appears as \"Label: value\" lines, grouped under section headers such as\n" +
            "PATIENT/TEST INFORMATION, INTERPRETATION, Conclusions, FINAL REPORT, IMPRESSION, COMPARISON,\n" +
            "or (in discharge summaries) Chief Complaint, Discharge Diagnosis, Discharge Disposition.\n" +
            "- Derive each column_name from the note's own label (the text before the colon); do not\n" +
            "  invent wording when a label already exists.\n" +
            "- Use lowercase snake_case: \"Technical Quality:\" -> technical_quality,\n" +
            "  \"REASON FOR THIS EXAMINATION:\" -> reason_for_this_examination.\n" +
            "- If a label embeds a unit, keep the unit in the name and put only the bare value in value:\n" +
            "  \"Weight (lb): 150\" -> \"weight_lb\",\"150\";  \"BP (mm Hg): 120/80\" -> \"bp_mm_hg\",\"120/80\".\n" +
            "- Capture large section bodies as one column each (impression, findings,\n" +
            "  brief_hospital_course, discharge_instructions, ...). For numbered or bulleted lists\n" +
            "  (Conclusions, Discharge Medications, ...), summarise the items into one concise string\n" +
            "  rather than copying them verbatim.\n" +
            "- For salient information with no label (e.g. the radiograph-type line, or a bare\n" +
            "  date/time header), give it a short descriptive snake_case name (e.g. exam, study_datetime).\n" +
            "- Never put a value, date, or measurement inside a column_name.\n" +
            "\n" +
            "# Consistent naming\n" +
            "Use the same column_name for the same concept every time. When a concept matches one of\n" +
            "these preferred names, use it exactly even if the note's wording differs; otherwise derive\n" +
            "from the note's label:\n" +
            "admission_date, discharge_date, study_date, indication, comparison, impression,\n" +
            "conclusions, findings.\n" +
            "\n" +
            "# De-identified text\n" +
            "MIMIC-III masks PHI with bracketed placeholders like [**2150-8-10**] (dates) or\n" +
            "[**Known lastname 1234**] (names/IDs).\n" +
            "- Treat a bracketed date as the note's real date — use it, normalized to YYYY-MM-DD.\n" +
            "- Treat bracketed names/IDs/locations as redacted — never extract them as values.\n" +
            "\n" +
            "# Dates\n" +
            "- Output dates as YYYY-MM-DD, exactly as written. Do not shift or reconcile.";

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

        // The model returns an open list of {column_name, value} pairs per note, so the
        // schema is a fixed envelope ("fields" array) around a variable set of fields.
        ObjectNode format = mapper.createObjectNode();
        format.put("type", "object");
        ObjectNode fields = format.putObject("properties").putObject("fields");
        fields.put("type", "array");
        ObjectNode items = fields.putObject("items");
        items.put("type", "object");
        ObjectNode itemProps = items.putObject("properties");
        itemProps.putObject("column_name").put("type", "string");
        itemProps.putObject("value").put("type", "string");
        items.putArray("required").add("column_name").add("value");
        format.putArray("required").add("fields");

        LlmClient llmClient = new OllamaClient(endpoint, model);

        // Pass 1: extract each row's fields, in row order, collecting the union of column
        // names. Columns are not known up front because the LLM chooses them per note.
        List<Map<String, String>> rowFields = new ArrayList<>();
        Set<String> columnNames = new LinkedHashSet<>();          // first-seen order
        for (Row row : input) {
            Map<String, String> extracted = new LinkedHashMap<>();
            String text = row.getString(textColName);
            if (text != null && !text.trim().isEmpty()) {         // don't call the LLM on nothing
                try {
                    String json = llmClient.generate(systemPrompt, "Clinical note:\n\n" + text, format);
                    JsonNode arr = mapper.readTree(json).path("fields");
                    if (arr.isArray()) {
                        for (JsonNode item : arr) {
                            // normalize the model's label to a stable snake_case key
                            String name = item.path("column_name").asText("").toLowerCase()
                                    .replaceAll("[^a-z0-9]+", "_")
                                    .replaceAll("^_+|_+$", "");
                            if (!name.isEmpty()) {
                                extracted.put(name, item.path("value").asText(""));   // last value wins
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("LlmEnrich: skipping row " + row.getRowNumber()
                            + " — " + e.getMessage());
                }
            }
            rowFields.add(extracted);
            columnNames.addAll(extracted.keySet());
        }

        // Pass 2: one StringColumn per discovered name, empty where a row lacked it.
        Table result = input.copy();
        for (String name : columnNames) {
            String colName = name;
            while (result.columnNames().contains(colName)) {
                colName = colName + "_llm";                       // never clobber a source column
            }
            StringColumn col = StringColumn.create(colName);
            for (Map<String, String> extracted : rowFields) {
                col.append(extracted.getOrDefault(name, ""));
            }
            result.addColumns(col);
        }
        return result;
    }
}
