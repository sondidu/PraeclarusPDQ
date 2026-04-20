package com.processdataquality.praeclarus.reader;

import com.processdataquality.praeclarus.annotation.Plugin;
import com.processdataquality.praeclarus.option.FileOption;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import tech.tablesaw.io.ReadOptions;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * @author Sean Dewantoro
 * @date 25/3/26
 */
@Plugin(
        name = "OCEL Objects Reader",
        author = "Sean Dewantoro",
        version = "1.0",
        synopsis = "Loads objects from an OCEL 2.0 log (XML or JSON).",
        fileDescriptors = "OCEL Files;application/xml;.xml;OCEL Files;application/xml;.xmlocel;OCEL Files;application/json;.jsonocel"
)
public class OcelObjectsReader extends AbstractDataReader {

    private final Map<String, Column<?>> _columns = new LinkedHashMap<>();
    // Keyed by the prefixed column name "<objectType>:<attrName>" (e.g. "Invoice:amount")
    // so that attributes with the same name in different object types stay distinct.
    private final Map<String, String> _attributeTypes = new HashMap<>();
    private final ObjectMapper _mapper = new ObjectMapper();

    public OcelObjectsReader() {
        super();
        addDefaultOptions();
    }

    @Override
    public Table read() throws IOException {
        try {
            InputStream raw = getSourceAsInputStream();
            if (raw == null) {
                throw new IOException("No OCEL input source specified");
            }
            BufferedInputStream is = new BufferedInputStream(raw);

            // Seed the core columns first so they appear at the start of the
            // output table, ahead of any type-specific attribute columns.
            getStringColumn("ocel:oid");
            getStringColumn("ocel:type");
            getStringColumn("ocel:object-relationships");

            if (isJson(is)) {
                JsonNode doc = _mapper.readTree(is);
                readObjectTypesJson(doc);
                parseObjectsJson(doc);
            } else {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document doc = builder.parse(is);
                doc.getDocumentElement().normalize();

                readObjectTypes(doc);
                parseObjects(doc);
            }

            return Table.create(new ArrayList<>(_columns.values()));
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to load OCEL file", e);
        }
    }

    @Override
    protected void addDefaultOptions() {
        getOptions().addDefault(new FileOption("Source", ""));
    }

    @Override
    protected ReadOptions getReadOptions() {
        return null;
    }

    // ---- XML parsing ----

    /**
     * Reads object-type definitions and records each attribute's OCEL type
     * under the prefixed key "<objectType>:<attrName>" so that typed columns
     * can be created during object parsing without cross-type collisions.
     */
    private void readObjectTypes(Document doc) {
        NodeList objectTypes = doc.getElementsByTagName("object-type");
        for (int i = 0; i < objectTypes.getLength(); i++) {
            Element objectType = (Element) objectTypes.item(i);
            String typeName = objectType.getAttribute("name");
            NodeList attrs = objectType.getElementsByTagName("attribute");
            for (int j = 0; j < attrs.getLength(); j++) {
                Element attr = (Element) attrs.item(j);
                String name = attr.getAttribute("name");
                String type = attr.getAttribute("type");
                if (!type.isEmpty()) {
                    _attributeTypes.put(typeName + ":" + name, type);
                }
            }
        }
    }

    /**
     * Iterates through the top-level <objects> section, adding one row per
     * object.
     */
    private void parseObjects(Document doc) {
        Element root = doc.getDocumentElement();
        Element objectsSection = getChildElement(root, "objects");
        if (objectsSection == null) return;

        NodeList children = objectsSection.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            if (!(children.item(i) instanceof Element)) continue;
            Element objectEl = (Element) children.item(i);
            if (!objectEl.getTagName().equals("object")) continue;
            parseObject(objectEl);
        }
    }

    /**
     * Parses a single object element into one table row.
     */
    private void parseObject(Element object) {
        String type = object.getAttribute("type");

        // Core OCEL fields
        getStringColumn("ocel:oid").append(object.getAttribute("id"));
        getStringColumn("ocel:type").append(type);

        // Type-specific attributes (column names are prefixed by type)
        Element attributesEl = getChildElement(object, "attributes");
        if (attributesEl != null) {
            parseObjectAttributes(type, attributesEl);
        }

        // Object-to-object relationships (same wrapper convention as events)
        Element relationshipsEl = getChildElement(object, "objects");
        List<String[]> relationships = relationshipsEl != null
                ? extractRelationships(relationshipsEl) : Collections.emptyList();

        getStringColumn("ocel:object-relationships").append(buildRelationshipsJson(relationships));

        padColumns(getRowCount());
    }

    /**
     * Collects typed attribute values for the current object and writes one
     * value per attribute name to the corresponding prefixed column.
     *
     * Time-varying attributes (same name repeated with different time=...
     * values) are resolved by last-value-wins: later <attribute> entries
     * override earlier ones. Document order is assumed to reflect temporal
     * order; a stricter implementation could compare the "time" attributes
     * directly.
     */
    private void parseObjectAttributes(String objectType, Element attributesEl) {
        Map<String, String> latest = new LinkedHashMap<>();
        NodeList attrs = attributesEl.getElementsByTagName("attribute");
        for (int i = 0; i < attrs.getLength(); i++) {
            Element attr = (Element) attrs.item(i);
            latest.put(attr.getAttribute("name"), attr.getTextContent().trim());
        }
        for (Map.Entry<String, String> entry : latest.entrySet()) {
            appendTypedValue(objectType + ":" + entry.getKey(), entry.getValue());
        }
    }

    /**
     * Extracts object-id and qualifier pairs from an <objects> wrapper
     * element. Handles both "qualifier" and "relationship" attribute names.
     */
    private List<String[]> extractRelationships(Element relationshipsEl) {
        List<String[]> relationships = new ArrayList<>();
        NodeList children = relationshipsEl.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            if (!(children.item(i) instanceof Element)) continue;
            Element el = (Element) children.item(i);
            String tag = el.getTagName();
            if (tag.equals("relationship") || tag.equals("object")) {
                String objectId = el.getAttribute("object-id");
                String qualifier = el.getAttribute("qualifier");
                if (qualifier.isEmpty()) {
                    qualifier = el.getAttribute("relationship");
                }
                relationships.add(new String[]{objectId, qualifier});
            }
        }
        return relationships;
    }

    /**
     * Builds a JSON array of {"objectId", "qualifier"} objects.
     */
    private String buildRelationshipsJson(List<String[]> relationships) {
        ArrayNode array = _mapper.createArrayNode();
        for (String[] rel : relationships) {
            ObjectNode obj = _mapper.createObjectNode();
            obj.put("objectId", rel[0]);
            obj.put("qualifier", rel[1]);
            array.add(obj);
        }
        return array.toString();
    }

    // ---- JSON parsing ----

    /**
     * Peeks at the first non-whitespace byte to decide whether the stream
     * holds JSON ('{' or '[') or XML. Resets the stream so the chosen parser
     * sees the full content.
     */
    private boolean isJson(BufferedInputStream is) throws IOException {
        is.mark(64);
        int b;
        do { b = is.read(); } while (b != -1 && Character.isWhitespace(b));
        is.reset();
        return b == '{' || b == '[';
    }

    /**
     * Reads object-type definitions from the JSON objectTypes array, building
     * the prefixed "<objectType>:<attrName>" to OCEL type map used when
     * creating typed columns during parseObjectJson.
     */
    private void readObjectTypesJson(JsonNode doc) {
        JsonNode objectTypes = doc.get("objectTypes");
        if (objectTypes == null || !objectTypes.isArray()) return;
        for (JsonNode ot : objectTypes) {
            String typeName = ot.path("name").asText("");
            JsonNode attrs = ot.get("attributes");
            if (attrs == null || !attrs.isArray()) continue;
            for (JsonNode a : attrs) {
                String name = a.path("name").asText("");
                String type = a.path("type").asText("");
                if (!name.isEmpty() && !type.isEmpty()) {
                    _attributeTypes.put(typeName + ":" + name, type);
                }
            }
        }
    }

    /**
     * Iterates through the JSON objects array, adding one row per object.
     */
    private void parseObjectsJson(JsonNode doc) {
        JsonNode objects = doc.get("objects");
        if (objects == null || !objects.isArray()) return;
        for (JsonNode object : objects) {
            parseObjectJson(object);
        }
    }

    /**
     * Parses a single JSON object into one table row.
     */
    private void parseObjectJson(JsonNode object) {
        String type = object.path("type").asText("");

        getStringColumn("ocel:oid").append(object.path("id").asText(""));
        getStringColumn("ocel:type").append(type);

        JsonNode attrs = object.get("attributes");
        if (attrs != null && attrs.isArray()) {
            parseObjectAttributesJson(type, attrs);
        }

        JsonNode rels = object.get("relationships");
        List<String[]> relationships = extractRelationshipsJson(rels);
        getStringColumn("ocel:object-relationships").append(buildRelationshipsJson(relationships));

        padColumns(getRowCount());
    }

    /**
     * Collects typed attribute values for the current JSON object with
     * last-value-wins semantics for time-varying attributes, matching the XML
     * path. Document order is assumed to reflect temporal order.
     */
    private void parseObjectAttributesJson(String objectType, JsonNode attrs) {
        Map<String, String> latest = new LinkedHashMap<>();
        for (JsonNode a : attrs) {
            latest.put(a.path("name").asText(""), a.path("value").asText(""));
        }
        for (Map.Entry<String, String> entry : latest.entrySet()) {
            appendTypedValue(objectType + ":" + entry.getKey(), entry.getValue());
        }
    }

    /**
     * Extracts object-id and qualifier pairs from a JSON relationships array.
     */
    private List<String[]> extractRelationshipsJson(JsonNode rels) {
        List<String[]> relationships = new ArrayList<>();
        if (rels == null || !rels.isArray()) return relationships;
        for (JsonNode r : rels) {
            relationships.add(new String[]{
                    r.path("objectId").asText(""),
                    r.path("qualifier").asText("")
            });
        }
        return relationships;
    }

    // ---- Typed value handling ----

    /**
     * Appends a value to the appropriately typed column based on the OCEL
     * attribute type (looked up by the prefixed column name). Missing or
     * unknown types default to string.
     */
    private void appendTypedValue(String columnName, String value) {
        String type = _attributeTypes.getOrDefault(columnName, "string");

        if (value == null || value.isEmpty()) {
            getColumnForType(columnName, type).appendMissing();
            return;
        }

        try {
            switch (type) {
                case "integer":
                    getLongColumn(columnName).append(Long.parseLong(value));
                    break;
                case "float":
                    getDoubleColumn(columnName).append(Double.parseDouble(value));
                    break;
                case "boolean":
                    getBooleanColumn(columnName).append(Boolean.parseBoolean(value));
                    break;
                case "time":
                case "date":
                    getDateTimeColumn(columnName).append(parseTimestamp(value));
                    break;
                default:
                    getStringColumn(columnName).append(value);
                    break;
            }
        } catch (NumberFormatException | DateTimeParseException e) {
            getColumnForType(columnName, type).appendMissing();
        }
    }

    private Column<?> getColumnForType(String name, String type) {
        switch (type) {
            case "integer":
                return getLongColumn(name);
            case "float":
                return getDoubleColumn(name);
            case "boolean":
                return getBooleanColumn(name);
            case "time":
            case "date":
                return getDateTimeColumn(name);
            default:
                return getStringColumn(name);
        }
    }

    // ---- Timestamp parsing ----

    private LocalDateTime parseTimestamp(String timeStr) {
        // Normalize: replace space separator with T, strip timezone suffix
        String normalized = timeStr.trim().replace(" ", "T");
        normalized = normalized.replaceAll("(Z|[+-]\\d{2}(:\\d{2})?)$", "");
        return LocalDateTime.parse(normalized, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    // ---- XML helpers ----

    private Element getChildElement(Element parent, String tagName) {
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i) instanceof Element) {
                Element child = (Element) children.item(i);
                if (child.getTagName().equals(tagName)) {
                    return child;
                }
            }
        }
        return null;
    }

    // ---- Column management ----

    private StringColumn getStringColumn(String name) {
        StringColumn column = (StringColumn) _columns.get(name);
        if (column == null) {
            column = StringColumn.create(name);
            addColumn(column);
        }
        return column;
    }

    private DateTimeColumn getDateTimeColumn(String name) {
        DateTimeColumn column = (DateTimeColumn) _columns.get(name);
        if (column == null) {
            column = DateTimeColumn.create(name);
            addColumn(column);
        }
        return column;
    }

    private LongColumn getLongColumn(String name) {
        LongColumn column = (LongColumn) _columns.get(name);
        if (column == null) {
            column = LongColumn.create(name);
            addColumn(column);
        }
        return column;
    }

    private DoubleColumn getDoubleColumn(String name) {
        DoubleColumn column = (DoubleColumn) _columns.get(name);
        if (column == null) {
            column = DoubleColumn.create(name);
            addColumn(column);
        }
        return column;
    }

    private BooleanColumn getBooleanColumn(String name) {
        BooleanColumn column = (BooleanColumn) _columns.get(name);
        if (column == null) {
            column = BooleanColumn.create(name);
            addColumn(column);
        }
        return column;
    }

    private void addColumn(Column<?> column) {
        _columns.put(column.name(), column);
        padColumn(column, getRowCount() - 1);
    }

    private int getRowCount() {
        StringColumn oid = (StringColumn) _columns.get("ocel:oid");
        return oid != null ? oid.size() : 0;
    }

    private void padColumns(int count) {
        for (Column<?> column : _columns.values()) {
            if (column.size() < count) {
                padColumn(column, count);
            }
        }
    }

    private void padColumn(Column<?> column, int count) {
        for (int i = column.size(); i < count; i++) {
            column.appendMissing();
        }
    }
}
