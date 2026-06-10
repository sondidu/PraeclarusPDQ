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
                parseObjectsJson(doc);
            } else {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document doc = builder.parse(is);
                doc.getDocumentElement().normalize();

                parseObjectsXml(doc);
            }

            // Attributes the object's own type owns but this object never set
            // become an empty history "[]"; other types' columns stay missing.
            fillOwnTypeEmpties();

            Table table = Table.create(new ArrayList<>(_columns.values()));
            getAuxiliaryDatasets().put("ocel:objects", table);
            return table;
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
     * Iterates through the top-level <objects> section, adding one row per
     * object.
     */
    private void parseObjectsXml(Document doc) {
        Element root = doc.getDocumentElement();
        Element objectsSection = getChildElementXml(root, "objects");
        if (objectsSection == null) return;

        NodeList children = objectsSection.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            if (!(children.item(i) instanceof Element)) continue;
            Element objectEl = (Element) children.item(i);
            if (!objectEl.getTagName().equals("object")) continue;
            parseObjectXml(objectEl);
        }
    }

    /**
     * Parses a single object element into one table row.
     */
    private void parseObjectXml(Element object) {
        String type = object.getAttribute("type");

        // Core OCEL fields
        getStringColumn("ocel:oid").append(object.getAttribute("id"));
        getStringColumn("ocel:type").append(type);

        // Type-specific attributes (column names are prefixed by type)
        Element attributesEl = getChildElementXml(object, "attributes");
        if (attributesEl != null) {
            parseObjectAttributesXml(type, attributesEl);
        }

        // Object-to-object relationships (same wrapper convention as events)
        Element relationshipsEl = getChildElementXml(object, "objects");
        List<String[]> relationships = relationshipsEl != null
                ? extractRelationshipsXml(relationshipsEl) : Collections.emptyList();

        getStringColumn("ocel:object-relationships").append(buildRelationshipsJson(relationships));

        padColumns(getRowCount());
    }

    /**
     * Collects the full attribute history for the current object. Each
     * attribute name maps to a JSON array of {"time","value"} entries in
     * document order, preserving time-varying attributes instead of collapsing
     * them last-value-wins. The array is written as a string to the prefixed
     * column "<objectType>:<attrName>".
     */
    private void parseObjectAttributesXml(String objectType, Element attributesEl) {
        Map<String, ArrayNode> histories = new LinkedHashMap<>();
        NodeList attrs = attributesEl.getElementsByTagName("attribute");
        for (int i = 0; i < attrs.getLength(); i++) {
            Element attr = (Element) attrs.item(i);
            ObjectNode entry = _mapper.createObjectNode();
            entry.put("time", attr.getAttribute("time"));
            entry.put("value", attr.getTextContent().trim());
            histories.computeIfAbsent(attr.getAttribute("name"),
                    k -> _mapper.createArrayNode()).add(entry);
        }
        for (Map.Entry<String, ArrayNode> entry : histories.entrySet()) {
            getStringColumn(objectType + ":" + entry.getKey())
                    .append(entry.getValue().toString());
        }
    }

    /**
     * Extracts object-id and qualifier pairs from an <objects> wrapper
     * element. Handles both "qualifier" and "relationship" attribute names.
     */
    private List<String[]> extractRelationshipsXml(Element relationshipsEl) {
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
     * Collects the full attribute history for the current JSON object,
     * mirroring the XML path: each attribute name maps to a JSON array of
     * {"time","value"} entries in document order, written as a string to the
     * prefixed column.
     */
    private void parseObjectAttributesJson(String objectType, JsonNode attrs) {
        Map<String, ArrayNode> histories = new LinkedHashMap<>();
        for (JsonNode a : attrs) {
            ObjectNode entry = _mapper.createObjectNode();
            entry.put("time", a.path("time").asText(""));
            entry.put("value", a.path("value").asText(""));
            histories.computeIfAbsent(a.path("name").asText(""),
                    k -> _mapper.createArrayNode()).add(entry);
        }
        for (Map.Entry<String, ArrayNode> entry : histories.entrySet()) {
            getStringColumn(objectType + ":" + entry.getKey())
                    .append(entry.getValue().toString());
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

    // ---- XML helpers ----

    private Element getChildElementXml(Element parent, String tagName) {
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

    /**
     * Converts still-missing attribute cells into an empty history "[]" when the
     * column belongs to the row's own object type, leaving cells that belong to
     * other object types missing. Runs once after all objects are parsed, so the
     * full (lazily created) set of attribute columns is known.
     */
    private void fillOwnTypeEmpties() {
        StringColumn typeCol = getStringColumn("ocel:type");
        for (Column<?> column : _columns.values()) {
            String name = column.name();
            int idx = name.indexOf(':');
            if (idx <= 0 || name.startsWith("ocel:")) continue;
            String columnType = name.substring(0, idx);
            StringColumn attrCol = (StringColumn) column;
            for (int row = 0; row < typeCol.size(); row++) {
                if (attrCol.isMissing(row) && columnType.equals(typeCol.get(row))) {
                    attrCol.set(row, "[]");
                }
            }
        }
    }
}
