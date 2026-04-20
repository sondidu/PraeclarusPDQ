package com.processdataquality.praeclarus.reader;

import com.processdataquality.praeclarus.annotation.Plugin;
import com.processdataquality.praeclarus.option.FileOption;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import tech.tablesaw.io.ReadOptions;

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
        name = "OCEL Events Reader",
        author = "Sean Dewantoro",
        version = "1.0",
        synopsis = "Loads an event log stored in OCEL 2.0 format (XML or JSON).",
        fileDescriptors = "OCEL Files;application/xml;.xml;OCEL Files;application/xml;.xmlocel;OCEL Files;application/json;.jsonocel"
)
public class OcelEventsReader extends AbstractDataReader {

    private final Map<String, Column<?>> _columns = new LinkedHashMap<>();
    private final Map<String, String> _attributeTypes = new HashMap<>();
    private final Map<String, String> _objectIdToType = new HashMap<>();
    private final ObjectMapper _mapper = new ObjectMapper();

    public OcelEventsReader() {
        super();
        addDefaultOptions();
    }

    @Override
    public Table read() throws IOException {
        try {
            InputStream is = getSourceAsInputStream();
            if (is == null) {
                throw new IOException("No OCEL input source specified");
            }

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(is);
            doc.getDocumentElement().normalize();

            readEventTypes(doc);
            readObjectIdTypes(doc);
            parseEvents(doc);

            return Table.create(new ArrayList<>(_columns.values()));
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to load OCEL XML file", e);
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
     * Reads event-type definitions to build an attribute name -> type map.
     * This allows creating properly typed columns for event attributes.
     */
    private void readEventTypes(Document doc) {
        NodeList eventTypes = doc.getElementsByTagName("event-type");
        for (int i = 0; i < eventTypes.getLength(); i++) {
            Element eventType = (Element) eventTypes.item(i);
            NodeList attrs = eventType.getElementsByTagName("attribute");
            for (int j = 0; j < attrs.getLength(); j++) {
                Element attr = (Element) attrs.item(j);
                String name = attr.getAttribute("name");
                String type = attr.getAttribute("type");
                if (!type.isEmpty()) {
                    _attributeTypes.put(name, type);
                }
            }
        }
    }

    /**
     * Builds an object-id to object-type lookup from the top-level <objects>
     * section. Required to group each event's relationships into the correct
     * per-object-type column, since event <relationship> elements only carry
     * the object-id, not its type.
     */
    private void readObjectIdTypes(Document doc) {
        Element root = doc.getDocumentElement();
        Element objectsSection = getChildElement(root, "objects");
        if (objectsSection == null) return;

        NodeList children = objectsSection.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            if (!(children.item(i) instanceof Element)) continue;
            Element objectEl = (Element) children.item(i);
            if (!objectEl.getTagName().equals("object")) continue;
            String id = objectEl.getAttribute("id");
            String type = objectEl.getAttribute("type");
            if (!id.isEmpty() && !type.isEmpty()) {
                _objectIdToType.put(id, type);
            }
        }
    }

    /**
     * Iterates through all event elements and adds each as a row.
     */
    private void parseEvents(Document doc) {
        NodeList events = doc.getElementsByTagName("event");
        for (int i = 0; i < events.getLength(); i++) {
            Node node = events.item(i);
            if (node instanceof Element && node.getParentNode().getNodeName().equals("events")) {
                parseEvent((Element) node);
            }
        }
    }

    /**
     * Parses a single event element into one table row.
     */
    private void parseEvent(Element event) {
        // Core OCEL fields
        getStringColumn("ocel:eid").append(event.getAttribute("id"));
        getStringColumn("ocel:activity").append(event.getAttribute("type"));
        getDateTimeColumn("ocel:timestamp").append(parseTimestamp(event.getAttribute("time")));

        // Event attributes
        Element attributesEl = getChildElement(event, "attributes");
        if (attributesEl != null) {
            parseEventAttributes(attributesEl);
        }

        // Event-to-object relationships, grouped into one column per object
        // type. Each cell holds a JSON array of {objectId, qualifier}. Types
        // the event does not reference are left missing via padColumns below.
        Element objectsEl = getChildElement(event, "objects");
        List<String[]> relationships = objectsEl != null
                ? extractRelationships(objectsEl) : Collections.emptyList();

        Map<String, List<String[]>> byType = new LinkedHashMap<>();
        for (String[] rel : relationships) {
            String type = _objectIdToType.get(rel[0]);
            if (type == null) continue; // referenced id not declared in <objects>
            byType.computeIfAbsent(type, k -> new ArrayList<>()).add(rel);
        }
        for (Map.Entry<String, List<String[]>> entry : byType.entrySet()) {
            getStringColumn(entry.getKey()).append(buildRelationshipsJson(entry.getValue()));
        }

        padColumns(getRowCount());
    }

    /**
     * Appends typed values for each attribute in the attributes element.
     */
    private void parseEventAttributes(Element attributesEl) {
        NodeList attrs = attributesEl.getElementsByTagName("attribute");
        for (int i = 0; i < attrs.getLength(); i++) {
            Element attr = (Element) attrs.item(i);
            String name = attr.getAttribute("name");
            String value = attr.getTextContent().trim();
            appendTypedValue(name, value);
        }
    }

    /**
     * Extracts object-id and qualifier pairs from an event's objects element.
     * Handles both "qualifier" and "relationship" attribute names.
     */
    private List<String[]> extractRelationships(Element objectsEl) {
        List<String[]> relationships = new ArrayList<>();
        NodeList children = objectsEl.getChildNodes();
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

    // ---- Typed value handling ----

    /**
     * Appends a value to the appropriately typed column based on the
     * attribute type from the event-type definitions.
     */
    private void appendTypedValue(String name, String value) {
        String type = _attributeTypes.getOrDefault(name, "string");

        if (value == null || value.isEmpty()) {
            getColumnForType(name, type).appendMissing();
            return;
        }

        try {
            switch (type) {
                case "integer":
                    getLongColumn(name).append(Long.parseLong(value));
                    break;
                case "float":
                    getDoubleColumn(name).append(Double.parseDouble(value));
                    break;
                case "boolean":
                    getBooleanColumn(name).append(Boolean.parseBoolean(value));
                    break;
                case "time":
                    getDateTimeColumn(name).append(parseTimestamp(value));
                    break;
                default:
                    getStringColumn(name).append(value);
                    break;
            }
        } catch (NumberFormatException | DateTimeParseException e) {
            getColumnForType(name, type).appendMissing();
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
        StringColumn eid = (StringColumn) _columns.get("ocel:eid");
        return eid != null ? eid.size() : 0;
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
