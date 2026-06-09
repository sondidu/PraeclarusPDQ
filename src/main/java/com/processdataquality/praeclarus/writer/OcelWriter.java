package com.processdataquality.praeclarus.writer;

import com.processdataquality.praeclarus.annotation.Plugin;
import com.processdataquality.praeclarus.util.DataCollection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import tech.tablesaw.api.DateTimeColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.columns.booleans.BooleanColumnType;
import tech.tablesaw.columns.datetimes.DateTimeColumnType;
import tech.tablesaw.columns.numbers.DoubleColumnType;
import tech.tablesaw.columns.numbers.LongColumnType;
import tech.tablesaw.io.WriteOptions;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * @author Sean Dewantoro
 * @date 26/4/28
 */
@Plugin(
        name = "OCEL Writer",
        author = "Sean Dewantoro",
        version = "1.0",
        synopsis = "Writes an OCEL 2.0 file (XML or JSON, chosen by destination " +
                "filename extension). Takes a single predecessor whose output " +
                "is the events table — an OCEL Events Reader directly, or an " +
                "OCEL Combiner to also include objects. An objects section is " +
                "written when an objects table is supplied via auxData under " +
                "the key \"ocel:objects\" (the OCEL Combiner does this).",
        // FileSaveEditor renders a single-type save dialog only, so this
        // multi-type descriptor degrades to an unfiltered picker. That is the
        // intended behaviour: the output format (XML vs JSON) is chosen from
        // the destination filename extension — see isJsonDestination().
        fileDescriptors = "OCEL XML Files;application/xml;.xmlocel;" +
                "OCEL JSON Files;application/json;.jsonocel"
)
public class OcelWriter extends AbstractDataWriter {

    private final ObjectMapper _mapper = new ObjectMapper();

    public OcelWriter() {
        super();
    }

    @Override
    protected WriteOptions getWriteOptions() {
        return null;
    }

    @Override
    public void write(Table table, DataCollection auxData) throws IOException {
        // The single predecessor's output is the events table (WriterNode
        // passes it as `table`). Objects are optional and arrive via auxData
        // under "ocel:objects" — the OCEL Combiner puts them there. If absent,
        // the writer emits empty object-types and objects sections, but events
        // (with their relationships) are written as-is.
        Table events = table;
        Table objects = auxData.getTable("ocel:objects");

        if (events == null) {
            throw new IOException("OcelWriter: no events table found. Connect " +
                    "a predecessor that outputs the OCEL events table (an OCEL " +
                    "Events Reader, or an OCEL Combiner).");
        }

        if (isJsonDestination()) {
            writeJson(events, objects);
        } else {
            writeXml(events, objects);
        }
    }

    private boolean isJsonDestination() {
        try {
            String name = getOptions().get("Destination").asString();
            if (name == null) return false;
            String lower = name.toLowerCase();
            return lower.endsWith(".json") || lower.endsWith(".jsonocel");
        } catch (Exception e) {
            return false;
        }
    }

    // ---- XML writing ----

    private void writeXml(Table events, Table objects) throws IOException {
        try {
            Document doc = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder().newDocument();

            Element root = doc.createElement("log");
            doc.appendChild(root);

            writeObjectTypesXml(doc, root, objects);
            writeEventTypesXml(doc, root, events);
            writeObjectsXml(doc, root, objects);
            writeEventsXml(doc, root, events);

            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(
                    "{http://xml.apache.org/xslt}indent-amount", "2");

            try (OutputStream out = getDestinationAsOutputStream()) {
                transformer.transform(new DOMSource(doc), new StreamResult(out));
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to write OCEL XML", e);
        }
    }

    private void writeObjectTypesXml(Document doc, Element root, Table objects) {
        Element objectTypesEl = doc.createElement("object-types");
        root.appendChild(objectTypesEl);
        if (objects == null) return;

        for (Map.Entry<String, List<Column<?>>> entry :
                collectObjectAttrColumnsByType(objects).entrySet()) {
            String typeName = entry.getKey();
            Element typeEl = doc.createElement("object-type");
            typeEl.setAttribute("name", typeName);
            Element attrsEl = doc.createElement("attributes");
            typeEl.appendChild(attrsEl);
            for (Column<?> col : entry.getValue()) {
                Element attrEl = doc.createElement("attribute");
                attrEl.setAttribute("name", stripPrefix(col.name(), typeName));
                attrEl.setAttribute("type", ocelTypeFor(col));
                attrsEl.appendChild(attrEl);
            }
            objectTypesEl.appendChild(typeEl);
        }
    }

    private void writeEventTypesXml(Document doc, Element root, Table events) {
        Element eventTypesEl = doc.createElement("event-types");
        root.appendChild(eventTypesEl);

        for (Map.Entry<String, List<Column<?>>> entry :
                collectEventAttrColumnsByType(events).entrySet()) {
            Element typeEl = doc.createElement("event-type");
            typeEl.setAttribute("name", entry.getKey());
            Element attrsEl = doc.createElement("attributes");
            typeEl.appendChild(attrsEl);
            for (Column<?> col : entry.getValue()) {
                Element attrEl = doc.createElement("attribute");
                attrEl.setAttribute("name", col.name());
                attrEl.setAttribute("type", ocelTypeFor(col));
                attrsEl.appendChild(attrEl);
            }
            eventTypesEl.appendChild(typeEl);
        }
    }

    private void writeObjectsXml(Document doc, Element root, Table objects) {
        Element objectsEl = doc.createElement("objects");
        root.appendChild(objectsEl);
        if (objects == null) return;

        StringColumn oidCol = (StringColumn) objects.column("ocel:oid");
        StringColumn typeCol = (StringColumn) objects.column("ocel:type");
        StringColumn relCol = (StringColumn) objects.column("ocel:object-relationships");
        Map<String, List<Column<?>>> attrsByType = collectObjectAttrColumnsByType(objects);

        for (int row = 0; row < objects.rowCount(); row++) {
            String type = typeCol.get(row);
            Element objectEl = doc.createElement("object");
            objectEl.setAttribute("id", oidCol.get(row));
            objectEl.setAttribute("type", type);

            Element attrsEl = doc.createElement("attributes");
            for (Column<?> col : attrsByType.getOrDefault(type, Collections.emptyList())) {
                String history = cellToString(col, row);
                if (history == null) continue;
                String attrName = stripPrefix(col.name(), type);
                for (String[] entry : parseHistory(history)) {
                    Element attrEl = doc.createElement("attribute");
                    attrEl.setAttribute("name", attrName);
                    attrEl.setAttribute("time", entry[0]);
                    attrEl.setTextContent(entry[1]);
                    attrsEl.appendChild(attrEl);
                }
            }
            if (attrsEl.hasChildNodes()) objectEl.appendChild(attrsEl);

            List<String[]> rels = parseRelationships(cellToString(relCol, row));
            if (!rels.isEmpty()) {
                Element relsEl = doc.createElement("objects");
                for (String[] rel : rels) {
                    Element relEl = doc.createElement("relationship");
                    relEl.setAttribute("object-id", rel[0]);
                    relEl.setAttribute("qualifier", rel[1]);
                    relsEl.appendChild(relEl);
                }
                objectEl.appendChild(relsEl);
            }
            objectsEl.appendChild(objectEl);
        }
    }

    private void writeEventsXml(Document doc, Element root, Table events) {
        Element eventsEl = doc.createElement("events");
        root.appendChild(eventsEl);

        StringColumn eidCol = (StringColumn) events.column("ocel:eid");
        StringColumn activityCol = (StringColumn) events.column("ocel:activity");
        DateTimeColumn timeCol = (DateTimeColumn) events.column("ocel:timestamp");
        Map<String, List<Column<?>>> attrsByType = collectEventAttrColumnsByType(events);
        List<Column<?>> relCols = relationshipColumns(events);

        for (int row = 0; row < events.rowCount(); row++) {
            String type = activityCol.get(row);
            Element eventEl = doc.createElement("event");
            eventEl.setAttribute("id", eidCol.get(row));
            eventEl.setAttribute("type", type);
            LocalDateTime dt = timeCol.get(row);
            if (dt != null) eventEl.setAttribute("time", formatTimestamp(dt));

            Element attrsEl = doc.createElement("attributes");
            for (Column<?> col : attrsByType.getOrDefault(type, Collections.emptyList())) {
                String value = cellToString(col, row);
                if (value == null) continue;
                Element attrEl = doc.createElement("attribute");
                attrEl.setAttribute("name", col.name());
                attrEl.setTextContent(value);
                attrsEl.appendChild(attrEl);
            }
            if (attrsEl.hasChildNodes()) eventEl.appendChild(attrsEl);

            Element relsEl = doc.createElement("objects");
            for (Column<?> col : relCols) {
                for (String[] rel : parseRelationships(cellToString(col, row))) {
                    Element relEl = doc.createElement("relationship");
                    relEl.setAttribute("object-id", rel[0]);
                    relEl.setAttribute("qualifier", rel[1]);
                    relsEl.appendChild(relEl);
                }
            }
            if (relsEl.hasChildNodes()) eventEl.appendChild(relsEl);
            eventsEl.appendChild(eventEl);
        }
    }

    // ---- JSON writing ----

    private void writeJson(Table events, Table objects) throws IOException {
        ObjectNode root = _mapper.createObjectNode();
        root.set("objectTypes", buildObjectTypesJson(objects));
        root.set("eventTypes", buildEventTypesJson(events));
        root.set("objects", buildObjectsJson(objects));
        root.set("events", buildEventsJson(events));

        // Serialise to a string then write+flush ourselves. Going through
        // ObjectMapper.writeValue(OutputStream,...) calls close() on the
        // stream without calling flush(), and the destination's LogWriter
        // only ships bytes to the browser when flush() fires.
        String json = _mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        try (OutputStream out = getDestinationAsOutputStream()) {
            out.write(json.getBytes(StandardCharsets.UTF_8));
            out.flush();
        }
    }

    private ArrayNode buildObjectTypesJson(Table objects) {
        ArrayNode array = _mapper.createArrayNode();
        if (objects == null) return array;
        for (Map.Entry<String, List<Column<?>>> entry :
                collectObjectAttrColumnsByType(objects).entrySet()) {
            String typeName = entry.getKey();
            ObjectNode type = _mapper.createObjectNode();
            type.put("name", typeName);
            ArrayNode attrs = _mapper.createArrayNode();
            for (Column<?> col : entry.getValue()) {
                ObjectNode attr = _mapper.createObjectNode();
                attr.put("name", stripPrefix(col.name(), typeName));
                attr.put("type", ocelTypeFor(col));
                attrs.add(attr);
            }
            type.set("attributes", attrs);
            array.add(type);
        }
        return array;
    }

    private ArrayNode buildEventTypesJson(Table events) {
        ArrayNode array = _mapper.createArrayNode();
        for (Map.Entry<String, List<Column<?>>> entry :
                collectEventAttrColumnsByType(events).entrySet()) {
            ObjectNode type = _mapper.createObjectNode();
            type.put("name", entry.getKey());
            ArrayNode attrs = _mapper.createArrayNode();
            for (Column<?> col : entry.getValue()) {
                ObjectNode attr = _mapper.createObjectNode();
                attr.put("name", col.name());
                attr.put("type", ocelTypeFor(col));
                attrs.add(attr);
            }
            type.set("attributes", attrs);
            array.add(type);
        }
        return array;
    }

    private ArrayNode buildObjectsJson(Table objects) {
        ArrayNode array = _mapper.createArrayNode();
        if (objects == null) return array;
        StringColumn oidCol = (StringColumn) objects.column("ocel:oid");
        StringColumn typeCol = (StringColumn) objects.column("ocel:type");
        StringColumn relCol = (StringColumn) objects.column("ocel:object-relationships");
        Map<String, List<Column<?>>> attrsByType = collectObjectAttrColumnsByType(objects);

        for (int row = 0; row < objects.rowCount(); row++) {
            String type = typeCol.get(row);
            ObjectNode obj = _mapper.createObjectNode();
            obj.put("id", oidCol.get(row));
            obj.put("type", type);

            ArrayNode attrs = _mapper.createArrayNode();
            for (Column<?> col : attrsByType.getOrDefault(type, Collections.emptyList())) {
                String history = cellToString(col, row);
                if (history == null) continue;
                String attrName = stripPrefix(col.name(), type);
                for (String[] entry : parseHistory(history)) {
                    ObjectNode attr = _mapper.createObjectNode();
                    attr.put("name", attrName);
                    attr.put("time", entry[0]);
                    attr.put("value", entry[1]);
                    attrs.add(attr);
                }
            }
            obj.set("attributes", attrs);

            ArrayNode rels = _mapper.createArrayNode();
            for (String[] rel : parseRelationships(cellToString(relCol, row))) {
                ObjectNode r = _mapper.createObjectNode();
                r.put("objectId", rel[0]);
                r.put("qualifier", rel[1]);
                rels.add(r);
            }
            obj.set("relationships", rels);

            array.add(obj);
        }
        return array;
    }

    private ArrayNode buildEventsJson(Table events) {
        ArrayNode array = _mapper.createArrayNode();
        StringColumn eidCol = (StringColumn) events.column("ocel:eid");
        StringColumn activityCol = (StringColumn) events.column("ocel:activity");
        DateTimeColumn timeCol = (DateTimeColumn) events.column("ocel:timestamp");
        Map<String, List<Column<?>>> attrsByType = collectEventAttrColumnsByType(events);
        List<Column<?>> relCols = relationshipColumns(events);

        for (int row = 0; row < events.rowCount(); row++) {
            String type = activityCol.get(row);
            ObjectNode event = _mapper.createObjectNode();
            event.put("id", eidCol.get(row));
            event.put("type", type);
            LocalDateTime dt = timeCol.get(row);
            if (dt != null) event.put("time", formatTimestamp(dt));

            ArrayNode attrs = _mapper.createArrayNode();
            for (Column<?> col : attrsByType.getOrDefault(type, Collections.emptyList())) {
                String value = cellToString(col, row);
                if (value == null) continue;
                ObjectNode attr = _mapper.createObjectNode();
                attr.put("name", col.name());
                attr.put("value", value);
                attrs.add(attr);
            }
            event.set("attributes", attrs);

            ArrayNode rels = _mapper.createArrayNode();
            for (Column<?> col : relCols) {
                for (String[] rel : parseRelationships(cellToString(col, row))) {
                    ObjectNode r = _mapper.createObjectNode();
                    r.put("objectId", rel[0]);
                    r.put("qualifier", rel[1]);
                    rels.add(r);
                }
            }
            event.set("relationships", rels);

            array.add(event);
        }
        return array;
    }

    // ---- Schema introspection (shared by XML and JSON paths) ----

    /**
     * Groups object-attribute columns by their type prefix. A column named
     * "Invoice:amount" belongs to the "Invoice" object-type. Object types that
     * appear in the data but have no attribute columns also get an entry (with
     * an empty list) so they still appear in the schema section.
     */
    private Map<String, List<Column<?>>> collectObjectAttrColumnsByType(Table objects) {
        Map<String, List<Column<?>>> result = new LinkedHashMap<>();
        Set<String> coreCols = new HashSet<>(Arrays.asList("ocel:oid", "ocel:type", "ocel:object-relationships"));
        for (Column<?> col : objects.columns()) {
            String name = col.name();
            if (coreCols.contains(name)) continue;
            int idx = name.indexOf(':');
            if (idx <= 0) continue;
            String type = name.substring(0, idx);
            result.computeIfAbsent(type, k -> new ArrayList<>()).add(col);
        }
        StringColumn typeCol = (StringColumn) objects.column("ocel:type");
        for (int row = 0; row < objects.rowCount(); row++) {
            result.computeIfAbsent(typeCol.get(row), k -> new ArrayList<>());
        }
        return result;
    }

    /**
     * Event attributes are unprefixed and shared across event types in the
     * flat events table, so we cannot infer ownership from column names.
     * Instead we scan rows: an attribute column is recorded against an event
     * type if at least one row of that type has a non-missing value in it.
     */
    private Map<String, List<Column<?>>> collectEventAttrColumnsByType(Table events) {
        List<Column<?>> attrCols = new ArrayList<>();
        for (Column<?> col : events.columns()) {
            String name = col.name();
            if (name.equals("ocel:eid") || name.equals("ocel:activity") || name.equals("ocel:timestamp")) continue;
            if (name.startsWith("ocel:object:")) continue;
            attrCols.add(col);
        }

        StringColumn activityCol = (StringColumn) events.column("ocel:activity");
        Map<String, Set<String>> seen = new LinkedHashMap<>();
        Map<String, List<Column<?>>> result = new LinkedHashMap<>();
        for (int row = 0; row < events.rowCount(); row++) {
            String type = activityCol.get(row);
            Set<String> seenForType = seen.computeIfAbsent(type, k -> new HashSet<>());
            List<Column<?>> attrsForType = result.computeIfAbsent(type, k -> new ArrayList<>());
            for (Column<?> col : attrCols) {
                if (col.isMissing(row)) continue;
                if (seenForType.add(col.name())) {
                    attrsForType.add(col);
                }
            }
        }
        return result;
    }

    private List<Column<?>> relationshipColumns(Table events) {
        List<Column<?>> result = new ArrayList<>();
        for (Column<?> col : events.columns()) {
            if (col.name().startsWith("ocel:object:")) result.add(col);
        }
        return result;
    }

    private String stripPrefix(String columnName, String typeName) {
        return columnName.substring(typeName.length() + 1);
    }

    private String ocelTypeFor(Column<?> col) {
        if (col.type() instanceof LongColumnType) return "integer";
        if (col.type() instanceof DoubleColumnType) return "float";
        if (col.type() instanceof BooleanColumnType) return "boolean";
        if (col.type() instanceof DateTimeColumnType) return "time";
        return "string";
    }

    private String cellToString(Column<?> col, int row) {
        if (col.isMissing(row)) return null;
        if (col.type() instanceof DateTimeColumnType) {
            return formatTimestamp(((DateTimeColumn) col).get(row));
        }
        return String.valueOf(col.get(row));
    }

    private List<String[]> parseRelationships(String json) {
        if (json == null || json.isEmpty()) return Collections.emptyList();
        List<String[]> result = new ArrayList<>();
        try {
            JsonNode arr = _mapper.readTree(json);
            if (!arr.isArray()) return result;
            for (JsonNode rel : arr) {
                result.add(new String[]{
                        rel.path("objectId").asText(""),
                        rel.path("qualifier").asText("")
                });
            }
        } catch (IOException e) {
            // Treat unparseable relationship JSON as none, rather than failing
            // the whole write.
        }
        return result;
    }

    /**
     * Parses an object-attribute history column (a JSON array of
     * {"time","value"} entries produced by the OCEL Objects Reader) into a list
     * of [time, value] pairs. An empty array yields no entries. Unparseable
     * JSON is treated as no entries rather than failing the whole write.
     */
    private List<String[]> parseHistory(String json) {
        if (json == null || json.isEmpty()) return Collections.emptyList();
        List<String[]> result = new ArrayList<>();
        try {
            JsonNode arr = _mapper.readTree(json);
            if (!arr.isArray()) return result;
            for (JsonNode entry : arr) {
                result.add(new String[]{
                        entry.path("time").asText(""),
                        entry.path("value").asText("")
                });
            }
        } catch (IOException e) {
            // Treat unparseable history JSON as none.
        }
        return result;
    }

    private String formatTimestamp(LocalDateTime dt) {
        if (dt == null) return null;
        return dt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }
}
