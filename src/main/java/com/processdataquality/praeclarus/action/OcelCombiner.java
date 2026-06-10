package com.processdataquality.praeclarus.action;

import com.processdataquality.praeclarus.annotation.Plugin;
import com.processdataquality.praeclarus.exception.InvalidOptionValueException;

import tech.tablesaw.api.Table;

import java.util.List;

/**
 * Bundles an OCEL events table and an OCEL objects table into a single stream
 * for the OCEL Writer.
 *
 * The writer takes only one predecessor, whose output becomes the events
 * table. Auxiliary datasets do not propagate through intermediate nodes — a
 * node only ever exposes its own plugin's aux datasets — so an OCEL Objects
 * Reader placed anywhere other than directly before the writer can never hand
 * its table over. This node closes that gap: wire both readers (events
 * required, objects optional) into it, and it outputs the events table while
 * republishing the objects table into its own aux under "ocel:objects", which
 * the writer reads as its optional objects section.
 *
 * @author Sean Dewantoro
 * @date 26/6/2
 */
@Plugin(
        name = "OCEL Combiner",
        author = "Sean Dewantoro",
        version = "1.0",
        synopsis = "Combines an OCEL events table and (optionally) an OCEL " +
                "objects table for the OCEL Writer. Outputs the events table " +
                "and carries the objects table via auxData under " +
                "\"ocel:objects\". Connect an OCEL Events Reader (required) " +
                "and optionally an OCEL Objects Reader, then connect this node " +
                "to an OCEL Writer."
)
public class OcelCombiner extends AbstractAction {

    public OcelCombiner() {
        super();
    }

    /**
     * Identifies the events and objects tables among the inputs by sniffing
     * their key columns ("ocel:eid" for events, "ocel:oid" for objects). The
     * input order is HashSet-derived in WriterNode, so we cannot rely on
     * position. Returns the events table as this node's output and stashes the
     * objects table, when present, in aux for the writer to pick up.
     */
    @Override
    public Table run(List<Table> inputSet) throws InvalidOptionValueException {
        Table events = null;
        Table objects = null;
        for (Table t : inputSet) {
            if (events == null && t.columnNames().contains("ocel:eid")) {
                events = t;
            } else if (objects == null && t.columnNames().contains("ocel:oid")) {
                objects = t;
            }
        }

        if (events == null) {
            throw new InvalidOptionValueException("OCEL Combiner: no events " +
                    "table among inputs (expected an \"ocel:eid\" column). " +
                    "Connect an OCEL Events Reader.");
        }
        if (objects != null) {
            getAuxiliaryDatasets().put("ocel:objects", objects);
        }
        return events;
    }

    @Override
    public int getMaxInputs() {
        return 2;
    }

}
