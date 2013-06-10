package org.voltdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltTableUtil;

import com.google.common.util.concurrent.Callables;

public class CSVFileWriter {
	
	protected final SystemProcedureExecutionContext m_systemProcedureContext;
	 private final char m_delimiter;
	    private final char m_fullDelimiters[];
	    private final byte m_schemaBytes[];
	    private final ArrayList<VoltType> m_columnTypes;
	    private int m_lastNumCharacters = 64 * 1024;
	    private static final VoltLogger logg = new VoltLogger("CSV");
	//new CSVFileWriter(CatalogUtil.getVoltTable(table), ',', null)
	public CSVFileWriter(VoltTable vt,
            char delimiter,
            char fullDelimiters[],SystemProcedureExecutionContext ctx)
	{
		m_systemProcedureContext=ctx; // konieczna inicjalizacja do pobrania bazy
		m_columnTypes = new ArrayList<VoltType>(vt.getColumnCount());
        for (int ii = 0; ii < vt.getColumnCount(); ii++) {
            m_columnTypes.add(vt.getColumnType(ii));
        }
        m_fullDelimiters = fullDelimiters;
        m_delimiter = delimiter;
        m_schemaBytes = vt.getSchemaBytes();
	}
	
	public void writeCSVFile()
	{
		
		final List<Table> tables = getTablesToSave(m_systemProcedureContext.getDatabase());
		
		// do poprawy bufor musi byc z bazy a nie snapshota
		snapshotBuffer.b.limit(serialized + headerSize);
        snapshotBuffer.b.position(0);
        Callable<BBContainer> valueForTarget = Callables.returning(snapshotBuffer);
		final BBContainer cont = input.call();
                if (cont == null) {
                    return;
                }
		ByteBuffer buf = ByteBuffer.allocate(m_schemaBytes.length + cont.b.remaining() - 4);
        buf.put(m_schemaBytes);
        cont.b.position(4);
        buf.put(cont.b);

        VoltTable vt = PrivateVoltTableFactory.createVoltTableFromBuffer(buf, true);
        logg.error("CSV file save");
        Pair<Integer, byte[]> p =
                        VoltTableUtil.toCSV(
                                vt,
                                m_columnTypes,
                                m_delimiter,
                                m_fullDelimiters,
                                m_lastNumCharacters);
        m_lastNumCharacters = p.getFirst();
        //return DBBPool.wrapBB(ByteBuffer.wrap(p.getSecond()));
	}
	
	public static final List<Table> getTablesToSave(Database database)
    {
        CatalogMap<Table> all_tables = database.getTables();
        ArrayList<Table> my_tables = new ArrayList<Table>();
        for (Table table : all_tables)
        {
            // Make a list of all non-materialized, non-export only tables
            if ((table.getMaterializer() != null) ||
                    (CatalogUtil.isTableExportOnly(database, table)))
            {
                continue;
            }
            my_tables.add(table);
        }
        return my_tables;
    }
}
