package org.voltdb;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig.AdditionalArgs;
import org.voltdb.CLIConfig.Option;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
// import org.voltdb.utils.CSVLoader.CSVConfig;
// import org.voltdb.utils.CSVLoader.CSVConfig;

import au.com.bytecode.opencsv_voltpatches.CSVParser;
import au.com.bytecode.opencsv_voltpatches.CSVReader;

public class CSVLoadData {
	/** Logowanie */
	protected static final VoltLogger m_log = new VoltLogger("CSV");
	
	/** Inforamcje o bledach*/
	private static Map<Long, String[]> errorInfo = new TreeMap<Long, String[]>();
	
	/** Zdefiniowane puste wartosci*/
	private static Map <VoltType, String> blankValues = new HashMap<VoltType, String>();
    static {
        blankValues.put(VoltType.NUMERIC, "0");
        blankValues.put(VoltType.TINYINT, "0");
        blankValues.put(VoltType.SMALLINT, "0");
        blankValues.put(VoltType.INTEGER, "0");
        blankValues.put(VoltType.BIGINT, "0");
        blankValues.put(VoltType.FLOAT, "0.0");
        blankValues.put(VoltType.TIMESTAMP, "0");
        blankValues.put(VoltType.STRING, "");
        blankValues.put(VoltType.DECIMAL, "0");
        blankValues.put(VoltType.VARBINARY, "");
    }
    
    /** Konfiguracja */
    private static CSVConfig config = null;
    
    
	 private static class CSVConfig extends CLIConfig {
	        @Option(shortOpt = "f", desc = "location of CSV input file")
	        String file = "";

	        @Option(shortOpt = "p", desc = "procedure name to insert the data into the database")
	        String procedure = "";

	        @Option(desc = "maximum rows to be read from the CSV file")
	        int limitrows = Integer.MAX_VALUE;

	        @Option(shortOpt = "r", desc = "directory path for report files")
	        String reportdir = System.getProperty("user.dir");

	        @Option(shortOpt = "m", desc = "maximum errors allowed")
	        int maxerrors = 100;

	        @Option(desc = "different ways to handle blank items: {error|null|empty} (default: error)")
	        String blank = "error";

	        @Option(desc = "delimiter to use for separating entries")
	        char separator = CSVParser.DEFAULT_SEPARATOR;

	        @Option(desc = "character to use for quoted elements (default: \")")
	        char quotechar = CSVParser.DEFAULT_QUOTE_CHARACTER;

	        @Option(desc = "character to use for escaping a separator or quote (default: \\)")
	        char escape = CSVParser.DEFAULT_ESCAPE_CHARACTER;

	        @Option(desc = "require all input values to be enclosed in quotation marks", hasArg = false)
	        boolean strictquotes = CSVParser.DEFAULT_STRICT_QUOTES;

	        @Option(desc = "number of lines to skip before inserting rows into the database")
	        int skip = CSVReader.DEFAULT_SKIP_LINES;

	        @Option(desc = "do not allow whitespace between values and separators", hasArg = false)
	        boolean nowhitespace = !CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE;

	        @Option(shortOpt = "s", desc = "list of servers to connect to (default: localhost)")
	        String servers = "localhost";

	        @Option(desc = "username when connecting to the servers")
	        String user = "";

	        @Option(desc = "password to use when connecting to servers")
	        String password = "";

	        @Option(desc = "port to use when connecting to database (default: 21212)")
	        int port = Client.VOLTDB_SERVER_PORT;

	        @AdditionalArgs(desc = "insert the data into database by TABLENAME.insert procedure by default")
	        String table = "";

	        @Override
	        public void validate() {
	            if (maxerrors < 0)
	                exitWithMessageAndUsage("abortfailurecount must be >=0");
	            if (procedure.equals("") && table.equals(""))
	                exitWithMessageAndUsage("procedure name or a table name required");
	            if (!procedure.equals("") && !table.equals(""))
	                exitWithMessageAndUsage("Only a procedure name or a table name required, pass only one please");
	            if (skip < 0)
	                exitWithMessageAndUsage("skipline must be >= 0");
	            if (limitrows > Integer.MAX_VALUE)
	                exitWithMessageAndUsage("limitrows to read must be < "
	                        + Integer.MAX_VALUE);
	            if (port < 0)
	                exitWithMessageAndUsage("port number must be >= 0");
	            if ((blank.equalsIgnoreCase("error") ||
	                    blank.equalsIgnoreCase("null") ||
	                    blank.equalsIgnoreCase("empty")) == false)
	                exitWithMessageAndUsage("blank configuration specified must be one of {error|null|empty}");
	        }

	        @Override
	        public void printUsage() {
	            System.out
	                .println("Usage: csvloader [args] tablename");
	            System.out
	                .println("       csvloader [args] -p procedurename");
	            super.printUsage();
	        }
	    }
	 
	 private static final class MyCallback implements ProcedureCallback {
	        private final long m_lineNum;
	        private final CSVConfig m_config;
	        private final String m_rowdata;

	        MyCallback(long lineNumber, CSVConfig cfg, String rowdata) {
	            m_lineNum = lineNumber;
	            m_config = cfg;
	            m_rowdata = rowdata;
	        }

	        @Override
	        public void clientCallback(ClientResponse response) throws Exception {
	            if (response.getStatus() != ClientResponse.SUCCESS) {
	                m_log.error( response.getStatusString() );
	                synchronized (errorInfo) {
	                    if (!errorInfo.containsKey(m_lineNum)) {
	                        String[] info = { m_rowdata, response.getStatusString() };
	                        errorInfo.put(m_lineNum, info);
	                    }
	                    if (errorInfo.size() >= m_config.maxerrors) {
	                        m_log.error("The number of Failure row data exceeds " + m_config.maxerrors);
	                        produceFiles();
	                        close_cleanup();
	                        System.exit(-1);
	                    }
	                }
	                return;
	            }

	            long currentCount = inCount.incrementAndGet();

	            if (currentCount % reportEveryNRows == 0) {
	                m_log.info( "Inserted " + currentCount + " rows" );
	            }
	        }
	    }

}
