package org.voltdb.utils;



import java.io.FileNotFoundException;
import java.io.FileReader;

import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;




import au.com.bytecode.opencsv_voltpatches.CSVParser;
import au.com.bytecode.opencsv_voltpatches.CSVReader;

public class CSVLoadData {
	/** Logowanie */
	protected static final VoltLogger m_log = new VoltLogger("CSV");

	/** Inforamcje o bledach */
	private static Map<Long, String[]> errorInfo = new TreeMap<Long, String[]>();

	/** Zdefiniowane puste wartosci */
	private static Map<VoltType, String> blankValues = new HashMap<VoltType, String>();
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
	
	private static final int waitSeconds = 10;
	
	/** Procedura wstawiania rekordow*/
	private static String insertProcedure = "";

	/** Konfiguracja */
	private static CSVConfig config = null;
	
	/** Lista typow VoltDB*/
	private static List <VoltType> typeList = new ArrayList<VoltType>();
	
	private static final AtomicLong inCount = new AtomicLong(0);
    private static final AtomicLong outCount = new AtomicLong(0);

	public static void main(String[] args) throws IOException,
			InterruptedException {

		int waits = 0;
		int shortWaits = 0;

		/* Konfiguracja pobierania nazwe pliku i tabeli*/
		CSVConfig cfg = new CSVConfig();
		cfg.parse(CSVLoadData.class.getName(), args);

		config = cfg;
		
		/*Ustawienie pliku i procedury insert*/
		if (config.file.equals(""))
        	System.exit(-1);
        if (!config.table.equals("")) {
            insertProcedure = config.table.toUpperCase() + ".insert";
        } else {
            insertProcedure = config.procedure;
        }
      

        String myinsert = insertProcedure;
        myinsert = myinsert.replaceAll("\\.", "_");
		
		
		CSVReader csvReader = null;
		try {
			/*Ustawienie klasy CSV reader*/
			csvReader = new CSVReader(new FileReader(config.file),
					config.separator, config.quotechar, config.escape,
					config.skip, config.strictquotes, config.nowhitespace);

		} catch (FileNotFoundException e) {
			m_log.error("CSV file '" + config.file + "' could not be found.");
			System.exit(-1);
		}
		assert (csvReader != null);
		// Split server list
		

		// Create connection
		ClientConfig c_config = new ClientConfig(config.user, config.password);
		c_config.setProcedureCallTimeout(0); // Set procedure all to infinite
												// timeout, see ENG-2670
		/*Polaczenie z serwerem*/
		
		Client csvClient = null;
		try {
			csvClient = CSVLoadData
					.getClient(c_config, config.servers, config.port);
		} catch (Exception e) {
			m_log.error("Error to connect to the servers:" + config.servers);
			close_cleanup();
			System.exit(-1);
		}
		assert (csvClient != null);

		try {
			ProcedureCallback cb = null;

			boolean lastOK = true;
			String line[] = null;

			int columnCnt = 0;
			VoltTable procInfo = null;
			boolean isProcExist = false;
			try {
				/*Zwrocenie informacji o procedurach - dokladnie ich argumentow*/
				procInfo = csvClient.callProcedure("@SystemCatalog",
						"PROCEDURECOLUMNS").getResults()[0];
				while (procInfo.advanceRow()) {
					/*Przejscie po kolejnych wierszach i wyszukanie procedu insert*/
					if (insertProcedure.matches((String) procInfo.get(
							"PROCEDURE_NAME", VoltType.STRING))) {
						columnCnt++;
						isProcExist = true;
						String typeStr = (String) procInfo.get("TYPE_NAME",
								VoltType.STRING);
						typeList.add(VoltType.typeFromString(typeStr));
					}
				}
			} catch (Exception e) {
				m_log.error(e.getMessage(), e);
				close_cleanup();
				System.exit(-1);
			}
			/*brak inserta*/
			if (isProcExist == false) {
				m_log.error("No matching insert procedure available");
				close_cleanup();
				System.exit(-1);
			}
			
			/*Wczytywanie wierszy CSV i insert*/
			while ((config.limitrows-- > 0)
					&& (line = csvReader.readNext()) != null) {
				outCount.incrementAndGet();
				boolean queued = false;
				while (queued == false) {
					StringBuilder linedata = new StringBuilder();
					/*Budowanie finalnej wersji stringa*/
					for (int i = 0; i < line.length; i++) {
						linedata.append("\"" + line[i] + "\"");
						if (i != line.length - 1)
							linedata.append(",");
					}
					String[] correctedLine = line;
					
					/*Utworzenie callbacka*/
					cb = new MyCallback(outCount.get(), config,
							linedata.toString());
					
					String lineCheckResult;
					/*Usuniecie spacji*/
					if ((lineCheckResult = checkparams_trimspace(correctedLine,
							columnCnt)) != null) {
						synchronized (errorInfo) {
							if (!errorInfo.containsKey(outCount.get())) {
								String[] info = { linedata.toString(),
										lineCheckResult };
								errorInfo.put(outCount.get(), info);
							}
							if (errorInfo.size() >= config.maxerrors) {
								m_log.error("The number of Failure row data exceeds "
										+ config.maxerrors);
								
								close_cleanup();
								System.exit(-1);
							}
						}
						break;
					}
					/*Wywolanie inserta*/
					queued = csvClient.callProcedure(cb, insertProcedure,
							(Object[]) correctedLine);
					/*Zaczekaj z insertem jesli zajety*/
					if (queued == false) {
						++waits;
						if (lastOK == false) {
							++shortWaits;
						}
						Thread.sleep(waitSeconds);
					}
					lastOK = queued;
				}
			}
			csvClient.drain();

		} catch (Exception e) {
			e.printStackTrace();
		}

		m_log.info("Inserted " + outCount.get() + " and acknowledged "
				+ inCount.get() + " rows (final)");
		if (waits > 0) {
			m_log.info("Waited " + waits + " times");
			if (shortWaits > 0) {
				m_log.info("Waited too briefly? " + shortWaits + " times");
			}
		}

		
		close_cleanup();
		csvReader.close();
		csvClient.close();
	}

	private static class CSVConfig extends CLIConfig {
		@Option(shortOpt = "f", desc = "location of CSV input file")
		String file = "";

		@Option(shortOpt = "p", desc = "procedure name to insert the data into the database")
		String procedure = "";

		@Option(desc = "maximum rows to be read from the CSV file")
		int limitrows = Integer.MAX_VALUE;

		

		// @Option(shortOpt = "m", desc = "maximum errors allowed")
		int maxerrors = 100;

		// @Option(desc = "different ways to handle blank items: {error|null|empty} (default: error)")
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
			if ((blank.equalsIgnoreCase("error")
					|| blank.equalsIgnoreCase("null") || blank
						.equalsIgnoreCase("empty")) == false)
				exitWithMessageAndUsage("blank configuration specified must be one of {error|null|empty}");
		}

		@Override
		public void printUsage() {
			System.out.println("Usage: ldr [args] tablename");
			System.out.println("       ldr [args] -p procedurename");
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
				m_log.error(response.getStatusString());
				synchronized (errorInfo) {
					if (!errorInfo.containsKey(m_lineNum)) {
						String[] info = { m_rowdata, response.getStatusString() };
						errorInfo.put(m_lineNum, info);
					}
					if (errorInfo.size() >= m_config.maxerrors) {
						m_log.error("The number of Failure row data exceeds "
								+ m_config.maxerrors);
						
						close_cleanup();
						System.exit(-1);
					}
				}
				return;
			}

			long currentCount = inCount.incrementAndGet();

			m_log.info("Inserted " + currentCount + " rows");

		}
	}

	private static Client getClient(ClientConfig config, String server,
			int port) throws Exception {
		final Client client = ClientFactory.createClient(config);

		
			client.createConnection(server, port);
		return client;
	}

	private static void close_cleanup() throws IOException,
			InterruptedException {
		inCount.set(0);
		outCount.set(0);
		errorInfo.clear();

		typeList.clear();

		
	}
	
	private static String checkparams_trimspace(String[] slot,
            int columnCnt) {
        if (slot.length == 1 && slot[0].equals("")) {
            return "Error: blank line";
        }
        if (slot.length != columnCnt) {
            return "Error: Incorrect number of columns. " + slot.length
                    + " found, " + columnCnt + " expected.";
        }
        for (int i = 0; i < slot.length; i++) {
            // trim white space in this line.
            slot[i] = slot[i].trim();
            // treat NULL, \N and "\N" as actual null value
            if ((slot[i]).equals("NULL") || slot[i].equals(VoltTable.CSV_NULL)
                    || !config.strictquotes && slot[i].equals(VoltTable.QUOTED_CSV_NULL))
                slot[i] = null;
            else if (slot[i].equals("")) {
                if (config.blank.equalsIgnoreCase("null") ) slot[i] = null;
                else if (config.blank.equalsIgnoreCase("empty"))
                    slot[i] = blankValues.get(typeList.get(i));
            }
        }

        return null;
    }
	


}
