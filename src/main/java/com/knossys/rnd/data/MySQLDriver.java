/**
 * https://stackoverflow.com/questions/24676111/streaming-mysql-resultset-with-fixed-number-of-results-at-a-time
 */
package com.knossys.rnd.data;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.Scanner;
import java.io.File;
import org.apache.commons.lang3.StringUtils;

/**
 * @author vvelsen
 */
public class MySQLDriver extends MySQLSSHDriver {

	private static Logger M_log = Logger.getLogger(MySQLDriver.class.getName());

	protected Boolean active = false;

	protected String useUnicode = "true";
	protected String useJDBCCompliantTimezoneShift = "true";
	protected String useLegacyDatetimeCode = "false";
	protected String serverTimezone = "UTC";
	
	protected Integer connectionTimeout = 2000;
	protected Integer socketTimeout = 2000;

	/**
	 * 
	 */
	public MySQLDriver () {
		//M_log.info("MySQLDriver ()");
	}
	
	/**
	 * Probably a bad choice to add this since it might be useless, but maybe we
	 * can do some clever things with queries before we execute them. At least 
	 * there will now be a single place where we can print it out without going
	 * through logging.
	 * 
	 * @param aQuery
	 * @return
	 */
	protected String cacheQuery (String aQuery) {
		System.out.println ("[[[ "+aQuery+" ]]]");
		return (aQuery);
	}
	
	/**
	 * @param rs
	 * @throws SQLException
	 */
  public static ArrayList<String> getColumnNames(ResultSet rs) throws SQLException {
  	ArrayList<String> colNames=new ArrayList<String> ();
  	
    if (rs == null) {
      return (colNames);
    }
    
    ResultSetMetaData rsMetaData = rs.getMetaData();
    int numberOfColumns = rsMetaData.getColumnCount();

    // get the column names; column indexes start from 1
    for (int i = 1; i < numberOfColumns + 1; i++) {
      String columnName = rsMetaData.getColumnName(i);
      
      // Get the name of the column's table name 
      //String tableName = rsMetaData.getTableName(i);
      
      //System.out.println("column name=" + columnName + " table=" + tableName + "");
      colNames.add(columnName);
    }
    
    return (colNames);
  }
	
	
	/**
	 *
	 * @return
	 */
	public Boolean getActive() {
		if (conn == null) {
			active = false;
		}
		return active;
	}

	/**
	 *
	 * @param active
	 */
	public void setActive(Boolean active) {
		this.active = active;
	}

	/**
	 * https://dev.mysql.com/doc/refman/5.7/en/keyring.html
	 * 
	 * alter table table_name encryption='Y'
	 * 
	 * After starting MySQL, we can unmount the “/mount/key” partition. In this case, the key will not be available and a potential hacker will not be able to restart MySQL with “–skip-grant-tables” option (without passwords). However, it also prevents normal restarts, especially SSTs (cluster full sync).
	 * 
	 * Additional notes:
	 * 
	 *  1. Encryption will affect the compression ratio, especially for the physical backups (logical backups, i.e. mysqldump does not matter as the data retrieved is not encrypted). If your original compressed backup size was only 10% of the database size, it will not be the case for the encrypted tables.
	 *  2. Data is not encrypted in flight and will not be encrypted on the replication slaves unless you enable the same options on the slaves. The encryption is also local to the server, so when encryption was just enabled on a server some tables may not be encrypted yet (but will be eventually)
	 *  3. To check which tables are encrypted, use the Information Schema INNODB_TABLESPACES_ENCRYPTION table, which contains encryption information. To find all tables that are encrypted, use this query:
	 *     
	 *  select * from information_schema.INNODB_TABLESPACES_ENCRYPTION where ENCRYPTION_SCHEME=1
	 * 
	 * @param aTableName
	 */
	public int encryptTable (String aTableName, Boolean encrypt) {
		M_log.info("encryptTable ("+encrypt+")");

    int result=-1;
		
		Statement statement = createStatement();

		if (statement == null) {
			return (result);
		}
		
		String doEncrypt="N";
		
		if (encrypt==true) {
			doEncrypt="Y";
		}

		String statementString="alter table " + dbName + "." + aTableName + " encryption='"+doEncrypt+"'";

		M_log.info("Executing: " + statementString);
		
		try {
			result = statement.executeUpdate(statementString);
		} catch (Exception e) {
			M_log.info("result: " + e.getMessage());
		} finally {
			closeStatement(statement,null);
		}
		
		return (result);
	}
	
	/**
	 *
	 */
	protected void configureEnvironment() {
		M_log.info("configureEnvironment ()");

		if (!StringUtils.isBlank(System.getProperty("dbHost"))) {
			// M_log.info ("Found dbHost to be: " + System.getProperty("dbHost"));
			dbHost = System.getProperty("dbHost");
			M_log.info("dbHost: " + dbHost);
		}

		if (!StringUtils.isBlank(System.getProperty("dbPort"))) {
			String tempPort = System.getProperty("dbPort");

			dbPort = Integer.parseInt(tempPort);
			M_log.info("dbPort: " + tempPort);
		}

		if (!StringUtils.isBlank(System.getProperty("dbName"))) {
			dbName = System.getProperty("dbName");
			M_log.info("dbName: " + dbName);
		}

		if (!StringUtils.isBlank(System.getProperty("dbTable"))) {
			dbTable = System.getProperty("dbTable");
			M_log.info("dbTable: " + dbTable);
		}

		if (!StringUtils.isBlank(System.getProperty("dbUsernameFile"))) {
			// First try to get username from secrets file.
			// The file is specified in an environment variable.
			try {
				dbUsername = new Scanner(new File(System.getProperty("dbUsernameFile"))).nextLine();
				M_log.info("dbUsername: " + dbUsername + " from " + System.getProperty("dbUsernameFile"));
			} catch (Exception e) {
			}
			;
		} else if (!StringUtils.isBlank(System.getProperty("dbUsername"))) {
			dbUsername = System.getProperty("dbUsername");
			M_log.info("dbUsername: " + dbUsername);
		}

		if (!StringUtils.isBlank(System.getProperty("dbPasswordFile"))) {
			// First try to get password from secrets file.
			// The file is specified in an environment variable.
			try {
				dbPassword = new Scanner(new File(System.getProperty("dbPasswordFile"))).nextLine();
				M_log.info("dbPassword from secrets");
			} catch (Exception e) {
			}
			;
		} else if (!StringUtils.isBlank(System.getProperty("dbPassword"))) {
			// Fall back to getting the value from the property.
			dbPassword = System.getProperty("dbPassword");
			M_log.info("dbPassword set.");
		}

		if (System.getProperty("dbTunnel") != null) {
			if (System.getProperty("dbTunnel").equalsIgnoreCase("true") == true) {
				setUseTunnel(true);
			} else {
				setUseTunnel(false);
			}
		}
	}

	/**
	 *
	 */
	public void init() {
		M_log.info("init ()");

		configureEnvironment();

		super.init();

		if (active == true) {
			M_log.info("Database connection already initalized, bump");
			return;
		}

		if ((dbHost.isEmpty() == true) || (dbName.isEmpty() == true) || (dbTable.isEmpty() == true) || (dbUsername.isEmpty() == true) || (dbPassword.isEmpty() == true)) {
			M_log.info("The service has not been configured yet, please modify or use standalone.xml as provided in the code repository");
			
			if (dbHost.isEmpty() == true) {
				M_log.info("dbHost is empty");
			}
			
			if (dbName.isEmpty() == true) {
				M_log.info("dbName is empty");
			}
			
			if (dbTable.isEmpty() == true) {
				M_log.info("dbTable is empty");
			}
			
			if (dbUsername.isEmpty() == true) {
				M_log.info("dbUsername is empty");
			}
			
			if (dbPassword.isEmpty() == true) {
				M_log.info("dbPassword is empty");
			}
				
			return;
		}

		connect();

		try {
			prepTables();
		} catch (Exception e) {
			M_log.info("Error creating or checking default tables: " + e.getMessage());
			active = false;
			return;
		}

		M_log.info("Connected to database and initialized");
	}

	/**
	 *
	 */
	public Boolean connect() {
		M_log.info("connect (" + dbHost + ":"+dbPort+")");

		if (openTunnel() == false) {
			return (false);
		}

		InetAddress address;
		try {
			address = InetAddress.getByName(dbHost);
		} catch (UnknownHostException e1) {
			M_log.info("Can't resolve MySQL hostname: " + e1.getMessage());
			return (false);
		}

		M_log.info("Resolved MySQL hostname to: " + address.getHostAddress());

		String safeAddress = address.getHostAddress();

		M_log.info("Determined safe ip address to be: " + safeAddress);

		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			M_log.info("Error loading JDBC driver: " + e.getMessage());
			return (false);
		}

		M_log.info("Driver loaded, connecting to database ...");
		
		/*
		if (this.getUseTunnel()==true) {
			useSSL="true";
		} else {
			useSSL="false";
		}
		*/
		
		// Note: https://www.codejava.net/java-se/jdbc/fix-error-public-key-retrieval-is-not-allowed

		String jdbcConfig = "jdbc:mysql://" + safeAddress + ":" + dbPort + "/" + dbName + "?useUnicode=" + useUnicode
		    + "&useJDBCCompliantTimezoneShift=" + useJDBCCompliantTimezoneShift + "&useLegacyDatetimeCode="
		    + useLegacyDatetimeCode + "&serverTimezone=" + serverTimezone + "&user=" + dbUsername + "&password="
		    + dbPassword + "&useSSL=" + useSSL + "&connectTimeout="+connectionTimeout.toString() + "&socketTimeout=" + socketTimeout.toString() + "&allowPublicKeyRetrieval=true";

		/*
		 * Be very careful with the debug line below since it will show usernames and
		 * passwords
		 */

		M_log.info("Using jdb string: " + jdbcConfig);

		try {
			conn = DriverManager.getConnection(jdbcConfig, dbUsername, dbPassword);
		} catch (SQLException e) {
			M_log.info("Error starting JDBC driver: " + e.getMessage());
			// e.printStackTrace();
			conn = null;
			active = false;
			return (false);
		}

		M_log.info("Connected, returning true");

		active = true;

		/*
		 * if (getUseTunnel()==true) { showTables (); }
		 */

		return (true);
	}

	/**
	 * You need to close the resultSet
	 */
	public Boolean close() {
		M_log.info("close ()");

		if (super.close() == false) {
			return (false);
		}

		if (conn == null) {
			M_log.info("Internal error: no DB connection available");
			active = false;
			return (false);
		}

		try {
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			return (false);
		}

		active = false;

		return (true);
	}

	/**
	 *
	 * @throws Exception
	 */
	public void prepTables() throws Exception {
		M_log.info("prepTables ()");

		M_log.info("IMPLEMENT IN CHILD CLASS!");
	}

	/**
	 * This reads everything! Although this is useful be careful with it
	 * 
	 * @throws Exception
	 */
	public ResultSet readDataBase() throws Exception {
		M_log.info("readDataBase()");

		ResultSet resultSet = null;
		Statement statement = null;

		if (conn == null) {
			M_log.info("Internal error: no DB connection available");
			return (null);
		}

		statement = createStatement();

		if (statement != null) {
			String statementString = "select * from " + dbName + "." + dbTable;

			M_log.info("Executing: " + statementString);

			resultSet = statement.executeQuery(statementString);

			closeStatement(statement, resultSet);
		} else {
			return (null);
		}

		return (resultSet);
	}

	/**
	 *
	 * @param resultSet
	 * @throws SQLException
	 */
	public void writeMetaData(ResultSet resultSet) throws SQLException {
		if (conn == null) {
			M_log.info("Internal error: no DB connection available");
			return;
		}

		M_log.info("The columns in the table are: ");

		M_log.info("Table: " + resultSet.getMetaData().getTableName(1));

		for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
			M_log.info("Column " + i + " " + resultSet.getMetaData().getColumnName(i));
		}
	}

	/**
	 *
	 * @param resultSet
	 * @throws SQLException
	 */
	public void writeResultSet(ResultSet resultSet) throws SQLException {
		// M_log.info ("writeResultSet ()");

		if (conn == null) {
			M_log.info("Internal error: no DB connection available");
			return;
		}

		while (resultSet.next()) {
			String id = resultSet.getString("id");
			String username = resultSet.getString("username");
			String json = resultSet.getString("json");

			M_log.info("ID: " + id);
			M_log.info("Username: " + username);
			M_log.info("JSON: " + json);
		}
	}

	/**
	 * @return
	 */
	public Statement createStatement() {
		M_log.info("createStatement ()");

		if ((conn == null) || (active == false)) {
			M_log.info("Internal error (no DB connection available) attempting to connect ...");
			if (connect() == false) {
				return (null);
			}

			M_log.info("Re-established our connection to the database, proceeding to create statement ...");
		}

		Statement statement = null;

		try {
			statement = conn.createStatement();
		} catch (SQLException e) {
			M_log.info("Internal error (exception), attempting to connect ...");
			if (connect() == false) {
				M_log.info("Error connecting after possible disconnect, giving up");
				return (null);
			}

			try {
				statement = conn.createStatement();
			} catch (SQLException e1) {
				M_log.info("Internal error (final exception): " + e1.getMessage());
				return (null);
			}
		}

		M_log.info("Statement created, returning ...");
		
		return statement;
	}

	/**
	 * You need to close the resultSet
	 */
	public void closeStatement(Statement statement, ResultSet resultSet) {
		M_log.info("closeStatement ()");

		if (conn == null) {
			M_log.info("Internal error: no DB connection available");
			return;
		}

		try {
			if (resultSet != null) {
				resultSet.close();
			}

			if (statement != null) {
				statement.close();
			}
		} catch (Exception e) {
			M_log.info("Internal error closing statement: " + e.getMessage());
		}
	}

	/**
	 * @param statement
	 * @param statementString
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 *         statements or (2) 0 for SQL statements that return nothing
	 */
	public int executeUpdate(Statement statement, String statementString) {
		M_log.info("executeUpdate ()");

		int resultSet = 0;
		Boolean retry = false;

		try {
			resultSet = statement.executeUpdate(statementString);
		} catch (Exception e) {
			M_log.info("Error, db response: " + e.getMessage());
			retry = true;
		}

		M_log.info("Query succeeded, returning result ...");

		if (retry == true) {
			M_log.info("Error, can't execute query, attempting a reconnect ...");

			if (connect() == false) {
				M_log.info("Error, can't connect (fatal)");
				return 0;
			}

			// It's beginning to look like the original statement is closed when a connect
			// fails
			statement = createStatement();

			try {
				resultSet = statement.executeUpdate(statementString);
			} catch (SQLException e) {
				M_log.info("Error, can't connect (fatal): " + e.getMessage());
				return 0;
			}
		}

		try {
			statement.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return resultSet;
	}

	/**
	 * @param statement
	 * @param statementString
	 * @return
	 */
	public ResultSet executeQuery(Statement statement, String statementString) {
		M_log.info("executeQuery ()");

		M_log.info(statementString);

		ResultSet resultSet = null;
		Boolean retry = false;

		try {
			resultSet = statement.executeQuery(statementString);
		} catch (Exception e) {
			M_log.info("Error, db response: " + e.getMessage());
			retry = true;
		}

		M_log.info("Query executed, evaluating if we need to do a retry ...");

		if (retry == true) {
			M_log.info("Error, can't execute query, attempting a reconnect ...");

			if (connect() == false) {
				M_log.info("Error, can't connect (fatal)");
				return (null);
			}

			// It's beginning to look like the original statement is closed when a connect
			// fails
			statement = createStatement();

			try {
				resultSet = statement.executeQuery(statementString);
			} catch (SQLException e) {
				M_log.info("Error, can't connect (fatal): " + e.getMessage());
				return (null);
			}
		}

		return resultSet;
	}

	/**
	 *
	 */
	public Boolean tableExists(String aTable) {

		ResultSet resultSet = null;
		Statement statement = null;
		int result = -1; // either (1) the row count for SQL Data Manipulation Language (DML) statements
		                 // or (2) 0 for SQL statements that return nothing

		try {
			statement = conn.createStatement();

			String statementString = "SELECT 1 FROM " + aTable + " LIMIT 1;";

			result = statement.executeUpdate(statementString);
		} catch (Exception e) {
			return (false);
		} finally {
			closeStatement(statement, resultSet);
		}

		if (result > 0) {
			return (true);
		}

		return (false);
	}

	/**
	 *
	 */
	public void showTables() {
		M_log.info("showTables ()");

		if (conn == null) {
			M_log.info("Error: no db connection available");
			return;
		}

		DatabaseMetaData md = null;

		try {
			md = conn.getMetaData();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}

		ResultSet rs = null;

		try {
			rs = md.getTables(null, null, "%", null);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}

		int index = 0;

		try {
			while (rs.next()) {
				M_log.info("Table [" + dbName + "][" + index + "]: " + rs.getString(3));
				index++;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
	}
}
