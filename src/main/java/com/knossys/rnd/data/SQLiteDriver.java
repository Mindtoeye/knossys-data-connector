package com.knossys.rnd.data;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
 
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

/**
 * @author vvelsen
 */
public class SQLiteDriver {
	
	private static Logger M_log = Logger.getLogger(SQLiteDriver.class.getName());
	
	protected Boolean active = false;
	
	private Connection conn = null;
			
	protected String useUnicode="true";
	protected String useJDBCCompliantTimezoneShift="true";
	protected String useLegacyDatetimeCode="false";
	protected String serverTimezone="UTC";	

	protected String dbName="default";
	protected String dbTable="default";
	protected String dbPath="/tmp";
	
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
	 * 
	 */
	protected void configureEnvironment () {	
		M_log.info("configureEnvironment ()");		
	}

	/**
	 * 
	 */
	public void init() {
		M_log.info("init ()");
		
		configureEnvironment ();		
		
		if (active==true) {
			M_log.info ("Database connection already initalized, bump");
			return;
		}
			
    connect ();
		
		try {
			prepTables ();
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
	public Boolean connect () {
		M_log.info("connect ()");
		
		M_log.info("Driver loaded, connecting to database ...");
		
    File directory = new File(dbPath);
    if (!directory.exists()){
      directory.mkdirs();
    }
		
		String jdbcConfig="jdbc:sqlite:"+dbPath+"/"+dbName+".db";
				
		M_log.info("Using jdb string: " + jdbcConfig);

		try {
			conn = DriverManager.getConnection(jdbcConfig);
			
      if (conn != null) {
        DatabaseMetaData meta = conn.getMetaData();
        System.out.println("The driver name is " + meta.getDriverName());
        System.out.println("A new database has been created.");
      }			
		} catch (SQLException e) {
			M_log.info("Error starting JDBC driver: " + e.getMessage());
			//e.printStackTrace();
			conn=null;
			active=false;
			return (false);
		}
		
		M_log.info("Connected, returning true");

		active = true;
		
		return (true);
	}
	
	/**
	 * You need to close the resultSet
	 */
	public Boolean close() {
		M_log.info("close ()");
		
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

		if (statement!=null) {
			String statementString="select * from "+dbName+"."+dbTable;
			
			M_log.info ("Executing: " + statementString);
			
			resultSet = statement.executeQuery(statementString);

			closeStatement (statement,resultSet);
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

		M_log.info ("The columns in the table are: ");

		M_log.info ("Table: " + resultSet.getMetaData().getTableName(1));

		for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
			M_log.info ("Column " + i + " " + resultSet.getMetaData().getColumnName(i));
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
		
		if ((conn == null) || (active==false)) {
			M_log.info("Internal error (no DB connection available) attempting to connect ...");
			if (connect ()==false) {
				return (null);
			}
			
			M_log.info("Re-established our connection to the database, proceeding to create statement ...");
		}

		Statement statement = null;
		
		try {
			statement = conn.createStatement();
		} catch (SQLException e) {
			M_log.info("Internal error (exception), attempting to connect ...");
			if (connect ()==false) {
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
   * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements that return nothing
   */
	protected int executeUpdate(Statement statement, String statementString) {
		M_log.info("executeUpdate ()");
		
		int resultSet=0;
		Boolean retry=false;
		
		try {
			resultSet = statement.executeUpdate(statementString);
		} catch (Exception e) {
			M_log.info("Error, db response: " + e.getMessage());
      retry=true;
		}
		
		M_log.info("Query succeeded, returning result ...");
		
		if (retry==true) {
			M_log.info("Error, can't execute query, attempting a reconnect ...");
			
			if (connect ()==false) {
				M_log.info("Error, can't connect (fatal)");
				return 0;
			}
			
			// It's beginning to look like the original statement is closed when a connect fails
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
	protected ResultSet executeQuery(Statement statement, String statementString) {
		M_log.info("executeQuery ()");
		
		ResultSet resultSet=null;
		Boolean retry=false;
		
		try {
			resultSet = statement.executeQuery(statementString);
		} catch (Exception e) {
			M_log.info("Error, db response: " + e.getMessage());
      retry=true;
		}
		
		M_log.info("Query executed, evaluating if we need to do a retry ...");
		
		if (retry==true) {
			M_log.info("Error, can't execute query, attempting a reconnect ...");
			
			if (connect ()==false) {
				M_log.info("Error, can't connect (fatal)");
				return (null);
			}
			
			// It's beginning to look like the original statement is closed when a connect fails
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
	public Boolean tableExists (String aTable) {
		
		ResultSet resultSet = null;
		Statement statement = null;		
		int result = -1; // either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements that return nothing
		
		try {
		  statement = conn.createStatement();
 		  
		  String statementString="SELECT 1 FROM " + aTable + " LIMIT 1;";		  
		  		  
			result = statement.executeUpdate(statementString);
		} catch (Exception e) {
			return (false);
		} finally {
			closeStatement(statement,resultSet);
		}		
		
		if (result>0) {
			return (true);
		}
		
		return (false);
	}
}
