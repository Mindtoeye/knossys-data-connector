package com.knossys.rnd.data;

import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class KDatabaseConnector extends MySQLDriver {

  private static Logger M_log = Logger.getLogger(KDatabaseConnector.class);
  
  /**
  * https://blog.jongallant.com/2020/04/java-dotenv-azure-sdk/
  */
  protected void configureEnvironment() {
    M_log.info("configureEnvironment ()");

    if (!StringUtils.isBlank(System.getenv("dbHost"))) {
      // M_log.info ("Found dbHost to be: " + System.getProperty("dbHost"));
      dbHost = System.getenv("dbHost");
      M_log.info("dbHost: " + dbHost);
    }

    if (!StringUtils.isBlank(System.getenv("dbPort"))) {
      String tempPort = System.getenv("dbPort");

      dbPort = Integer.parseInt(tempPort);
      M_log.info("dbPort: " + tempPort);
    }

    if (!StringUtils.isBlank(System.getenv("dbName"))) {
      dbName = System.getenv("dbName");
      M_log.info("dbName: " + dbName);
    }

    if (!StringUtils.isBlank(System.getenv("dbTable"))) {
      dbTable = System.getenv("dbTable");
      M_log.info("dbTable: " + dbTable);
    }
    
    if (!StringUtils.isBlank(System.getenv("dbUsername"))) {
      dbUsername = System.getenv("dbUsername");
      M_log.info("dbUsername: " + dbUsername);
    }    

    if (!StringUtils.isBlank(System.getenv("dbPassword"))) {
      // Fall back to getting the value from the property.
      dbPassword = System.getenv("dbPassword");
      M_log.info("dbPassword set.");
    }   
   
    if (System.getenv("dbTunnel") != null) {
      if (System.getenv("dbTunnel").equalsIgnoreCase("true") == true) {
        setUseTunnel(true);
      } else {
        setUseTunnel(false);
      }
    }
  }  
  
  /**
  * @throws Exception
  */
  public void prepTables() throws Exception {
    M_log.info("prepTables ()");

    Statement statement=createStatement();

    if (statement==null) {
      M_log.info("Fatal error, can't create MySQL statement");
      return;
    }
    
    String statementString="";
    int result = -1;
    
    statementString="CREATE TABLE IF NOT EXISTS knossys.files (\n"
      + " id VARCHAR(40) NOT NULL,\n"
      + " date VARCHAR(100) NOT NULL,\n"        
      + " filename VARCHAR(200) NOT NULL,\n"
      + " data LONGTEXT NOT NULL,\n"
      + " PRIMARY KEY (id));";
      
    M_log.info ("Executing: " + statementString);
          
    result = statement.executeUpdate(statementString);
    
    M_log.info ("Result: " + result);    
  }
}
