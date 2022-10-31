package com.knossys.rnd.data;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

/**
 * @author vvelsen
 */
public class EnvironmentTools {
	
	/**
	 * @param s
	 * @return
	 */
	public static Properties parsePropertiesString(String s) {
    // load() returning void rather than the Properties object
    // so this takes 3 lines instead of "return new Properties().load(...);"
    final Properties p = new Properties();
    try {
			p.load(new StringReader(s));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    return p;
  }
	
	/**
	 * Via: 
	 *   https://stackoverflow.com/questions/585534/what-is-the-best-way-to-find-the-users-home-directory-in-java/586345
	 *   https://www.technicalkeeda.com/java-tutorials/read-environment-variables-in-java
	 *   
	 * @return
	 */
	public static String getHome () {
		return (System.getProperty("user.home"));
	}
	
	/**
	 * @param aVariable
	 * @return
	 */
	public static String getVariable (String aVariable) {
		// First see if the variable is an environment variable
		String attempt=System.getenv(aVariable);
		if (attempt!=null) {
			return(attempt);
		}
		
		// Next see if the variable is defined in the standalone.xml file
		attempt=System.getProperty(aVariable);
		if (attempt!=null) {
			return(attempt);
		}
		
		return ("");
	}
}
