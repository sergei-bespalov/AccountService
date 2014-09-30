package sergei.accountservice.util;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Util {
	/*
	 * connect to MySQL database
	 * @param addr database Internet address
	 * @param port database port
	 * @param dbName database name
	 * @param user database user
	 * @param password database user password
	 */
	public static Connection connectToMySQL
	(InetAddress addr, Integer port, String dbName, String user, String password)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		String connString = "jdbc:mysql://"+addr.getHostAddress()+":"+port+"/"
				+dbName+"?user="+user+"&password="+password;
		return DriverManager.getConnection(connString);
	}
}
