package sergei.accountservice.util;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class SettingsReader {
	private Integer		 dbPort       = null;
	private Inet4Address addr         = null;
	private String       databaseName = null;
	private String		 user         = null;
	private String		 password     = null;
	private String		 table        = null;
	private String		 idColumn     = null;
	private String		 valueColumn  = null;
	private Integer		 rmiPort      = null;
	/*
	 * reads the configuration file
	 * @param path the path to the configuration file
	 */
	public  void readSettings(String path) 
			throws SAXException, IOException, ParserConfigurationException
	{
		File fXmlFile = new File(path);
		
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		
		dbPort = Integer.parseInt(doc.getElementsByTagName("dbPort").item(0).getTextContent());
		addr = (Inet4Address) InetAddress.getByName(doc.getElementsByTagName("dbIPAdrress").item(0).getTextContent());
		databaseName = doc.getElementsByTagName("databaseName").item(0).getTextContent();
		user = doc.getElementsByTagName("user").item(0).getTextContent();
		password = doc.getElementsByTagName("password").item(0).getTextContent();
		table = doc.getElementsByTagName("table_name").item(0).getTextContent();
		idColumn = doc.getElementsByTagName("column_id_name").item(0).getTextContent();
		valueColumn = doc.getElementsByTagName("column_value_name").item(0).getTextContent();
		rmiPort= Integer.parseInt(doc.getElementsByTagName("rmi_registry_port").item(0).getTextContent());
	}
	
	public Integer getDBPort()
	{
		return dbPort;
	}
	
	public Integer getRmiPort()
	{
		return rmiPort;
	}
	public Inet4Address getAddr()
	{
		return addr;
	}
	
	public String getDatabaseName()
	{
		return databaseName;
	}
	
	public String getUser()
	{
		return user;
	}
	
	public String getPassword()
	{
		return password;
	}
	
	public String getTableName()
	{
		return table;
	}
	
	public String getIdColumn()
	{
		return idColumn;
	}
	
	public String getValueColumn()
	{
		return valueColumn;
	}
	
	public void reset()
	{
		dbPort = null;
        addr = null;
        databaseName = null;
        user = null;
		password = null;
		table = null;
		idColumn = null;
		valueColumn = null;		
	}
}

