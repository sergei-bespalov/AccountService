package sergei.accountservice.engine;

import java.io.PrintStream;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.util.Scanner;

import sergei.accountservice.impl.AccountServiceImpl;
import sergei.accountservice.util.SettingsReader;
import sergei.accountservice.util.Util;

public class StartService {

	public static void main(String[] args) {
		SettingsReader sr = new SettingsReader();
		Scanner scan = new Scanner(System.in);
		PrintStream printer = System.out;
		try{
			sr.readSettings("settings.xml");
			Connection conn = Util.connectToMySQL
					(sr.getAddr(), sr.getDBPort(), sr.getDatabaseName(), sr.getUser(), sr.getPassword());
            AccountServiceImpl service = new AccountServiceImpl
            		(conn, sr.getTableName(), sr.getIdColumn(), sr.getValueColumn(), 5000L);
			Registry registry = LocateRegistry.createRegistry(sr.getRmiPort());
            sr.reset();
            sr = null;
			registry.rebind("AccountService", service);
			printer.println("AccountService is running");
			while (scan.hasNext())
			{
				String inputStr = scan.nextLine();
				if (inputStr.equals("stat")) {
					printer.println(service.getStatistics());
				}
				if (inputStr.equals("stat reset")) {
					service.resetStatistics();
					printer.println("AccountService statistics were successfully reset");
				}
				if (inputStr.equals("stop")) {
					registry.unbind("AccountService");
					service = null;
					UnicastRemoteObject.unexportObject(registry, true);
					conn.close();
					System.gc();
					break;
				}
				if (inputStr.contains("set time")){
					inputStr = inputStr.replace("set time", "");
					inputStr = inputStr.trim();
					try{
						service.setTimeUnit(inputStr);
						printer.println("The unit of time is set. AccountService statistics were successfully reset");
					}catch(IllegalArgumentException e){
						System.err.println("Exeptrion: "+e);
					}
				}
			}
		}catch(Exception e){
			System.err.println("Exception: "+e);
		}
		finally{
			scan.close();
		}
	}
}
