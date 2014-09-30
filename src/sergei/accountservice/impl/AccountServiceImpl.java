package sergei.accountservice.impl;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentMap;
import java.util.Date;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import sergei.accountservice.intf.AccountService;
import sergei.accountservice.util.UnitOfTime;

public class AccountServiceImpl extends UnicastRemoteObject implements AccountService{
    /**
	 * 
	 */
	private static final long serialVersionUID = -283431263506521783L;
    private Connection                   conn              = null;
    private Statement                    st                = null;
    private String                       TABLE             = null;
    private String                       ID                = null;
    private String						 DATA              = null;
    private ConcurrentMap<Integer, Long> cache             = null;
    private Long                         countGetAmount    = null;
    private Long                         countAddAmount    = null;
    private Integer                      getAmountPerTime  = null;
    private Integer                      tGetAmountPerTime = null;
    private Integer                      addAmountPerTime  = null;
    private Integer                      tAddAmountPerTime = null;
    private Long                         startTime         = null;
    private Long                         timeUnit          = null;
    /* 
     * Constructor
	 * @param dbCommection  database connection
	 * @param tableName service works with this table
	 * @param idColumnName balance identifier stored in this column
	 * @param dataColumnName current balance stored in this column
	 * @param cacheSize cache size
	 * */
    public AccountServiceImpl(Connection dbCommection, String tableName,
    							 String idColumnName, String dataColumnName,
    							 Long cacheSize)
			throws RemoteException, SQLException {
		super();
		conn  = dbCommection;
		TABLE = tableName;
		ID    = idColumnName;
		DATA  = dataColumnName;
		st    = conn.createStatement();
		setTimeUnit(UnitOfTime.SECOND);
		cache = new ConcurrentLinkedHashMap.Builder<Integer, Long>()
				.maximumWeightedCapacity(cacheSize)
				.build();	
		resetStatistics();		
	}

	@Override
	public synchronized Long getAmount(Integer id) throws SQLException, RemoteException {
		if (!cache.containsKey(id)){
			ResultSet rs = st.executeQuery("select "+DATA
					                     +" from " +TABLE
					                     +" where "+ID +"=" +id);
			if(rs.next()){
				cache.put(id, rs.getLong(DATA));
				}
			else{
				cache.put(id,0L);
			}
		}
		writeStatistics("getAmount");		
		return cache.get(id);
	}
	
	@Override
	public synchronized void addAmount(Integer id, Long value) throws SQLException, RemoteException {
		if(cache.containsKey(id)) cache.remove(id);
		ResultSet rs = st.executeQuery("select " +ID 
                                      +" from " +TABLE
                                      +" where " +ID +"=" +id);
		if (rs.next()){
			st.executeUpdate("update " +TABLE
                            +" set " +DATA +"=" +DATA +"+" +value
                            +" where " +ID +"=" +id);
		}else{
			st.executeUpdate("insert into " +TABLE +" (" +ID +"," +DATA +")"
                            +" values (" +id +"," +value +")");
		}
		st.executeUpdate("commit");
		writeStatistics("addAmount");
	}
	/*
	 * sets the unit of time, which is used in the statistics
	 * @param t unit of time
	 */
	public void setTimeUnit(Long t)
	{
		timeUnit = t;
		resetStatistics();
	}
	/*
	 * sets the unit of time, which is used in the statistics
	 * @param t unit of time
	 */
	public void setTimeUnit(String t) throws IllegalArgumentException
	{		
		if (t.equals("millisecond"))  setTimeUnit(UnitOfTime.MSECOND);
		else if (t.equals("second"))  setTimeUnit(UnitOfTime.SECOND);
		else if (t.equals("minute"))  setTimeUnit(UnitOfTime.MINUTE);
		else if (t.equals("hour"))    setTimeUnit(UnitOfTime.HOUR);
		else if (t.equals("day"))     setTimeUnit(UnitOfTime.DAY);
		else if (t.equals("week"))    setTimeUnit(UnitOfTime.WEEK);
		else throw new IllegalArgumentException("Unit of time \""+t+"\" not supported");
	}		
	/*
	 * create statistics for addAmount, getAmount
	 * @param method method name
	 */
	private void writeStatistics(String method)
	{
		if(method.equals("addAmount"))countAddAmount++;
		if(method.equals("getAmount"))countGetAmount++;
		Long time = new Date().getTime()-startTime;
		if(time <= timeUnit){
			if(method.equals("addAmount"))tAddAmountPerTime++;
			if(method.equals("getAmount"))tGetAmountPerTime++;
		}else{
			startTime = new Date().getTime();
			tAddAmountPerTime=0;
			tGetAmountPerTime=0;}
		if(tAddAmountPerTime > addAmountPerTime){
			addAmountPerTime = tAddAmountPerTime;
		}
		if(tGetAmountPerTime > getAmountPerTime){
			getAmountPerTime = tGetAmountPerTime;
		}
	}
	/*
	 * Reset addAmount, getAmount statistics
	 */
	public void resetStatistics()
	{
		startTime = new Date().getTime();
		countGetAmount = 0L;
		countAddAmount = 0L;
		getAmountPerTime = 0;
		addAmountPerTime = 0;
		tGetAmountPerTime = 0;
		tAddAmountPerTime = 0;
	}
	/*
	 * requests getAmount
	 */
	public Long CountGetAmount()
	{
		return countGetAmount;
	}
	/*
	 * requests addAmount
	 */	
	public Long CountAddAmount()
	{
		return countAddAmount;
	}
	/*
	 * maximum requests getAmount per unit of time
	 */	
	public Integer getAmountPerTime()
	{
		return getAmountPerTime;
	}
	/*
	 * maximum requests addAmount per unit of time
	 */		
	public Integer addAmountPerTime()
	{
		return addAmountPerTime;
	}
	/*
	 * Retrieves addAmount,getAmount statistics
	 */
	public String getStatistics()
	{
		StringBuffer Statistics =  new StringBuffer();
		String time;
		if      (timeUnit == UnitOfTime.MSECOND)time ="millisecond";
		else if (timeUnit == UnitOfTime.SECOND) time ="second";
		else if (timeUnit == UnitOfTime.MINUTE) time ="minute";
		else if (timeUnit == UnitOfTime.HOUR)	time ="hour";
		else if (timeUnit == UnitOfTime.DAY)    time ="day";
		else if (timeUnit == UnitOfTime.WEEK)   time ="week";
		else time = timeUnit/UnitOfTime.SECOND + " second";
		Statistics.append("Count of getAmount: " +CountGetAmount()+"\n");
		Statistics.append("getAmount per " +time +": " +getAmountPerTime()+"\n");
		Statistics.append("Count of addAmount: " +": " +CountAddAmount()+"\n");
		Statistics.append("addAmount per " +time +": " +addAmountPerTime());	
		return Statistics.toString();
	}	
}
