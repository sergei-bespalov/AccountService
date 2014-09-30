package sergei.accountservice.intf;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

public interface AccountService extends Remote {
	 /**
	    * Retrieves current balance or zero if addAmount() method was called before for specified id
	    *   
	    * @param id balance identifier
	    * @throws SQLException,RemoteException
	    */
	    Long getAmount(Integer id) throws SQLException,RemoteException;

	    /**
	     * Increases balance or set if addAmount() method was called first time
	     *
	     * @param id balance identifier
	     * @param value positive or negative value, which must be added to current balance
	     * @throws SQLException, RemoteException 
	     */
	    void addAmount(Integer id, Long value) throws SQLException,RemoteException;
}
