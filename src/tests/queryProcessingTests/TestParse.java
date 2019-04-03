package tests.queryProcessingTests;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.List;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.queryProcessing.TableManipulate;
import DBMS.queryProcessing.parse.Parse;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;
import graphicalInterface.draw.DrawTable;

public class TestParse {

	public static void main(String[] args) throws RemoteException {
		Kernel.start();
		DBConnection connection = DistributedTransactionManagerController.getInstance().getLocalConnection("empresa", "admin", "admin");
		Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
			
			public void run(ITransaction transaction) {
				
				Parse p = new Parse();
				
				try {
					List<Plan> plans = p.parse("select id from usuarios",Kernel.getCatalog().getSchemabyName("empresa") );
					//p.parse("Create database nome",  null);
					
					for (Plan plan : plans) {
						plan.setTransaction(transaction);						
				
						TableManipulate temp = (TableManipulate) plan.execute();
						DrawTable ij = new DrawTable( transaction, temp);
						ij.reloadMatriz();
						
					}
					
					
				} catch (SQLException e) {
					
					e.printStackTrace();
				
				}
			
				transaction.commit();
			}

			@Override
			public void onFail(ITransaction transaction, Exception e) {
				transaction.abort();
				
			}
			
		});
	}

}
