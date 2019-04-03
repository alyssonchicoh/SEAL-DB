package tests.queryProcessingTests;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SortOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;
import graphicalInterface.draw.DrawTable;

public class TestSortOperation {
	
	public static void main(String[] args) throws RemoteException, InterruptedException {
		Kernel.start();
		DBConnection connection = DistributedTransactionManagerController.getInstance().getLocalConnection("empresa", "admin", "admin");
		
		
		Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {

			public void run(ITransaction transaction) {
				
				ITable t = Kernel.getCatalog().getSchemabyName("empresa").getTableByName("usuarios");

				System.out.println("Name: "+t.getName());
				System.out.println("Path: " +t.getPath());
				System.out.println("TableID: "+t.getTableID());
				//System.out.println("NumberOfBlocks: "+t.getNumberOfBlocks());
				//System.out.println("NumberOfTuples: "+t.getNumberOfTuples());
				//System.out.println("ColumnStructure: "+t.getColumnStructure());
				System.out.println("ColumnNames: "+Arrays.toString(t.getColumnNames()));
				
				
				Plan plan = new Plan(transaction);
				
				TableOperation tableOP = new TableOperation();
				tableOP.setResultLeft(t);
				
				plan.addOperation(tableOP);
				
				SortOperation sort = new SortOperation();
				List<String> columnsSorted = new ArrayList<>();
				columnsSorted.add("nome");
				sort.setColumnSorted(new ArrayList<String>());
			
				plan.addOperation(tableOP,sort);
				
				ITable temp =  plan.execute();
				//System.out.println(temp.getNumberOfTuples());
				DrawTable ij = new DrawTable( transaction, temp);
				ij.reloadMatriz();
				
			}

			@Override
			public void onFail(ITransaction transaction, Exception e) {
				transaction.abort();
				
			}

		});
		
	
}
	
}
