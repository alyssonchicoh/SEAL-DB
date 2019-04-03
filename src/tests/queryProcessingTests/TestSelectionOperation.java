package tests.queryProcessingTests;


import java.rmi.RemoteException;
import java.util.Arrays;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SelectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;
import graphicalInterface.draw.DrawTable;

public class TestSelectionOperation {

	
	public static void main(String[] args) throws RemoteException {
		Kernel.start();
		DBConnection connection = DistributedTransactionManagerController.getInstance().getLocalConnection("company", "admin", "admin");
		Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
	
			public void run(ITransaction transaction) {
				
				Plan plan = new Plan(transaction);
				ITable t = Kernel.getCatalog().getSchemabyName("company").getTableByName("empregado");
			

				System.out.println("Name: "+t.getName());
				System.out.println("Path: " +t.getPath());
				System.out.println("TableID: "+t.getTableID());
				//System.out.println("NumberOfBlocks: "+t.getNumberOfBlocks());
				//System.out.println("NumberOfTuples: "+t.getNumberOfTuples());
				System.out.println("ColumnStructure: "+Arrays.toString(t.getColumnNames()));
		
				
				
				
				TableOperation tableOP = new TableOperation();
				tableOP.setResultLeft(t);
				plan.addOperation(tableOP);
				SelectionOperation selectOP = new SelectionOperation();
				selectOP.getAttributesOperatorsValues().add(new Condition("id", ">=", "32"));
				selectOP.getAttributesOperatorsValues().add(new Condition("id", "<=", "64"));
			
				plan.addOperation(tableOP,selectOP);
				ITable temp = plan.execute();
						
				//System.out.println(temp.getNumberOfTuples());
				
				DrawTable ij = new DrawTable( transaction,temp);
				ij.reloadMatriz();
				transaction.commit();
				
			}

			@Override
			public void onFail(ITransaction transaction, Exception e) {
				transaction.abort();
				
			}
		});
		
		
		
	}

}
