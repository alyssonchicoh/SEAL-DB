package tests.queryProcessingTests;


import java.rmi.RemoteException;
import java.sql.SQLException;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.parse.Parse;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.HashJoin;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.JoinOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.Transaction;
import DBMS.transactionManager.TransactionRunnable;
import graphicalInterface.draw.DrawTable;

public class TestJoinOperation {

	
	public static void main(String[] args) throws RemoteException {
		
		Kernel.start();
		
		DBConnection connection = DistributedTransactionManagerController.getInstance().getLocalConnection("company", "admin", "admin");
		
		Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
			
			public void run(ITransaction transaction) {
				
				ITable t1 = Kernel.getCatalog().getSchemabyName("company").getTableByName("empregado");
			//	t1.update(transaction);

				ITable t2 = Kernel.getCatalog().getSchemabyName("company").getTableByName("empregado");
				
			//	t2.update(transaction);
				
				
				Plan plan = new Plan(transaction);
				
				TableOperation tableOP1 = new TableOperation();
				tableOP1.setResultLeft(t1);
				
				TableOperation tableOP2 = new TableOperation();
				tableOP2.setResultLeft(t2);
				
				JoinOperation joinOP = new JoinOperation();
				joinOP.setJoinAlgorithm(new HashJoin());
				//joinOP.setJoinAlgorithm(new MergeJoin());
				//joinOP.setJoinAlgorithm(new BlockNestedLoopJoin());
				
				joinOP.getAttributesOperatorsValues().add(new Condition("id", "==", "id"));
				
				plan.addOperation(joinOP);
				
				
				plan.addOperationDown(true, joinOP, tableOP1);
				plan.addOperationDown(false, joinOP, tableOP2);
				
				ITable temp = plan.execute();
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
	
	
	
	public void test(){
		
		DBConnection connection = DistributedTransactionManagerController.getInstance().getLocalConnection("company", "admin", "admin");
		
		ITransaction transaction = ITransaction.getNewInstance(connection);	
		transaction.execRunnable(new TransactionRunnable() {
			
			@Override
			public void run(ITransaction transaction) {
				
				String SQL = "INSERT INTO employee (id,name,salary,department_id) VALUES (1,'Gustavo',2000,1)";
				ISchema schema = Kernel.getCatalog().getSchemabyName("company");				
				
				try {
					Plan plan = Parse.getNewInstance().parseSQL(SQL, schema);
					plan.execute();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				transaction.commit();
			}
			
			@Override
			public void onFail(ITransaction transaction, Exception e) {
				System.out.println(e.getMessage());
				transaction.abort();
			}
		});
		
		
		
		
		
		
		
	}

}
