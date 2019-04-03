package tests.queryProcessingTests;






import java.util.Arrays;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;



public class TestTableOperation {

		
	public static void main(String[] args) throws Exception {
		Kernel.start();
		DBConnection connection = DistributedTransactionManagerController.getInstance().getLocalConnection("company", "admin", "admin");
		Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
			
			public void run(ITransaction transaction) {
				ITable t = Kernel.getCatalog().getSchemabyName("company").getTableByName("departamento");
				
				//t.update(transaction);

				System.out.println("Name: "+t.getName());
				System.out.println("Path: " +t.getPath());
				System.out.println("TableID: "+t.getTableID());
				//System.out.println("NumberOfBlocks: "+t.getNumberOfBlocks());
				//System.out.println("NumberOfTuples: "+t.getNumberOfTuples());
				System.out.println("ColumnStructure: "+Arrays.toString(t.getColumnNames()));
				
				//System.out.println(Kernel.getCatalog().getTable("1").getName());
				
			
				/*
				ArrayList<ITuple> dsa = t.getTuplesFromBlock(tr,"1");
				for (ITuple tuple : dsa) {
					System.out.println(Arrays.toString(tuple.getData()));
				}
				*/
				
				TableScan tr2 = new TableScan(transaction, t);
				int c = 0;
				ITuple tuple = tr2.nextTuple();
				
				
				while(tuple!=null){
					
					
					System.out.println(Arrays.toString(tuple.getData()));
					
					tuple = tr2.nextTuple();
			
					c++;
				}
				
			//	DrawTable ij = new DrawTable( tr, t);
				//ij.reloadMatriz();
				System.out.println(">>>"+c);
				
				System.out.println(t.getNumberOfBlocks(transaction));
				
				//printFile("temp"+File.separator+"orders2.b");
				
				transaction.commit();
				
			}

			@Override
			public void onFail(ITransaction transaction, Exception e) {
				transaction.abort();
				
			}
			
		});
		
		
		
	}

}
