package tests;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import DBMS.Kernel;
import DBMS.bufferManager.IPage;
import DBMS.connectionManager.DBConnection;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.queryProcessing.ITable;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;





public class TestTPCCLoad {
	
	
	public static int count = 0;
	
	public static void main(String[] args) {
		
		
		try {
			
			Kernel.ENABLE_RECOVERY = false;
			Kernel.ENABLE_IN_MEMORY_MODE = true;
			Kernel.ENABLE_TEMP_BUFFER = true;
			
			Kernel.start(); 
			
			DBConnection connection = DistributedTransactionManagerController.getInstance().getLocalConnection("tpch", "admin", "admin");
		
			Thread.sleep(1000);
			
			Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
				
				@Override
				public void run(ITransaction transaction) {
		
					System.out.println("\n <... Load Process - Database Block Size: " + Kernel.BLOCK_SIZE + " bytes ...> \n");
					
					String tpchSourceFile = "D:\\tpcc\\";
					
					int total = 0;
					
				
					try {
			/*			

					loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("customer"), tpchSourceFile);
					loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("district"), tpchSourceFile);
					loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("history"), tpchSourceFile);
					loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("item"), tpchSourceFile);
					loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("new_order"), tpchSourceFile);
					loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("order_line"), tpchSourceFile);
					loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("orders"), tpchSourceFile);
					loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("stock"), tpchSourceFile);
					loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("warehouse"), tpchSourceFile);
*/


					
					total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("customer"));
					total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("district"));
					total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("history"));	
				    total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("item"));
					total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("new_order"));
					total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("order_line"));
					total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("orders"));
					total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("stock"));
					total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("warehouse"));
					
					//showDetails(null, null);
					
					int inMemoryPages = 0;
					for (IPage p : Kernel.getBufferManager().getBufferPolicy().getPages()) {
						inMemoryPages++;
						if(p.getTuplesCache()!=null)System.out.println("ERROR!");
					}
					
					
					System.out.println("Total Number of Blocks: " + total);
					System.out.println("Policy Current Number of Pages: " + inMemoryPages);
						
					} catch (FileNotFoundException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					transaction.commit();
					
					System.out.println("\n <... Finish Load ...>");
					System.gc();
					//System.exit(0);
				}
				
				@Override
				public void onFail(ITransaction transaction, Exception e) {
					System.out.println("TestTPCCLoad -: ERROR->>>> " + count) ;
					System.out.println(e.getMessage());
					transaction.abort();
					System.exit(0);
				}
			},false,false);
			
			
			connection = null;
			
		}catch (Exception e) {
			e.printStackTrace();
			System.out.println("ERROR->>>> " + count) ;
			System.exit(0);
		}
		


		
		
	}
		
	
	public static void loadTable(ITransaction transaction, ITable table,String fileName) throws FileNotFoundException, InterruptedException {
		fileName = fileName+table.getName()+".csv";
		
		System.out.println("-- Start -- table: " + table + " file: " + fileName);
		
		System.out.println("Number of blocks:" + table.getNumberOfBlocks(transaction));
		//System.out.println(table.getNumberOfTuples(transaction));
		
		//if(true)return;
		
		File file = new File(fileName);
		Scanner input = new Scanner(file);
		count = 0;
		input.nextLine();
		while (input.hasNextLine()) {
		    String tuple = input.nextLine();
	
		   // System.out.println(tuple);
		    String [] data = tuple.split(",");
		    String newTuple = "";
		    for (String string : data) {
		    	if(isNumeric(string)) {
		    		newTuple += string + "|";
		    	}else {
		    		newTuple += "'"+string.trim()+"'" + "|";
		    	}
			}
		    
		   //System.out.println(newTuple);
		    //if(true)return;
		    
		    table.writeTuple(transaction, newTuple);
		   // System.gc();
		  //  Thread.sleep(10);
		
		    count++;
		    if(count % 10000 == 0)
		    	System.out.println("..."+count);
		    
		    
		    
		}
		Kernel.getBufferManager().flush();
		input.close();
		
		System.out.println("-- Finish -- table: " + table + " file: " + fileName + " ---> " + count + " inserted" );
		
	}
	
	
	
	private static int showDetails(ITransaction transaction, ITable table)  throws FileNotFoundException, InterruptedException {
		if(transaction==null)return 0;
		long lStartTime = System.nanoTime();
		System.out.println("--------------------------------------------------------");
		System.out.println("Table:" + table.getName());
		int blocks = table.getNumberOfBlocks(transaction);
		System.out.println("Blocks: " + blocks);
		System.out.println("Tuples: " + table.getNumberOfTuples(transaction));
		long lEndTime = System.nanoTime();
		long output = lEndTime - lStartTime;
	    System.out.println("Table Scan (Elapsed time): " + output / 1000000 + " ms");
		return blocks;
	}
	
	
	private static boolean isNumeric(String a) {
		try {
			Double.parseDouble(a);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	/*
	 * 	public static void loadTable(ITransaction transaction, ITable table,String fileName) throws FileNotFoundException, InterruptedException {
		
		System.out.println("-- Start -- table: " + table + " file: " + fileName);
		
		//System.out.println(table.getNumberOfBlocks(transaction));
		//System.out.println(table.getNumberOfTuples(transaction));
		
		//if(true)return;
		
		File file = new File(fileName);
		Scanner input = new Scanner(file);
		int count = 0;
		while (input.hasNextLine()) {
		    String tuple = input.nextLine();
		  //  System.out.println(tuple);
		    
		    String sql = "INSERT INTO " + table.getName() + " (";
			
			for (int i = 0; i < table.getColumnNames().length; i++) {
				sql += table.getColumnNames()[i];
				if (i != table.getColumnNames().length - 1) {
					sql += ", ";
				}
			}
			
			sql+=") VALUES (";
			
			String [] data = tuple.split("\\|");
		    
		    for (String string : data) {
		    	if(isNumeric(string)) {
		    		sql += string;
		    	}else {
		    		sql += "'"+string.trim()+"'";
		    	}
		    	if(string != data[data.length-1]) {
		    		sql+=",";
		    	}
			}
			
			sql+=")";
				
			System.out.println(sql);
			    
		    
		    System.gc();
		    Thread.sleep(100);
		    count++;
		    if(count % 10000 == 0)
		    	System.out.println(count);
		    
		}
		
	
		Kernel.getBufferManager().flush();
		
		System.out.println("-- Finish -- table: " + table + " file: " + fileName + " ---> " + count + " inserted" );
		
	}
	 */
}
