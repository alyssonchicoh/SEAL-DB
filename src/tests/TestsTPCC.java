package tests;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;

import DBMS.Kernel;
import DBMS.bufferManager.policies.ARC;
import DBMS.bufferManager.policies.LRU;
import DBMS.bufferManager.policies.LRU2Q;
import DBMS.connectionManager.DBConnection;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.parse.Parse;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.AggregationOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.FilterOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.GroupResultsOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.JoinOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.ProjectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SelectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SortOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.Transaction;
import DBMS.transactionManager.TransactionRunnable;
import javafx.util.Callback;

public class TestsTPCC implements Callback<ITransaction,ITransaction>{
	
	
	
	public static void main(String[] args) throws InterruptedException {
		
		if(args != null && args.length > 0) {
			if(args[0].equals("ARC")) {
				Kernel.setBufferPolicy(ARC.class);
			}
			if(args[0].equals("LRU2Q")) {
				Kernel.setBufferPolicy(LRU2Q.class);	
			}
		}
		
		
		Kernel.ENABLE_RECOVERY = false;
		Kernel.ENABLE_IN_MEMORY_MODE = true;
		Kernel.ENABLE_TEMP_BUFFER = true;
		Kernel.ENABLE_LOG_REQUESTS = false;
		
		Kernel.start(); 
		
		TestsTPCC test = new TestsTPCC();
		test.numberOfTransactions = 1;
		test.serial = true;
		
		test.startBenchmark();
	}
	
	
	
	private int numberOfTransactions;
	private boolean serial;
	private int tIds = 1;
	
	private DBConnection connection; 
	long lStartTime;
	
	public void startBenchmark() throws InterruptedException {
		
		connection = DistributedTransactionManagerController.getInstance().getLocalConnection("tpcc", "admin", "admin");
		
		System.out.println("\n\n <... Starting TPCC Benchmark ...>");
		System.out.println("Number of Transactions:" + numberOfTransactions);
		System.out.println("Serial: " + serial);
		System.out.println();
		Thread.sleep(2000);
	
		lStartTime = System.nanoTime();
		
		if(!serial) {
			for (int i = 0; i < numberOfTransactions; i++) {
				executeTransaction(connection, this);
			}
		}else {
			executeTransaction(connection, this);
		}
	
	}
	
	private int countExecutions = 0;
	
	@Override
	public synchronized ITransaction call(ITransaction t) {
		countExecutions++;
		if(countExecutions == numberOfTransactions) {
			System.out.println("\n\n <... Finish TPCC Benchmark ...> total time: " + (System.nanoTime() - lStartTime) / 1000000 + " ms");
			
		    // ((BufferManager)Kernel.getBufferManager()).close(); //persist log request
			Kernel.getBufferManager().getBufferPolicy().savePage();
			Kernel.getBufferManager().saveColdData();
			System.exit(0);
		}
		if(serial==true) {
			executeTransaction(connection, this);
		}
		return null;
	}
	
	public void executeTransaction(DBConnection connection, Callback<ITransaction,ITransaction> call) {
		
		Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
			@Override
			public void run(ITransaction transaction) {
				if(serial==false)((Transaction)transaction).setIdT(tIds++);
				
				long lStartTime = System.nanoTime();
				
				
				
				calcTime(() -> Q1(transaction),"T"+transaction.getIdT()+"-Q1");
				//calcTime(() -> Q2(transaction),"T"+transaction.getIdT()+"-Q2");
				//calcTime(() -> Q3(transaction),"T"+transaction.getIdT()+"-Q3");
				//calcTime(() -> Q4(transaction),"T"+transaction.getIdT()+"-Q4");
				//calcTime(() -> Q5(transaction),"T"+transaction.getIdT()+"-Q5");

				
				transaction.commit();
				System.out.println("\n <... Finish Transaction T"+transaction.getIdT()+" ...> total time: " + (System.nanoTime() - lStartTime) / 1000000 + " ms");
				if(call!=null)call.call(transaction);
			}
			
			@Override
			public void onFail(ITransaction transaction, Exception e) {
				System.out.println(e.getMessage());
				transaction.abort();
				//System.exit(0);
			}
		},false,true);
	}
	

	

	@SuppressWarnings("deprecation")
	public static void Q1(ITransaction transaction) { 
		
		
		String datetime = new Date().getYear()+"";

		String w_id = "1"; //PARAM?
		String d_id = "2"; //PARAM?
		String c_id = "3"; //PARAM?

		String sql = " SELECT c_discount, c_last, c_credit, w_tax " +
		// " INTO :c_discount, :c_last, :c_credit, :w_tax
		" FROM customer, warehouse " +
		" WHERE w_id = " + w_id + " AND c_w_id = w_id AND " + 
		" c_d_id = " + d_id + " AND c_id = " + c_id + "; ";
	
		IntoVariable c_discount = new IntoVariable();
		IntoVariable c_last = new IntoVariable();
		IntoVariable c_credit = new IntoVariable();
		IntoVariable w_tax = new IntoVariable();
		
		if(execAndIntoSQL(sql,transaction,c_discount,c_last,c_credit, w_tax) == null) return;
	
//		System.out.println(c_discount);
//		System.out.println(c_last);
//		System.out.println(c_credit);
//		System.out.println(w_tax);
		
		IntoVariable d_next_o_id = new IntoVariable();
		IntoVariable d_tax = new IntoVariable();
		
		sql = " SELECT d_next_o_id, d_tax " +
			// "INTO :d_next_o_id, :d_tax "+
			" FROM district "+
			" WHERE d_id = "+d_id+" AND d_w_id = "+w_id+";";
				
		if(execAndIntoSQL(sql, transaction, d_next_o_id,d_tax) == null) return;
		
//		System.out.println(d_next_o_id);
//		System.out.println(d_tax);
		
		

		sql = " UPDATE district SET d_next_o_id = "+ (new Integer(d_next_o_id.v) + 1) +
				 " WHERE d_id = " + d_id+ " AND d_w_id = " + w_id;
		
		execSQL(sql, transaction);
		
		String  o_id = d_next_o_id.v;
		
		
		String o_ol_cnt = "3"; //PARAM?
		String o_all_local = "2"; //PARAM?
		
		sql =  " INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,o_entry_d, o_ol_cnt, o_all_local) "+
			   " VALUES ("+o_id +", "+d_id+", "+w_id+", "+c_id+", "+ datetime +", "+o_ol_cnt+", "+o_all_local+");";
		
		
		execSQL(sql, transaction);
		
		sql =  "insert into -new_order- (no_o_id, no_d_id, no_w_id) VALUES ("+o_id+", "+d_id+", "+d_id+");";
		sql = sql.replaceAll("-", " ");
		execSQL(sql, transaction);
		
		
		
		String supware[] = {"1","2","6"}; //???
		String itemid[] = {"1","2","6"};  //???
		String qty[] = {"1","2","6"};
		
		String iname[] = new String[new Integer(o_ol_cnt)];
		String price[] = new String[new Integer(o_ol_cnt)];
		String stock[] = new String[new Integer(o_ol_cnt)];
		
		char bg[] = new char[new Integer(o_ol_cnt)];
		
		double amt[] = new double[new Integer(o_ol_cnt)];
		double total = 0;
		
		for (int ol_number = 1; ol_number <= new Integer(o_ol_cnt); ol_number++) {
			
			int ol_supply_w_id = new Integer(supware[ol_number-1]); //=atol(supware[ol_number-1])
			if (ol_supply_w_id != new Integer(w_id)) o_all_local = "0"; 
			int ol_i_id = new Integer(itemid[ol_number-1]);
			int ol_quantity = new Integer(qty[ol_number-1]); 
			
			//EXEC SQL WHENEVER NOT FOUND GOTO invaliditem; ????
			
			IntoVariable i_price = new IntoVariable();
			IntoVariable i_name = new IntoVariable();
			IntoVariable i_data = new IntoVariable();
			
			sql = " SELECT i_price, i_name , i_data "+
					// INTO :i_price, :i_name, :i_data
					 " FROM item "+ 
					 " WHERE i_id = "+ol_i_id; 
			
			if(execAndIntoSQL(sql, transaction, i_price, i_name, i_data) == null) continue;
			
//			System.out.println(i_price);
//			System.out.println(i_name);
//			System.out.println(i_data);
						
			 price[ol_number-1] = i_price.v; 
			 iname[ol_number-1] = i_name.v; //strncpy(iname[ol_number-1],i_name,24);
			 
			 //EXEC SQL WHENEVER NOT FOUND GOTO sqlerr; 
			 
			 IntoVariable s_quantity = new IntoVariable();
			 IntoVariable s_data = new IntoVariable();
			 IntoVariable s_dist_01 = new IntoVariable();
			 IntoVariable s_dist_02 = new IntoVariable();
			 IntoVariable s_dist_03 = new IntoVariable();
			 IntoVariable s_dist_04 = new IntoVariable();
			 IntoVariable s_dist_05 = new IntoVariable();
			// Param s_dist_06 = new Param(); Sem dist06
			 IntoVariable s_dist_07 = new IntoVariable();
			 IntoVariable s_dist_08 = new IntoVariable();
			 IntoVariable s_dist_09 = new IntoVariable();
			 IntoVariable s_dist_10 = new IntoVariable();
			 
			
			 sql = " SELECT s_quantity, s_data, "+
					 " s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
					// " s_dist_06, "
					 + " s_dist_07, s_dist_08, s_dist_09, s_dist_10 "+
	
					 " FROM stock "+
					 " WHERE s_i_id = "+ol_i_id+" AND s_w_id = "+ol_supply_w_id +";";
			 
			 if(execAndIntoSQL(sql, transaction, 
					 s_quantity,
					 s_data,
					 s_dist_01,
					 s_dist_02,
					 s_dist_03,
					 s_dist_04,
					 s_dist_05,
				//	 s_dist_06,
					 s_dist_07,
					 s_dist_08,
					 s_dist_09,
					 s_dist_10) == null) return;
			 
			
				
			 double ol_dist_info = 0;
			 //pick_dist_info(ol_dist_info, ol_w_id); //pick correct s_dist_xx
			 stock[ol_number-1] = s_quantity.v; 
			 
			
			 
			if ((strstr(i_data.v, "original") > 0) && (strstr(s_data.v, "original") > 0)) {
				bg[ol_number - 1] = 'B';
			} else {
				bg[ol_number - 1] = 'G';

			}
			
			 if (new Integer(s_quantity.v) > ol_quantity) {
				 s_quantity.v = new Integer(s_quantity.v) - ol_quantity + "";	 
			 }else{
				 s_quantity.v = (new Integer(s_quantity.v) - ol_quantity + 91)+"";				 
			 }
			 
			 
			 sql = " UPDATE stock SET s_quantity = "+ s_quantity +
					 "  WHERE s_i_id = "+ ol_i_id +
					 " AND s_w_id = "+ol_supply_w_id + ";"; 
			 
			 execSQL(sql, transaction);
			 
			 double ol_amount = new Integer(ol_quantity) * 
					 new Double(i_price.v) * 
					 (1+ new Double(w_tax.v) + new Double(d_tax.v)) * 
					 (1 - new Double(c_discount.v));
			 
			 amt[ol_number-1] = ol_amount;
			 total += ol_amount; 
			
			 
			 sql = 
			" INSERT "+
			"  INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, "+
			" ol_quantity, ol_amount, ol_dist_info) "+
			"  VALUES ("+ o_id +" , " + d_id + " , " + w_id + " , " + ol_number + " , " +
			"  " + ol_i_id + " , "+ ol_supply_w_id + " , " +
			"  " + ol_quantity + " , " + ol_amount + " , " + ol_dist_info + "); ";
			 
			execSQL(sql, transaction);
		}
		
		//EXEC SQL COMMIT WORK; 
		
	}

	public static void Q2(ITransaction transaction) {
			
		String datetime = new Date().toString();
		
		String w_id = "1";
		String h_amount = "10";
	
		
		String sql =  "UPDATE warehouse SET w_ytd = " + h_amount+ //w_ytd = w_ytd + h_amount
					  "WHERE w_id= "+w_id+";"; 
		
		execSQL(sql, transaction);
		
	
		sql = " SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name "+
		 //INTO :w _street_1, :w_street_2, :w_city, :w _state, :w_zip, :w_name
		 " FROM warehouse "+
		 " WHERE w_id ="+ w_id;
		
		IntoVariable w_street_1 = new IntoVariable();
		IntoVariable w_street_2 = new IntoVariable();
		IntoVariable w_city = new IntoVariable();
		IntoVariable w_state = new IntoVariable();
		IntoVariable w_zip = new IntoVariable();
		IntoVariable w_name = new IntoVariable();
		
		if(execAndIntoSQL(sql, transaction, w_street_1,w_street_2,w_city,w_state,w_zip,w_name) == null) return;
		
		
		String d_id = "2";
		
		sql =  "UPDATE  district  SET d_ytd  = " + h_amount + //d_ytd  = d_ytd  + h_amount
				"WHERE d_w_id= " + w_id + " AND d_id="+d_id; 
	
		execSQL(sql, transaction);
		
		IntoVariable d_street_1 = new IntoVariable();
		IntoVariable d_street_2 =new IntoVariable();
		IntoVariable d_city = new IntoVariable();
		IntoVariable d_state = new IntoVariable();
		IntoVariable d_zip = new IntoVariable();
		IntoVariable d_name = new IntoVariable();
		
		sql = "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name "+
		// INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
		 " FROM district " +
		 " WHERE d_w_id="+w_id+" AND d_id = "+d_id+"; ";
		
		if(execAndIntoSQL(sql, transaction, d_street_1, d_street_2,d_city,d_state,d_zip,d_name) == null) return;
		
		
		String byname = ""; //param?
		
		IntoVariable namecnt = new IntoVariable();
		
		String c_last = "3";
		String c_d_id = "10";
		String c_w_id = "5";
		
		
		if(byname != null) {
		
			sql = "SELECT count(c_id) "+
					" FROM customer " +
					"WHERE c_last= "+c_last+" AND c_d_id= "+c_d_id+" AND c_w_id= " + c_w_id; 
			
			if(execAndIntoSQL(sql,transaction,namecnt) == null) return;
			//CURSOR
			sql = "SELECT * "+
					" FROM customer " +
					"WHERE c_last= "+c_last+" AND c_d_id= "+c_d_id+" AND c_w_id= " + c_w_id; 
			
			ITable result = execSQL(sql, transaction);
			
			
			if (Integer.parseInt(namecnt.v) % 2 == 0) {
				namecnt.v = Integer.parseInt(namecnt.v)+1+""; //Locate midpoint customer
			}
	
		
			TableScan cursor = new TableScan(transaction, result);
			ITuple tuple = cursor.nextTuple();
			
			for (int n=0; n<Integer.parseInt(namecnt.v)/2  && tuple!=null; n++){ 
				//System.out.println(Arrays.toString(tuple.getData()));
				tuple = cursor.nextTuple();
			}
			
			
			
		}else {
			
			sql = " SELECT c_first, c_middle, c_last, " +
					" c_street_1, c_street_2, c_city, c_state, c_zip, "+
					"  c_phone, c_credit, c_credit_lim, "+
					"  c_discount, c_balance, c_since "+
					" FROM customer " +
					"WHERE c_last= "+c_last+" AND c_d_id= "+c_d_id+" AND c_w_id= " + c_w_id; 
			
			
				IntoVariable c_first = new IntoVariable();
				IntoVariable c_middle = new IntoVariable();
				IntoVariable c_last_2 = new IntoVariable();
				IntoVariable c_street_1 = new IntoVariable();
				IntoVariable c_street_2 = new IntoVariable();
				IntoVariable c_city = new IntoVariable();
				IntoVariable c_state = new IntoVariable();
				IntoVariable c_zip = new IntoVariable();
				IntoVariable c_phone = new IntoVariable();
				IntoVariable c_credit = new IntoVariable();
				IntoVariable c_credit_lim = new IntoVariable();
				IntoVariable c_discount = new IntoVariable();
				IntoVariable c_balance = new IntoVariable();
				IntoVariable c_since = new IntoVariable();
				
				if(execAndIntoSQL(sql, transaction, c_first, c_middle, c_last_2,
						 c_street_1, c_street_2, c_city, c_state, c_zip,
						 c_phone, c_credit, c_credit_lim,
						 c_discount, c_balance, c_since ) == null) return;
				
				c_balance.v = Double.parseDouble(c_balance.v) + Double.parseDouble(h_amount) + "";
				//c_credit[2]=' \0'; 
				
				String c_id = "3";
				
				if (strstr(c_credit.v, "BC") == 0 ){
				
				IntoVariable c_data = new IntoVariable();
				
				
				
				sql = "  SELECT c_data " +
						//+ "INTO :c_data " +
					" FROM customer " +
					" WHERE c_w_id = "+c_w_id+" AND c_d_id = "+c_d_id+" AND c_id = "+c_id+";";
				
				if(execAndIntoSQL(sql,transaction,c_data) == null) return;
				
				/*
				sprintf(c_new_data,"| %4d %2d %4d %2d %4d $%7.2f %12c %24c",
	 			c_id,c_d_id,c_w_id,d_id,w_id,h_amount,
	 			h_date, h_data);
	 			strncat(c_new_data,c_data,500-strlen(c_new_data)); 
				 */
				
				String c_new_data = c_data +" "+ c_id +" "+c_d_id +" "+c_w_id +" "+d_id +" "+ w_id +" "+h_amount +" ";
				
				sql = " UPDATE customer "+
				 " SET c_balance = " +c_balance+", c_data = "+c_new_data +
				 " WHERE c_w_id = "+c_w_id+" AND c_d_id = "+c_d_id+" AND "+
				 " c_id = "+c_id+"; "; 
				
				execSQL(sql, transaction);
					
				}else {
	
					sql = " UPDATE customer SET c_balance = "+ c_balance +
	 					" WHERE c_w_id = "+c_w_id+" AND c_d_id = "+c_d_id+" AND "+
	 					" c_id = " + c_id+ ";"; 
					 
					execSQL(sql, transaction);
					
				}
				
				String h_data = w_name.v + " "+ d_name.v;
				
				sql = " INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, " +
						" h_w_id, h_date, h_amount, h_data) " +
						" VALUES ("+c_d_id+", "+c_w_id+", "+c_id+", "+d_id+", " +
						w_id+" , "+datetime+", " + h_amount + ", "+h_data+");  ";
				
				execSQL(sql, transaction);
			
		}
		
		
		
		
		
	}
			
	
	public static void Q3(ITransaction transaction) {
	
		String byname = null;
		
		String sql;
		String d_id = "1";
		String w_id = "1";
		
		if (byname != null){
			
			String c_last = "2";
	
			
			IntoVariable namecnt = new IntoVariable();
			
			sql = " SELECT count(c_id) " +
					//INTO :namecnt
					" FROM customer "+
					" WHERE c_last="+c_last+" AND c_d_id= "+d_id+" AND c_w_id= " + w_id + ";";
			
			if(execAndIntoSQL(sql, transaction, namecnt) == null)return;
			
			sql = " SELECT c_balance, c_first, c_middle, c_id "+
			 " FROM customer "+
			 " WHERE c_last = "+c_last+" AND c_d_id ="+d_id+" AND c_w_id = "+w_id+ " "+
			 " ORDER BY c_first; ";
		
			ITable result = execSQL(sql, transaction);
	
			TableScan c_name = new TableScan(transaction, result);
			ITuple tuple = c_name.nextTuple();
	
			if (Double.parseDouble(namecnt.v) % 2 == 0) namecnt.v = (Double.parseDouble(namecnt.v) + 1 ) +"";  // Locate midpoint customer
			
			for (int n=0; n<Double.parseDouble(namecnt.v)/ 2 && tuple!=null; n++){
			 
				/*
				 * EXEC SQL FETCH c_name
	 				INTO :c_balance, :c_first, :c_middle, :c_id; 
				 */
				
				tuple =  c_name.nextTuple();
			}
	
		}else {
			IntoVariable c_balance = new IntoVariable();
			IntoVariable c_first = new IntoVariable();
			IntoVariable c_middle = new IntoVariable();
			IntoVariable c_last = new IntoVariable();

			String c_id = "6";
			
			sql = " SELECT c_balance, c_first, c_middle, c_last "+
			 //" INTO :c_balance, :c_first, :c_middle, :c_last "+
			 " FROM customer "+
			 " WHERE c_id = "+c_id+" AND c_d_id = "+d_id+" AND c_w_id = "+w_id+";  ";
			
			if(execAndIntoSQL(sql, transaction, c_balance,c_first,c_middle,c_last) == null)return;;
			
		}
		
		IntoVariable o_id = new IntoVariable(); 
		IntoVariable o_carrier_id = new IntoVariable();
		IntoVariable entdate = new IntoVariable();
	
		
		sql = " SELECT o_id, o_carrier_id, o_entry_d"+
		 // INTO :o_id, :o_carrier_id, :entdate
		 " FROM orders "+
		 " ORDER BY o_id DESC;"; 
		
		if(execAndIntoSQL(sql, transaction, o_id,o_carrier_id,entdate)== null)return;;
		
		sql = " SELECT ol_i_id, ol_supply_w_id, ol_quantity, "+
		 " ol_amount, ol_delivery_d "+
		 " FROM order_line "+
		 " WHERE ol_o_id="+o_id+" AND ol_d_id="+d_id+" AND ol_w_id="+w_id+";";
		
		ITable result = execSQL(sql, transaction);
	
		TableScan  c_line = new TableScan(transaction, result);
		ITuple tuple =  c_line.nextTuple();
		
		while(tuple!=null) {
			/*
			  	EXEC SQL FETCH c_line
	 			INTO :ol_i_id[i], :ol_supply_w_id[i], :ol_quantity[i],
	 			:ol_amount[i], :ol_delivery_d[i]; 
			 */
			
			tuple = c_line.nextTuple();
		}
			
	}
	
	public static void Q4(ITransaction transaction) {
	
		String datetime = "12";
	
		int DIST_PER_WARE = 1;
		
		 for (int d_id=1; d_id <= DIST_PER_WARE; d_id++){ 
		
			String w_id = "1";
				 
			IntoVariable no_o_id = new IntoVariable();
			
			String sql = "SELECT no_o_id " +
			" FROM new_order " +
			" WHERE no_d_id = "+d_id+" AND no_w_id = "+w_id+" " +
			" ORDER BY no_o_id; ";
			
			//EXEC SQL DELETE FROM new_order WHERE CURRENT OF c_no; 
			
			if(execAndIntoSQL(sql, transaction,no_o_id) == null)return;
			
			sql = " SELECT o_c_id "+
					//INTO :c_id 
					" FROM orders "+
					" WHERE o_id = "+no_o_id+" AND o_d_id = "+d_id+" AND "+
					" o_w_id = "+w_id+"; ";
			
		
			IntoVariable c_id = new IntoVariable();
			
			if(execAndIntoSQL(sql, transaction, c_id) == null)return;
		
		
			String o_carrier_id = "15";
			
			sql =  " UPDATE orders SET o_carrier_id = "+ o_carrier_id +
					" WHERE o_id = "+no_o_id+" AND o_d_id = "+d_id+" AND "+
					" o_w_id = "+w_id+"; ";
			
			execSQL(sql, transaction);
			
			sql =  "UPDATE order_line SET ol_delivery_d = " + datetime +
					" WHERE ol_o_id = "+no_o_id+" AND ol_d_id = "+d_id+" AND "+
					" ol_w_id = "+w_id+"; ";
			
			execSQL(sql, transaction);
		
			IntoVariable ol_total = new IntoVariable(); 
			
			sql = "SELECT SUM(ol_amount) "+
					 //INTO :ol_total
					 " FROM order_line "+
					 " WHERE ol_o_id = "+no_o_id+" AND ol_d_id = "+d_id+" AND "+
					 " ol_w_id = "+w_id+"; ";
			
			if(execAndIntoSQL(sql, transaction, ol_total) == null)return;
			
			sql =  "UPDATE customer SET c_balance = "+ ol_total + //c_balance = c_balance + ol_total 
					" WHERE c_id = "+no_o_id+" AND  c_d_id  = "+d_id+" AND "+
					 " c_w_id = "+w_id+"; ";
			
			execSQL(sql, transaction);
			 
		 }
		
	}
	
		
	@SuppressWarnings("deprecation")
	public static void Q5(ITransaction transaction) { //slev() 
		
	
	String w_id = "1"; //PARAM?
	String d_id = "2"; //PARAM?
	
	String sql = "Select d_next_o_id "+
				//INTO :o_id
				" FROM district "+
				" WHERE d_w_id="+w_id+" AND d_id="+d_id+"; ";
	
	IntoVariable o_id = new IntoVariable();
	
	if(execAndIntoSQL(sql, transaction, o_id) == null)return;
	
	
	String threshold = "20";
	
	sql =  " SELECT COUNT(s_i_id) "+ //SELECT COUNT(DISTINCT (s_i_id)) 
			//"INTO :stock_count "+
			" FROM order_line, stock "+
			" WHERE ol_w_id= "+ w_id +" AND "+
			" ol_d_id= " + d_id +" AND ol_o_id < "+ o_id.v +" AND "+
			" ol_o_id >= "+(new Integer(o_id.v)-20)+" AND s_w_id = "+ w_id +" AND "+
			" s_i_id = ol_i_id AND s_quantity < "+ threshold +"; ";
		
	
	IntoVariable stock_count = new IntoVariable();
	
	if(execAndIntoSQL(sql, transaction, stock_count) == null)return;
	
	
	}
	

	
	public static String getFirstResult(ITransaction transaction, ITable result) {
		ITuple t = new TableScan(transaction, result).nextTuple();
		if (t != null) {
			return t.getStringData();
		} else {
			return null;
		}

	}

	public static void showResult(ITransaction transaction, ITable result, int lines) {
		System.out.println("\n---------------------------------------- " + result.getName()
				+ "---------------------------------------- ");
		System.out.println(Arrays.toString(result.getColumnNames()));
		int count = 0;
		TableScan tr2 = new TableScan(transaction, result);
		ITuple tuple = tr2.nextTuple();
		while (tuple != null) {
			System.out.println(Arrays.toString(tuple.getData()));
			tuple = tr2.nextTuple();
			count++;
			if (lines > 0 && count == lines)
				break;
		}
		System.out
				.println("\n--" + count + " tuples -----------------------------------------------------------------");
	}

	public static TableOperation newTable(Plan plan, String tableName) {
		ITable table = Kernel.getCatalog().getSchemabyName("tpch").getTableByName(tableName);
		TableOperation tableOP = new TableOperation();
		tableOP.setPlan(plan);
		tableOP.setResultLeft(table);
		return tableOP;
	}

	public static ProjectionOperation newProjection(Plan plan, String... columns) {
		ProjectionOperation projectionOP = new ProjectionOperation();
		projectionOP.setPlan(plan);
		projectionOP.setAttributesProjected(columns);
		return projectionOP;
	}

	public static SelectionOperation newSelection(Plan plan, Condition... conditions) {
		SelectionOperation selectOP = new SelectionOperation();
		selectOP.setPlan(plan);
		for (Condition condition : conditions) {
			selectOP.getAttributesOperatorsValues().add(condition);
		}
		return selectOP;
	}

	public static JoinOperation newJoin(Plan plan, Condition... conditions) {
		JoinOperation joinOP = new JoinOperation();
		joinOP.setPlan(plan);
		for (Condition condition : conditions) {
			joinOP.getAttributesOperatorsValues().add(condition);
		}
		return joinOP;
	}

	public static SortOperation newSort(Plan plan, boolean isASC, String... columns) {
		SortOperation sortOP = new SortOperation();
		sortOP.setPlan(plan);
		sortOP.setColumnSorted(columns);
		sortOP.setOrder(isASC);
		return sortOP;
	}

	public static AggregationOperation newAggregation(Plan plan, Condition... conditions) {
		AggregationOperation aggregationOP = new AggregationOperation();
		aggregationOP.setPlan(plan);
		for (Condition condition : conditions) {
			aggregationOP.getAttributesOperatorsValues().add(condition);
		}
		return aggregationOP;
	}

	public static FilterOperation newFilterOperation(Plan plan, Condition... conditions) {
		FilterOperation filterOP = new FilterOperation();
		filterOP.setPlan(plan);
		for (Condition condition : conditions) {
			filterOP.getAttributesOperatorsValues().add(condition);
		}
		return filterOP;
	}

	public static GroupResultsOperation newGroupResultsOperation(Plan plan, AbstractPlanOperation result0,
			AbstractPlanOperation result1) {
		GroupResultsOperation groupResultsOP = new GroupResultsOperation();
		groupResultsOP.setLeft(result0);
		groupResultsOP.setRight(result1);
		groupResultsOP.setPlan(plan);
		return groupResultsOP;
	}

	public static void calcTime(Runnable r, String name) {
		System.out.println("---> " + name + " Running...");
		long lStartTime = System.nanoTime();
		r.run();
		long lEndTime = System.nanoTime();
		long output = lEndTime - lStartTime;
		System.out.println("<--- Finish " + name + " Time: " + output / 1000000 + " ms");
	}

	public static int strstr(String haystack, String needle) {
		if (haystack == null || needle == null)
			return 0;

		if (needle.length() == 0)
			return 0;

		for (int i = 0; i < haystack.length(); i++) {
			if (i + needle.length() > haystack.length())
				return -1;

			int m = i;
			for (int j = 0; j < needle.length(); j++) {
				if (needle.charAt(j) == haystack.charAt(m)) {
					if (j == needle.length() - 1)
						return i;
					m++;
				} else {
					break;
				}

			}
		}

		return -1;
	}

	public static ITable execSQL(String sql, ITransaction transaction) {
		System.out.println("T"+transaction.getIdT()+" Exec SQL: " + sql);
		try {
			Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpcc"));
			plan.setTransaction(transaction);
			ITable result = plan.execute();
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static class IntoVariable {
		String v;

		public String toString() {
			return v;
		}
	}

	public static IntoVariable[] execAndIntoSQL(String sql, ITransaction transaction, IntoVariable... columns) {
		System.out.println("T"+transaction.getIdT()+" Exec SQL: " + sql);
		try {
			Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpcc"));
			plan.setTransaction(transaction);
			// showResult(transaction, plan.execute());
			ITable result = plan.execute();
			// System.out.println(Arrays.toString(result.getColumnNames()));
			String data = getFirstResult(transaction, result);
			if (data == null) {
				System.out.println("^^^^^ [null values result] ^^^^^");
				return null;
			}
			// System.out.println(data);
			String values[] = data.split("\\|");
			// System.out.println(columns.length + " - " + values.length);

			for (int i = 0; i < columns.length; i++) {
				columns[i].v = values[i];

			}
			return columns;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}




}
