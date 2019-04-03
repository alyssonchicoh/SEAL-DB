package tests;

import java.sql.SQLException;
import java.util.Arrays;

import DBMS.Kernel;
import DBMS.bufferManager.BufferManager;
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
import DBMS.transactionManager.TransactionRunnable;

public class TestsTPCH {
	
	
	
	public static void main(String[] args) throws InterruptedException {
		
		//Kernel.setBufferPolicy(LRU2Q.class);
		
		Kernel.ENABLE_RECOVERY = false;
		Kernel.ENABLE_IN_MEMORY_MODE = true;
		Kernel.ENABLE_TEMP_BUFFER = true;
		
		Kernel.start(); 
		
		DBConnection connection = DistributedTransactionManagerController.getInstance().getLocalConnection("tpch", "admin", "admin");

		
		System.out.println("\n <... Starting TPCH Benchmark ...>");
		Thread.sleep(1000);

		Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
			
			@Override
			public void run(ITransaction transaction) {
				calcTime(() -> Q1(transaction));
				calcTime(() -> Q2(transaction));
				calcTime(() -> Q3(transaction));
				calcTime(() -> Q4(transaction));
				calcTime(() -> Q5(transaction));
				calcTime(() -> Q6(transaction));
				//Q7(transaction);
				//Q8(transaction);
				calcTime(() -> Q10(transaction));
				calcTime(() -> Q11(transaction));
				calcTime(() -> Q12(transaction));
				calcTime(() -> Q13(transaction));
				calcTime(() -> Q14(transaction));
				calcTime(() -> Q17(transaction));
				//Q19(transaction);
				//Q21(transaction);
				//Q22(transaction);
				
				//Falta 9, 15, 16, 18, 20
				
				transaction.commit();
				System.out.println("\n <... Finish TPCH Benchmark ...>");
				((BufferManager)Kernel.getBufferManager()).closeLog();
				System.exit(0);
			}
			
			@Override
			public void onFail(ITransaction transaction, Exception e) {
				System.out.println(e.getMessage());
				transaction.abort();
				System.exit(0);
			}
		},false,false);
		
	}
	
	
/*	
	select
	l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc, count(*) as count_order
	from
	lineitem
	where
	l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
	group by
	l_returnflag,
	l_linestatus
	order by
	l_returnflag,
	l_linestatus;
*/

public static void Q1(ITransaction transaction) {
			
	//		String sql = "Select * from lineitem order by l_linestatus";

						String sql = " SELECT l.l_returnflag, l.l_linestatus, sum(l.l_quantity) , sum(l.l_extendedprice) , " +
			" sum(l.l_discount) , sum(l.l_tax), " +
			" avg(l.l_quantity), avg(l.l_extendedprice), " +
			" avg(l.l_discount), count(l.l_extendedprice) " +
			" FROM lineitem l " +
			" WHERE " +
			" l.l_shipdate >= '1900' and " +
			" l.l_shipdate < '1998' " +
			" GROUP BY l.l_returnflag, l.l_linestatus " +
			" ORDER BY l.l_returnflag, l.l_linestatus ";
			
			try {
				Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpch"));
				plan.setTransaction(transaction);
				System.out.printf("Q1 Running...");
				//showResult(transaction, plan.execute());
				plan.execute();
				System.out.println(" <-- Finish");
			} catch (SQLException e) {
				e.printStackTrace();
			}
			 
			
		}
	
	
public static void Q2(ITransaction transaction) {
		

		String subSelectSQL = " SELECT min(ps_supplycost) FROM part p, supplier s, partsupp ps, nation n, region r  "
				+ " WHERE "
				+ " p.partkey = ps.partkey and  " 
				+ " s.suppkey = ps.suppkey and  " 
				+ " s.nationkey = n.nationkey and  "
				+ " n.regionkey = r.regionkey and  " 
				+ " r.r_name = 'EUROPE'  ";

		
		try {
			Plan planMinSupplycost = new Parse().parseSQL(subSelectSQL, Kernel.getCatalog().getSchemabyName("tpch"));
			planMinSupplycost.setTransaction(transaction);
			System.out.printf("Q2 Running...");
			
			String resultValueMinSupplycost = getFirstResult(transaction, planMinSupplycost.execute());
			
			String sql = " SELECT s_acctbal, s_name, n_name, p.partkey, p_mfgr, s_address, s_phone, s_comment "
					+ " FROM part p, supplier s, partsupp ps, nation n, region r  "
					+ " WHERE 	 p.partkey = ps.partkey and  " 
					+ "  s.suppkey = ps.suppkey and  "
					+ "  s.nationkey = n.nationkey and  " 
					+ "  n.regionkey = r.regionkey and  "
					+ "  r.r_name = 'EUROPE' and " 
					+ "  p.p_size = 20  and " 
					+ "  p.p_type > 'L' and "
					+ "  ps.ps_supplycost = " + resultValueMinSupplycost 
					+ " ORDER BY "
					+ " s_acctbal, s_name, n_name, p.partkey desc ";
			
			Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpch"));
			plan.setTransaction(transaction);
			
			//showResult(transaction, plan.execute());
			plan.execute();
			
			System.out.println(" <-- Finish");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
		
/*
select
l_orderkey,
sum(l_extendedprice*(1-l_discount)) as revenue,
o_orderdate,
o_shippriority
from
customer,
orders,
lineitem
where
c_mktsegment = '[SEGMENT]'
and c_custkey = o_custkey
and l_orderkey = o_orderkey
and o_orderdate < date '[DATE]'
and l_shipdate > date '[DATE]'
group by
l_orderkey,
o_orderdate,
o_shippriority
order by
revenue desc,
o_orderdate;
*/

public static void Q3(ITransaction transaction) {
		
		
		String sql =  " SELECT l.orderkey, sum(l.l_extendedprice), o.o_orderdate, o.o_shippriority "+
				" FROM customer c, order o, lineitem l "+
				" WHERE "+
				" c.c_mktsegment = 'AUTOMOBILE' "+
				" and c.custkey = o.custkey "+
				" and l.orderkey = o.orderkey "+
				" and l.l_shipdate >= '1998' and "+
				" l.l_shipdate < '1990' "+
				" GROUP BY l.orderkey, o.o_orderdate, o.o_shippriority "+ 
				" ORDER BY l.l_extendedprice desc, o.o_orderdate";
		
		try {
			Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpch"));
			plan.setTransaction(transaction);
			System.out.printf("Q3 Running...");
			plan.execute();
			System.out.println(" <-- Finish");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		 
		
	}

		
	/*
	select 
	o_orderpriority,  count(o_orderpriority)
	from  
	orders 
	where  
	o_orderdate >= date '[DATE]' and 
	o_orderdate < date '[DATE]' + interval '3' month and 
	exists ( select  * from  lineitem where  l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by  o_orderpriority order by  o_orderpriority)
  group by  o_orderpriority order by  o_orderpriority; 	

*/	

public static void Q4(ITransaction transaction) {

		Plan plan = new Plan(transaction);
		
		//from orders 
		TableOperation orders = newTable(plan, "order");

//		where  o_orderdate >= date '[DATE]' and o_orderdate < date '[DATE]' + interval '3' month and 
		SelectionOperation selectionOrders = newSelection(plan, 
				new Condition("o_orderdate",">=","'1900'"),
				new Condition("o_orderdate","<","'1994'"));
		selectionOrders.setLeft(orders);

		//and exists ( select  * from  lineitem where  l_orderkey = o_orderkey and l_commitdate < l_receiptdate )
		TableOperation lineitem = newTable(plan, "lineitem");
		FilterOperation filterLineitem = newFilterOperation(plan, new Condition("l_commitdate", "<", "l_receiptdate","0","0","and",Condition.COLUMN_COLUMN));
		filterLineitem.setLeft(lineitem);
		
		FilterOperation filterCorrelation = newFilterOperation(plan, new Condition("orderkey", "==", "orderkey","0","1","and",Condition.CORRELATION_EXISTS));
		GroupResultsOperation group = newGroupResultsOperation(plan, selectionOrders, filterLineitem);
		filterCorrelation.setLeft(group);
		
		
		//select o_orderpriority,  count(o_orderpriority)
		// group by o_orderpriority order by o_orderpriority;
		AggregationOperation agg = newAggregation(plan,
				new Condition("o_orderpriority", AggregationOperation.GROUPING, null),
				new Condition("o_orderpriority", AggregationOperation.COUNT, null));
		SortOperation sort = newSort(plan, true, "o_orderpriority");
		agg.setLeft(sort);
		sort.setLeft(filterCorrelation);
		
		
		
		plan.addOperation(agg);
		System.out.printf("Q4 Running...");
		//showResult(transaction, plan.execute());
		plan.execute();
		System.out.println(" <-- Finish");

	}
	
	/*
	SELECT c.custkey, c.c_name, c.c_acctbal,  c.c_phone, n.n_name, c.c_address, c.c_comment
	FROM customer c, order o, lineitem l, nation n
	WHERE
	c.nationkey = n.nationkey and
	c.custkey = o.custkey and 
	o.orderkey = l.orderkey and
	o.o_orderdate >= '1900' and 
	o.o_orderdate < '1994' and 
	l_returnflag = 'R' 
	ORDER BY c.custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment
	*/

public static void Q5(ITransaction transaction) {
		
		
		String sql = " SELECT c.custkey, c.c_name, c.c_acctbal,  c.c_phone, n.n_name, c.c_address, c.c_comment " +
		" FROM customer c, order o, lineitem l, nation n " +
		" WHERE " +
		" c.nationkey = n.nationkey and " +
		" c.custkey = o.custkey and " + 
		" o.orderkey = l.orderkey and " +
		" o.o_orderdate >= '1900' and " +
		" o.o_orderdate < '1994' and " +
		" l_returnflag = 'R' " +
		" ORDER BY c.custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment ";
		
		try {
			Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpch"));
			plan.setTransaction(transaction);
			System.out.printf("Q5 Running...");
			plan.execute();
			System.out.println(" <-- Finish");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		 
		
	}
	

/*
	select
	sum(l_extendedprice*l_discount) as revenue
	from
	lineitem
	where
	l_shipdate >= date '[DATE]'
	and l_shipdate < date '[DATE]' + interval '1' year
	and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
	and l_quantity < [QUANTITY];

*/	

public static void Q6(ITransaction transaction) {
			
			
			String sql = " SELECT sum(l.l_extendedprice), sum(l.l_discount) "+
					" FROM lineitem l  "+
					" WHERE  "+
					" l.l_shipdate >= '1900' and  "+
					" l.l_shipdate < '1994' and  "+
					" l.l_discount >= 0.01 and "+
					" l.l_discount < 0.02 and "+
					" l.l_quantity < 5 ";
					
			try {
				Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpch"));
				plan.setTransaction(transaction);
				System.out.printf("Q6 Running...");
				plan.execute();
				System.out.println(" <-- Finish");
			} catch (SQLException e) {
				e.printStackTrace();
			}
			 
			
		}
	
	
	
	/*
	Query 10 - OK

	select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
	from
	customer,
	orders,
	lineitem,
	nation
	where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '[DATE]'
	and o_orderdate < date '[DATE]' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
	group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
	order by
	revenue desc;

*/
	
	
public static void Q10(ITransaction transaction) {
			
			
			String sql = " SELECT sum(l.l_extendedprice), sum(l.l_discount)  "+
			 " FROM customer c, order o, lineitem l, nation n  "+
			 " WHERE "+
			 " c.nationkey = n.nationkey and "+
			 " c.custkey = o.custkey  and  "+
			 " l.orderkey = o.orderkey and "+
			 " o.o_orderdate >= '1900' and  "+
			 " o.o_orderdate < '1994' and  "+
			 "  l.l_returnflag = 'R'  "+
			 " GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment "+
			 " ORDER BY l.l_extendedprice ";
			 
			
			try {
				Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpch"));
				plan.setTransaction(transaction);
				System.out.printf("Q10 Running...");
				plan.execute();
				System.out.println(" <-- Finish");
			} catch (SQLException e) {
				e.printStackTrace();
			}
			 
			
		}
	
	
	
	/*
	
	Query 11 - OK

	select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
	from
	partsupp,
	supplier,
	nation
	where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = '[NATION]'
	group by
	ps_partkey having
	sum(ps_supplycost * ps_availqty) > (
	select
	sum(ps_supplycost * ps_availqty) * [FRACTION]
	from
	partsupp,
	supplier,
	nation
	where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = '[NATION]'
	)
	order by
	value desc;
*/

public static void Q11(ITransaction transaction) {
			

			String subSelectSQL = "SELECT sum(ps.ps_supplycost)  "+
					" FROM partsupp ps, supplier s, nation n  "+ 
					"  WHERE ps.suppkey = s.suppkey AND  "+
					" 	s.nationkey = n.nationkey AND "+
					" 	n.n_name = 'BRAZIL' ";
			
			try {
				Plan planMinSupplycost = new Parse().parseSQL(subSelectSQL, Kernel.getCatalog().getSchemabyName("tpch"));
				planMinSupplycost.setTransaction(transaction);
				System.out.printf("Q11 Running...");
				
				String resultValueMinSupplycost = getFirstResult(transaction, planMinSupplycost.execute());
				
			//	System.out.println(" >>>> " + resultValueMinSupplycost);
				
				String sql = " SELECT ps.partkey, sum(ps_supplycost), sum(ps_availqty) "+
							" FROM partsupp ps, supplier s, nation n "+
						"  WHERE ps.suppkey = s.suppkey AND "+
						"  s.nationkey = n.nationkey AND "+
						"  n.n_name = 'BRAZIL' "+
						"  GROUP BY ps.partkey "; 
					
				
				Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpch"));
				plan.setTransaction(transaction);
				
				
				//" HAVING sum(ps_supplycost) > " + resultValueMinSupplycost +
				SelectionOperation having = newSelection(plan, new Condition("ps_supplycost", ">", resultValueMinSupplycost));
				
				//" ORDER BY sum(ps.ps_supplycost) ";
				SortOperation sort = newSort(plan, true, "ps_supplycost");
				
				plan.addOperation(plan.getRoot(),having);
				plan.addOperation(plan.getRoot(),sort);
						
			//	showResult(transaction, plan.execute());
				plan.execute();
				
				System.out.println(" <-- Finish");
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}

	
	/*
	
	select
	l_shipmode,
	sum(case
	when o_orderpriority ='1-URGENT'
	or o_orderpriority ='2-HIGH'
	then 1
	else 0
	end) as high_line_count,
	sum(case
	when o_orderpriority <> '1-URGENT'
	and o_orderpriority <> '2-HIGH'
	then 1
	else 0
	end) as low_line_count
	from
	orders,
	lineitem
	where
	o_orderkey = l_orderkey
	and l_shipmode in ('[SHIPMODE1]', '[SHIPMODE2]')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '[DATE]'
	and l_receiptdate < date '[DATE]' + interval '1' year
	group by
	l_shipmode
	order by
	l_shipmode;

	*/

public static void Q12(ITransaction transaction) {
			
			
			String sql =  " SELECT l.l_shipmode, sum(o.orderkey), sum(o.orderkey) "+
					" FROM order o, lineitem l  "+
					" WHERE  "+
					" o.orderkey = l.orderkey  "+
					" and l.l_shipmode = 'TRUCK'  "+
					" and l.l_commitdate < l.l_receiptdate  "+
					" and l.l_shipdate < l.l_commitdate  "+
					" and l.l_receiptdate >= '1900'  "+
					" and l_receiptdate < '1994'  "+
					" GROUP BY l.l_shipmode  "+
					" ORDER BY l.l_shipmode  ";
			
			try {
				Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpch"));
				plan.setTransaction(transaction);
				System.out.printf("Q12 Running...");
				plan.execute();
				System.out.println(" <-- Finish");
			} catch (SQLException e) {
				e.printStackTrace();
			}
			 
			
		}
	
	
	/*
	select
	c_count, count(*) as custdist
	from (
	select
	c_custkey,
	count(o_orderkey)
	from
	customer left outer join orders on
	c_custkey = o_custkey
	and o_comment not like ‘%[WORD1]%[WORD2]%’
	group by
	c_custkey
	)as c_orders (c_custkey, c_count)
	group by
	c_count
	order by
	custdist desc,
	c_count desc;

	*/
	
public static void Q13(ITransaction transaction) {
		
		Plan plan = new Plan(transaction);

		TableOperation orders = newTable(plan, "order");
		
		TableOperation customer = newTable(plan, "customer");

		SelectionOperation selectionOrders = newSelection(plan, 
				new Condition("o_comment","!=","null"));
		selectionOrders.setLeft(orders);


		JoinOperation join = newJoin(plan, new Condition("custkey","==","custkey")); //left outer join
		join.setLeft(selectionOrders);
		join.setRight(customer);
		
		SortOperation sort = newSort(plan, true, "custkey");
		sort.setLeft(join);
		
		AggregationOperation agg = newAggregation(plan,
				new Condition("custkey", AggregationOperation.GROUPING, null),
				new Condition("orderkey", AggregationOperation.COUNT, null));
		agg.setLeft(sort);

		
		
		AggregationOperation agg2 = newAggregation(plan,
				new Condition("orderkey", AggregationOperation.GROUPING, null),
				new Condition("custkey", AggregationOperation.COUNT, null));
		agg2.setLeft(agg);
		
		
		SortOperation sort2 = newSort(plan, true, "orderkey");
		sort2.setLeft(agg2);
		

		plan.addOperation(sort2);
		System.out.printf("Q13 Running...");
		//showResult(transaction, plan.execute());
		plan.execute();
	
		System.out.println(" <-- Finish");
	
	}
	

	/*
	
	Query 14 - OK

	select
	100.00 * sum(case
	when p_type like 'PROMO%'
	then l_extendedprice*(1-l_discount)
	else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
	from
	lineitem,
	part
	where
	l_partkey = p_partkey
	and l_shipdate >= date '[DATE]'
	and l_shipdate < date '[DATE]' + interval '1' month;

	 */
	
	
public static void Q14(ITransaction transaction) {
			
			
			String sql = " SELECT sum(l.l_discount) " +
			" FROM lineitem l, part p " +
			" WHERE " +
			" l.partkey = p_partkey" +
			" and l.l_shipdate >= '1900' " +
			" and l_shipdate < '1994'";
			
			try {
				Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpch"));
				plan.setTransaction(transaction);
				System.out.printf("Q14 Running...");
				plan.execute();
				//showResult(transaction, plan.execute());
				System.out.println(" <-- Finish");
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}

	
	
	/*
	Select
	sum(l_extendedprice) / 7.0 as avg_yearly
	from
	lineitem,
	part
	where
	p_partkey = l_partkey
	and p_brand = '[BRAND]'
	and p_container = '[CONTAINER]'
	and l_quantity < (
	select
	0.2 * avg(l_quantity)
	from
	lineitem
	where
	l_partkey = p_partkey
	);
	*/


public static void Q17(ITransaction transaction){
		Plan plan = new Plan(transaction);
		
		TableOperation lineitem = newTable(plan, "lineitem");
		TableOperation part = newTable(plan, "part");


		SelectionOperation selectionPart = newSelection(plan, 
				new Condition("p_container","==","'MED PKG'"),
				new Condition("p_brand","==","'Brand#52'"));
		selectionPart.setLeft(part);

		
		
		JoinOperation join = newJoin(plan, new Condition("partkey","==","partkey"));
		join.setLeft(selectionPart);
		join.setRight(lineitem);

		FilterOperation filterCorrelation = newFilterOperation(plan, new Condition("partkey", "==", "partkey","0","1","and",Condition.CORRELATION_EXISTS),															 new Condition("l_quantity", "<", "l_quantity","0","1","and",Condition.CORRELATION_EXISTS));
		GroupResultsOperation group = newGroupResultsOperation(plan, join, lineitem);
		filterCorrelation.setLeft(group);
		
		ProjectionOperation projectionOperation = newProjection(plan, "l_extendedprice");
		projectionOperation.setLeft(filterCorrelation);
		
		plan.addOperation(projectionOperation );
		System.out.printf("Q7 Running...");
		//showResult(transaction, plan.execute());
		plan.execute();
		System.out.println(" <-- Finish");
	}
	
	
public static String getFirstResult(ITransaction transaction, ITable result) {
		
		return new TableScan(transaction, result).nextTuple().getStringData();
		
	}
	
	
public static void showResult(ITransaction transaction, ITable result) {
		System.out.println("\n---------------------------------------- " + result.getName() + "---------------------------------------- ");
		System.out.println(Arrays.toString(result.getColumnNames()));
		int count = 0;
		TableScan tr2 = new TableScan(transaction, result);
		ITuple tuple = tr2.nextTuple();
		while(tuple!=null){
			System.out.println(Arrays.toString(tuple.getData()));
			tuple = tr2.nextTuple();
			count++;
		}
		System.out.println("\n--"+count+" tuples -----------------------------------------------------------------");
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
	
	public static JoinOperation newJoin(Plan plan, Condition... conditions ) {
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
	
	public static GroupResultsOperation newGroupResultsOperation(Plan plan, AbstractPlanOperation result0, AbstractPlanOperation result1) {		
		GroupResultsOperation groupResultsOP = new GroupResultsOperation();
		groupResultsOP.setLeft(result0);
		groupResultsOP.setRight(result1);
		groupResultsOP.setPlan(plan);
		return groupResultsOP;
	}
	
	public static void calcTime(Runnable r) {
		long lStartTime = System.nanoTime();
		r.run();
		long lEndTime = System.nanoTime();
		long output = lEndTime - lStartTime;
	    System.out.println("QueryTime: " + output / 1000000 + " ms");
	}
	
}
