package tests;
import java.io.ByteArrayInputStream;

import trcToSql.trcGrammar.*;
import trcToSql.trcQueryElements.*;
import trcToSql.visitors.*;

public class TestTrcToSQL{

	public static void main(String[] args) throws Exception{
		//TrcGrammar parser = new TrcGrammar(System.in);
		TrcGrammar parser = new TrcGrammar(new ByteArrayInputStream("{x.a | R(x)}".getBytes()));
 		VisitorToString v = new VisitorToString();
 		
 		Query p = parser.query();

		p.accept(v);		
 		System.out.println("\n" + v.stringResult + "\n"); 
		
		p.accept(new VisitorSQLNF());
		p.accept(v);
		System.out.println("\n" + v.stringResult + "\n"); 

		p.accept(new VisitorScope());
//		DbManager dbManager = DbManager.getInstance();
//		
//		VisitorToSQL vSql = new VisitorToSQL(dbManager.getDbSchema("Teste"));
//		String s = p.accept(vSql);
//		System.out.println("\n" + s + "\n"); 
//
//		System.out.println(vSql.getErrorLog().getFormulaErrors());
//		System.out.println(vSql.getErrorLog().getScopeErrors());

	}
	
}