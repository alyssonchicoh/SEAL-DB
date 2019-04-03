package trcToSql.trcQueryElements;
import trcToSql.visitors.*;

public class Constant{
	public String c;
	public Constant(String c){
		this.c = c;
	}

	
	public String accept(VisitorString v){
		return v.visit(this);
	}

	public void accept(Visitor v){
		v.visit(this);
	}
	
}