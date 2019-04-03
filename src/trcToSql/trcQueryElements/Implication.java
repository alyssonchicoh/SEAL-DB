package trcToSql.trcQueryElements;
import trcToSql.visitors.*;

public class Implication extends Formula{
	public Formula f1;
	public Formula f2;
	public Implication(Formula f1, Formula f2){
		this.f1 = f1;
		this.f2 = f2;
	}

	public void accept(Visitor v){
		v.visit(this);
	}


	public String accept(VisitorString v){
		return v.visit(this);
	}
	
	public Formula accept(VisitorFormula v){
		return v.visit(this);
	}
}