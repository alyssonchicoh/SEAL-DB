package trcToSql.trcQueryElements;
import trcToSql.visitors.*;

public class InnerFormula extends Formula{
	public Formula f;
	public InnerFormula(Formula f){
		this.f = f;
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