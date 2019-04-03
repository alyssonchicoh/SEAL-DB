package trcToSql.trcQueryElements;
import trcToSql.visitors.*;
public abstract class Formula{
	public abstract void accept(Visitor v);
	public abstract Formula accept(VisitorFormula v);
	public abstract String accept(VisitorString v);
}