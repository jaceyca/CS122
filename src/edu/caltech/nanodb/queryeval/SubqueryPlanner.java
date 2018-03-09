package edu.caltech.nanodb.queryeval;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionProcessor;

public class SubqueryPlanner implements ExpressionProcessor {
    public void enter(Expression node) {

    }

    public Expression leave(Expression node) {
        return null;
    }
}
