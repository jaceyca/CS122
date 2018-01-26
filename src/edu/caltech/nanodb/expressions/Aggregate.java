package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Aggregate implements ExpressionProcessor {

    public List<FunctionCall> aggregateFunctions = new ArrayList<>();
    public int currPos = -1;

    public void enter(Expression node) {
        if (node instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) node;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction && !aggregateFunctions.isEmpty()) {
                // If the one of the previous nodes contained an aggregate function, then this
                // function that was called by the previous node cannot be an aggregate function.
                throw new IllegalArgumentException("Aggregate functions cannot contain " +
                        "other function calls in their arguments.");
            }

            // Before we enter into the next child node (if it exists), we must add the call to
            // our growing list. We can check if this is empty to see if there have been
            // any aggregate calls, which is useful for raising errors.
            aggregateFunctions.add(call);
            currPos += 1; // Keep this value so we can create column values to correspond to our map
        }
    }
    // If we want to change the node that is traversed, we must do it here.
    public Expression leave(Expression node) {
        if (node instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) node;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                // Do stuff
                // Make sure this is how to use ColumnValue!!!! who dat boy?? idk
                ColumnName colName = new ColumnName("#A" + Integer.toString(currPos+1));
                ColumnValue colVal = new ColumnValue(colName);
                node = colVal;
                currPos -= 1; // Decrement counter as we re-visit previous nodes
            }
        }
        return node;
    }

    public Map<String, FunctionCall> prepareMap() {
        Map<String, FunctionCall> columnReferenceMap = new HashMap<>();
        for (int i = 0; i < aggregateFunctions.size(); i++) {
            columnReferenceMap.put("#A" + Integer.toString(i+1), aggregateFunctions.get(i));
        }
        return columnReferenceMap;
    }
}
