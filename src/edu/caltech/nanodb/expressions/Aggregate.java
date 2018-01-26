package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Aggregate implements ExpressionProcessor {

    public List<FunctionCall> aggregateFunctions = new ArrayList<>();
    public List<FunctionCall> aggregateFunctionsStorage = new ArrayList<>();
    public int numAggregates = -1; // We use this for renaming of our columns

    public void enter(Expression node) {
        if (node instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) node;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                if (!aggregateFunctions.isEmpty()) {
                    // If the one of the previous nodes contained an aggregate function, then this
                    // function that was called by the previous node cannot be an aggregate function.
                    throw new IllegalArgumentException("Aggregate functions cannot contain " +
                            "other function calls in their arguments.");
                }
                // Before we enter into the next child node (if it exists), we must add the call to
                // our growing list. We can check if this is empty to see if there have been
                // any aggregate calls, which is useful for raising errors.
                aggregateFunctions.add(call);
                aggregateFunctionsStorage.add(call);
                numAggregates += 1; // Keep this value so we can create column values to correspond to our map
            }
        }
    }
    // If we want to change the node that is traversed, we must do it here.
    public Expression leave(Expression node) {
        if (node instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) node;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                // Make sure this is how to use ColumnValue!!!! who dat boy?? idk
                ColumnName colName = new ColumnName("#A" + Integer.toString(numAggregates+1));
                node = new ColumnValue(colName);
//                System.out.println("currPos: " + currPos);
//                System.out.println("List size: " + aggregateFunctions.size());
                aggregateFunctions.remove(aggregateFunctions.size()-1);
//                System.out.println("New list size: " + aggregateFunctions.size());
//                numAggregates -= 1; // Decrement counter as we re-visit previous nodes
            }
        }
        return node;
    }

    public Map<String, FunctionCall> prepareMap() {
        Map<String, FunctionCall> columnReferenceMap = new HashMap<>();
        for (int i = 0; i < aggregateFunctionsStorage.size(); i++) {
            columnReferenceMap.put("#A" + Integer.toString(i+1), aggregateFunctionsStorage.get(i));
        }
        return columnReferenceMap;
    }
}
