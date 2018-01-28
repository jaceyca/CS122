package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.plannodes.RenameNode;

import java.util.*;

public class Aggregate implements ExpressionProcessor {

    public List<FunctionCall> aggregateFunctions = new ArrayList<>();
    public List<FunctionCall> rememberFunctionCalls = new ArrayList<>();
    public int numAggregates = -1; // We use this for renaming of our columns
    public Set<String> rememberColNames = new HashSet<>();
    public Map<FunctionCall, String> mapFunctionToString = new HashMap<>();

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
                if (!rememberFunctionCalls.contains(call)) {
                    numAggregates += 1; // Keep this value so we can create column values to correspond to our map
                    String colNameString = "#A" + Integer.toString(numAggregates+1);
                    mapFunctionToString.put(call, colNameString);
                    rememberFunctionCalls.add(call);
                    aggregateFunctions.add(call);
                }
            }
        }
    }
    // If we want to change the node that is traversed, we must do it here.
    public Expression leave(Expression node) {
        if (node instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) node;
            Function f = call.getFunction();
            // If i already see something that is in my set, I want to rename that node
            // to the corresponding A# value so it can evaluate that easily again.
            if (f instanceof AggregateFunction) {
                // Make sure this is how to use ColumnValue!!!! who dat boy?? idk
                String colNameString = "#A" + Integer.toString(numAggregates+1);
                if (!rememberColNames.contains(colNameString)) {
                    rememberColNames.add(colNameString);
                    ColumnName colName = new ColumnName(colNameString);
                    node = new ColumnValue(colName);
                    aggregateFunctions.remove(aggregateFunctions.size() - 1);
                } else if (mapFunctionToString.containsKey(call)){
                    // If this is true, then we have seen the functionCall "call" before. In this case,
                    // we want to reuse the columnName we have stored for it using our amazing
                    // mapFuncitonToString map.
                    System.out.println("colNameStr: " + colNameString);
                    String name = mapFunctionToString.get(call);
                    System.out.println("name: " + name);
                    ColumnName colName = new ColumnName(name);
                    node = new ColumnValue(colName);
                }
            }
        }
        return node;
    }

    public Map<String, FunctionCall> prepareMap() {
        Map<String, FunctionCall> columnReferenceMap = new HashMap<>();
        for (int i = 0; i < rememberFunctionCalls.size(); i++) {
            columnReferenceMap.put("#A" + Integer.toString(i+1), rememberFunctionCalls.get(i));
        }
        return columnReferenceMap;
    }
}
