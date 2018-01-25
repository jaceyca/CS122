package edu.caltech.nanodb.queryeval;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.plannodes.*;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;

import edu.caltech.nanodb.expressions.Expression;

import edu.caltech.nanodb.relations.TableInfo;


/**
 * This class generates execution plans for very simple SQL
 * <tt>SELECT * FROM tbl [WHERE P]</tt> queries.  The primary responsibility
 * is to generate plans for SQL <tt>SELECT</tt> statements, but
 * <tt>UPDATE</tt> and <tt>DELETE</tt> expressions will also use this class
 * to generate simple plans to identify the tuples to update or delete.
 */
public class SimplePlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SimplePlanner.class);


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
                             List<SelectClause> enclosingSelects) throws IOException {

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Not implemented:  enclosing queries");
        }

        PlanNode plan;
        FromClause fromClause = selClause.getFromClause();

        if (!selClause.isTrivialProject()) {
//            throw new UnsupportedOperationException(
//                    "Not implemented:  project");
            // Here, we support the situations where there is no child plan,
            // and no expression references a column name
            if (fromClause == null) {
                plan = new ProjectNode(selClause.getSelectValues());
                plan.prepare();
                return plan;
            }
        }

        // First, we complete the FROM clause so we have something to work with. This is the birth of a miracle
        // This will support basic joins (not NATURAL joins or joins with USING). Left and right outer joins
        // will be supported as well (no full-outer joins) and subqueries in the FROM clause.
        plan = completeFromClause(fromClause, selClause);

        // Now we will support grouping and aggregation. This will support multiple aggregate operations
        // in a SELECT expression.
        Map<String, FunctionCall> temp = new HashMap<>();
        HashedGroupAggregateNode groupAggregateNode = new HashedGroupAggregateNode(plan, selClause.getGroupByExprs()), temp);


        return plan;
    }

    public PlanNode completeFromClause(FromClause fromClause, SelectClause selClause) throws IOException {
        PlanNode fromPlan = null;
        if (fromClause.isBaseTable()) {
            // If we have this case, then our behavior is as before. Simple! Skiddle dee doo!
            fromPlan = makeSimpleSelect(fromClause.getTableName(), selClause.getWhereExpr(), null);
        } // Now we need to handle subqueries
        else if (fromClause.isDerivedTable()){
            // If we have this case, then we have to evaluate what's inside the select query first.
            // We can do this by simply recursively calling our makePlan function on that sub-query.
            fromPlan = makePlan(fromClause.getSelectClause(), null);
        }
        else if (fromClause.isJoinExpr()) {
            // In the joins, it is possible that the left and right clauses are derived tables or also joins.
            // So, we recursively call completeFromClause to complete the set up of those from clauses.
            FromClause leftFromClause = fromClause.getLeftChild();
            FromClause rightFromClause = fromClause.getRightChild();
            PlanNode leftChild = completeFromClause(leftFromClause, selClause);
            PlanNode rightChild = completeFromClause(rightFromClause, selClause);
            fromPlan = new NestedLoopJoinNode(leftChild, rightChild,
                    fromClause.getJoinType(), fromClause.getComputedJoinExpr());
        }
        return fromPlan;
    }

    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
                                       List<SelectClause> enclosingSelects) throws IOException {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                    "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.prepare();
        return selectNode;
    }
}

