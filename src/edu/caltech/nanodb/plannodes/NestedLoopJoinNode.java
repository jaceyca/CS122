package edu.caltech.nanodb.plannodes;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.TupleLiteral;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.queryeval.ColumnStats;
import edu.caltech.nanodb.queryeval.PlanCost;
import edu.caltech.nanodb.queryeval.SelectivityEstimator;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Tuple;

import java.util.ArrayList;


/**
 * This plan node implements a nested-loop join operation, which can support
 * arbitrary join conditions but is also the slowest join implementation.
 */
public class NestedLoopJoinNode extends ThetaJoinNode {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(NestedLoopJoinNode.class);


    /** Most recently retrieved tuple of the left relation. */
    private Tuple leftTuple;

    /** Most recently retrieved tuple of the right relation. */
    private Tuple rightTuple;

    private Tuple prevLeftTuple;

    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;

    private boolean matchFound;

    private boolean isLeftOuter;
    private boolean isRightOuter;

    public NestedLoopJoinNode(PlanNode leftChild, PlanNode rightChild,
                JoinType joinType, Expression predicate) {

        super(leftChild, rightChild, joinType, predicate);
        if (joinType == JoinType.RIGHT_OUTER) {
            super.swap();
        }
    }


    /**
     * Checks if the argument is a plan node tree with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof NestedLoopJoinNode) {
            NestedLoopJoinNode other = (NestedLoopJoinNode) obj;

            return predicate.equals(other.predicate) &&
                leftChild.equals(other.leftChild) &&
                rightChild.equals(other.rightChild);
        }

        return false;
    }


    /** Computes the hash-code of the nested-loop plan node. */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /**
     * Returns a string representing this nested-loop join's vital information.
     *
     * @return a string representing this plan-node.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("NestedLoop[");

        if (predicate != null)
            buf.append("pred:  ").append(predicate);
        else
            buf.append("no pred");

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /**
     * Creates a copy of this plan node and its subtrees.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        NestedLoopJoinNode node = (NestedLoopJoinNode) super.clone();

        // Clone the predicate.
        if (predicate != null)
            node.predicate = predicate.duplicate();
        else
            node.predicate = null;

        return node;
    }


    /**
     * Nested-loop joins can conceivably produce sorted results in situations
     * where the outer relation is ordered, but we will keep it simple and just
     * report that the results are not ordered.
     */
    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** True if the node supports position marking. **/
    public boolean supportsMarking() {
        return leftChild.supportsMarking() && rightChild.supportsMarking();
    }


    /** True if the node requires that its left child supports marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** True if the node requires that its right child supports marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    @Override
    public void prepare() {
        // Need to prepare the left and right child-nodes before we can do
        // our own work.
        leftChild.prepare();
        rightChild.prepare();

        // Use the parent class' helper-function to prepare the schema.
        prepareSchemaStats();

        float selectivity = SelectivityEstimator.estimateSelectivity(predicate, schema, stats);
        float tuples = 0;
        if(joinType == JoinType.INNER || joinType == joinType.LEFT_OUTER)
        {
            tuples = selectivity * leftChild.cost.numTuples * rightChild.cost.numTuples;
        }
        else if(joinType == JoinType.LEFT_OUTER)
        {
            tuples += (1 - selectivity) * leftChild.cost.numTuples;
        }
        else if(joinType == JoinType.RIGHT_OUTER)
        {
            tuples += (1 - selectivity) * rightChild.cost.numTuples;
        }

        cost = new PlanCost(tuples, leftChild.cost.tupleSize + rightChild.cost.tupleSize,
            leftChild.cost.cpuCost + rightChild.cost.cpuCost + (leftChild.cost.numTuples * rightChild.cost.numTuples),
            rightChild.cost.numBlockIOs + leftChild.cost.numBlockIOs);
    }


    public void initialize() {
        super.initialize();

        if (joinType == JoinType.LEFT_OUTER) {
            isLeftOuter = true;
        } else if (joinType == JoinType.RIGHT_OUTER) {
            isRightOuter = true;
        }
        if (isRightOuter) {
//            swap();
        }
        matchFound = false;
        done = false;
        leftTuple = null;
        rightTuple = null;
        prevLeftTuple = null;
    }


    /**
     * Returns the next joined tuple that satisfies the join condition.
     *
     * @return the next joined tuple that satisfies the join condition.
     *
     * @throws IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IOException {
        if (done) {
            return null;
        }
        while (getTuplesToJoin()) {
            if (!done) {
                if (canJoinTuples()) {
                    return joinTuples(leftTuple, rightTuple);
                }
                else if (!matchFound && isLeftOuter) {
                    return joinTuples(prevLeftTuple, new TupleLiteral(rightSchema.numColumns()));
                }
                else if (!matchFound && isRightOuter) {
                    return joinTuples(new TupleLiteral(rightSchema.numColumns()), prevLeftTuple);
                }
            }
        }

        return null;
    }


    /**
     * This helper function implements the logic that sets {@link #leftTuple}
     * and {@link #rightTuple} based on the nested-loop logic.
     *
     * @return {@code true} if another pair of tuples was found to join, or
     *         {@code false} if no more pairs of tuples are available to join.
     */
    private boolean getTuplesToJoin() throws IOException {
        if (done)
            return false;

        // starting the outer loop
        if (leftTuple == null) {
            incrementRight();

            if (leftTuple == null) {
                done = true;
                return false;
            }
//            rightTuple = rightChild.getNextTuple();
        }
        else {
            incrementRight();
        }

        // iterate through the left table (outer relation)
        while (leftTuple != null) {
            if (rightTuple != null) {

                if (canJoinTuples()) {
                    matchFound = true;
//                    incrementRight();
                    return true;
                }
                incrementRight();
            }
            else if (isLeftOuter && !matchFound) {
                prevLeftTuple = leftTuple;
                incrementRight();
                return true;
            }
            else if (isRightOuter && !matchFound) {
                prevLeftTuple = leftTuple;
                incrementRight();
                return true;
            }
            // if we have reached the end of the inner relation, we need to reset
            // the tuple back to the start
            else {
                incrementRight();
            }
        }
        done = true;
        return false;
    }

    private void incrementRight() throws IOException {
        if (rightTuple == null) {
            rightChild.initialize();
            leftTuple = leftChild.getNextTuple();
            rightTuple = rightChild.getNextTuple();
            matchFound = false;
            if (leftTuple == null) {
                done = true;
//                return false;
            }
        }
        else {
                rightTuple = rightChild.getNextTuple();
        }
//        return true;
    }


    private boolean canJoinTuples() {
        // If the predicate was not set, we can always join them!
        if (predicate == null)
            return true;

        environment.clear();
        environment.addTuple(leftSchema, leftTuple);
        environment.addTuple(rightSchema, rightTuple);

        return predicate.evaluatePredicate(environment);
    }


    public void markCurrentPosition() {
        leftChild.markCurrentPosition();
        rightChild.markCurrentPosition();
    }


    public void resetToLastMark() throws IllegalStateException {
        leftChild.resetToLastMark();
        rightChild.resetToLastMark();

        // TODO:  Prepare to reevaluate the join operation for the tuples.
        //        (Just haven't gotten around to implementing this.)
    }


    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }
}
