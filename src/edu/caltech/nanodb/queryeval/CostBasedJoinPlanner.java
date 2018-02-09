package edu.caltech.nanodb.queryeval;


import java.io.IOException;

import java.util.*;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.SelectValue;
import edu.caltech.nanodb.relations.Schema;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;

import edu.caltech.nanodb.relations.JoinType;
import sun.java2d.pipe.SpanShapeRenderer;

import javax.sql.rowset.Predicate;


/**
 * This planner implementation uses dynamic programming to devise an optimal
 * join strategy for the query.  As always, queries are optimized in units of
 * <tt>SELECT</tt>-<tt>FROM</tt>-<tt>WHERE</tt> subqueries; optimizations
 * don't currently span multiple subqueries.
 */
public class CostBasedJoinPlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CostBasedJoinPlanner.class);


    /**
     * This helper class is used to keep track of one "join component" in the
     * dynamic programming algorithm.  A join component is simply a query plan
     * for joining one or more leaves of the query.
     * <p>
     * In this context, a "leaf" may either be a base table or a subquery in
     * the <tt>FROM</tt>-clause of the query.  However, the planner will
     * attempt to push conjuncts down the plan as far as possible, so even if
     * a leaf is a base table, the plan may be a bit more complex than just a
     * single file-scan.
     */
    private static class JoinComponent {
        /**
         * This is the join plan itself, that joins together all leaves
         * specified in the {@link #leavesUsed} field.
         */
        public PlanNode joinPlan;

        /**
         * This field specifies the collection of leaf-plans that are joined by
         * the plan in this join-component.
         */
        public HashSet<PlanNode> leavesUsed;

        /**
         * This field specifies the collection of all conjuncts use by this join
         * plan.  It allows us to easily determine what join conjuncts still
         * remain to be incorporated into the query.
         */
        public HashSet<Expression> conjunctsUsed;

        /**
         * Constructs a new instance for a <em>leaf node</em>.  It should not
         * be used for join-plans that join together two or more leaves.  This
         * constructor simply adds the leaf-plan into the {@link #leavesUsed}
         * collection.
         *
         * @param leafPlan the query plan for this leaf of the query.
         *
         * @param conjunctsUsed the set of conjuncts used by the leaf plan.
         *        This may be an empty set if no conjuncts apply solely to
         *        this leaf, or it may be nonempty if some conjuncts apply
         *        solely to this leaf.
         */
        public JoinComponent(PlanNode leafPlan, HashSet<Expression> conjunctsUsed) {
            leavesUsed = new HashSet<>();
            leavesUsed.add(leafPlan);

            joinPlan = leafPlan;

            this.conjunctsUsed = conjunctsUsed;
        }

        /**
         * Constructs a new instance for a <em>non-leaf node</em>.  It should
         * not be used for leaf plans!
         *
         * @param joinPlan the query plan that joins together all leaves
         *        specified in the <tt>leavesUsed</tt> argument.
         *
         * @param leavesUsed the set of two or more leaf plans that are joined
         *        together by the join plan.
         *
         * @param conjunctsUsed the set of conjuncts used by the join plan.
         *        Obviously, it is expected that all conjuncts specified here
         *        can actually be evaluated against the join plan.
         */
        public JoinComponent(PlanNode joinPlan, HashSet<PlanNode> leavesUsed,
                             HashSet<Expression> conjunctsUsed) {
            this.joinPlan = joinPlan;
            this.leavesUsed = leavesUsed;
            this.conjunctsUsed = conjunctsUsed;
        }
    }

    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        // This is a very rough sketch of how this function will work,
        // focusing mainly on join planning:
        //
        // 1)  Pull out the top-level conjuncts from the WHERE and HAVING
        //     clauses on the query, since we will handle them in special ways
        //     if we have outer joins.
        //
        // 2)  Create an optimal join plan from the top-level from-clause and
        //     the top-level conjuncts.
        //
        // 3)  If there are any unused conjuncts, determine how to handle them.
        //
        // 4)  Create a project plan-node if necessary.
        //
        // 5)  Handle other clauses such as ORDER BY, LIMIT/OFFSET, etc.
        //
        // Supporting other query features, such as grouping/aggregation,
        // various kinds of subqueries, queries without a FROM clause, etc.,
        // can all be incorporated into this sketch relatively easily.

        // NOTES: We will proceed similarly as the makePlan() method from SimplePlanner
        // However, we will not proceed similarly for the following reasons...
        // We must store conjuncts from WHERE and HAVING before we do anything,
        // but we also note that we must not push down aggregate functions from the
        // HAVING clause because those cannot be pushed down. So, we need to find a way
        // to store the aggregates that are aggregates (maybe use InstanceOf Aggregate)
        // and we keep those conjuncts. Those conjuncts will then be used where we normally
        // take care of HAVING clause as in our SimplePlanner.
        // In the instructions above, it tells us to handle unused conjuncts. These unused
        // conjuncts are most likely the left over aggregates from the HAVING expression
        // that we will just handle like we did in SimplePlanner after we do the
        // makeJoinPlan. All the other conjuncts we
        // get from the having, we can add to the makeJoinPlan. Also, all the WHERE
        // conjuncts can be put in the makeJoinPlan because WHERE expressions
        // should not have any aggregates. We also modify the completeFromClause function
        // because we only use that if the fromClause is not a Join Expression. If it is,
        // then we just use makeJoinPlan.

        // More OH notes: We need to collect conjuncts from HAVING and we don't really have
        // to check for aggregates that we don't have to pass down because they should be
        // renamed if we did that correctly. So, after that, there should be unused conjuncts
        // which we will have to handle after we makeJoinPlan().
        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Not implemented:  enclosing queries");
        }

        PlanNode plan;
        FromClause fromClause = selClause.getFromClause();
//        System.out.println("makePlan1");

        // Here, we support the situations where there is no child plan,
        // and no expression references a column name
        if (fromClause == null) {
            plan = new ProjectNode(selClause.getSelectValues());
            plan.prepare();
            return plan;
        }

        Aggregate processor = new Aggregate();
        List<SelectValue> selectValues = selClause.getSelectValues();
        List<Expression> groupByExpressions = selClause.getGroupByExprs();

        for (SelectValue sv : selectValues) {
            // Skip select-values that aren't expressions
            if (!sv.isExpression())
                continue;
            Expression e = sv.getExpression().traverse(processor);
            sv.setExpression(e);
        }

        HashSet<Expression> conjuncts = new HashSet<>();
        // Now we will support grouping and aggregation. This will support multiple aggregate operations
        // in a SELECT expression. We also extract conjuncts from the WHERE expression
        Expression whereExpression = selClause.getWhereExpr();
        // Where clauses cannot have aggregates so we check for that here
        if (whereExpression != null) {
            whereExpression.traverse(processor);
            PredicateUtils.collectConjuncts(whereExpression, conjuncts);
            if (!processor.aggregateFunctions.isEmpty())
                throw new IllegalArgumentException("WHERE clauses cannot have aggregates");
        }


        // At this point, we have collected all the conjuncts (except having???)
        // Would there any be any unused conjuncts from WHERE clause? Probably not.

        if (fromClause.isJoinExpr()) {
            JoinComponent joinComponent = makeJoinPlan(fromClause, conjuncts);
            // Now we must remove all the conjuncts that makeJoinPlan used
            conjuncts.removeAll(joinComponent.conjunctsUsed);
            plan = joinComponent.joinPlan;
//            plan = completeFromClause(fromClause, selClause, processor);
        } else {
            // We complete the FROM clause so we have something to work with. This is the birth of a miracle
            // This will support basic joins (not NATURAL joins or joins with USING). Left and right outer joins
            // will be supported as well (no full-outer joins) and subqueries in the FROM clause. Also, at this
            // point, we know we have a FROM clause, so we don't have to check that in completeFromClause.
            plan = completeFromClause(fromClause, selClause, processor);
        }

        Map<String, FunctionCall> columnReferenceMap = processor.prepareMap();

        if (!groupByExpressions.isEmpty() || !columnReferenceMap.isEmpty())
            plan = new HashedGroupAggregateNode(plan, groupByExpressions, columnReferenceMap);

        // Next, we handle HAVING expressions (if one exists) here
        Expression havingExpression = selClause.getHavingExpr();
        if (havingExpression != null) {
            havingExpression.traverse(processor);
            selClause.setHavingExpr(havingExpression);
            plan = new SimpleFilterNode(plan, havingExpression);
//            PredicateUtils.collectConjuncts(havingExpression, conjuncts);
        }

        // Now, we will use the rest of the conjuncts that makeJoinPlan didn't use
        // We should have used all of them, which is why the following line probably
        // makes everything fail?? There are still some conjuncts left over
        // from only collecting from WHERE clause. I don't think this makes sense.
//        if (!conjuncts.isEmpty())
//            plan = new SimpleFilterNode(plan, PredicateUtils.makePredicate(conjuncts));

                // Here, we support the situations where there is a child plan, and we
        // have to project the select values specified by the select clause.
        plan = new ProjectNode(plan, selectValues);

        // Now we will support ORDER BY clauses
        List<OrderByExpression> orderBy = selClause.getOrderByExprs();
        if (!orderBy.isEmpty())
            plan = new SortNode(plan, orderBy);

        plan.prepare();
        return plan;
    }
    public PlanNode completeFromClause(FromClause fromClause, SelectClause selClause,
                                       Aggregate processor) throws IOException {
        PlanNode fromPlan = null;
        if (fromClause.isBaseTable()) {
            // If we have this case, then our behavior is as before. Simple!
            fromPlan = makeSimpleSelect(fromClause.getTableName(), selClause.getWhereExpr(), null);
        } // Now we need to handle subqueries
        else if (fromClause.isDerivedTable()){
            // If we have this case, then we have to evaluate what's inside the select query first.
            // We can do this by simply recursively calling our makePlan function on that sub-query.
            fromPlan = makePlan(fromClause.getSelectClause(), null);
        }
        else if (fromClause.isJoinExpr()) { // This should never get used
            // If we have an ON clause, then we need to use our Aggregate class to check
            // if that ON clause has an aggregate. It should not have one.
//            System.out.println("completeFromClause.isJoin");
            Expression onExpression = fromClause.getOnExpression();
            if (onExpression != null) {
//                System.out.println("completeFromClause.hasOnExpression");
                onExpression.traverse(processor);
                if (!processor.aggregateFunctions.isEmpty())
                    throw new IllegalArgumentException("ON clauses cannot have aggregates");
            }

            // In the joins, it is possible that the left and right clauses are derived tables or also joins.
            // So, we recursively call completeFromClause to complete the set up of those from clauses.
            FromClause leftFromClause = fromClause.getLeftChild();
            FromClause rightFromClause = fromClause.getRightChild();
            PlanNode leftChild = completeFromClause(leftFromClause, selClause, processor);
            PlanNode rightChild = completeFromClause(rightFromClause, selClause, processor);
//            System.out.println("completeFromClause.newNestedLoopJoinNode");
            fromPlan = new NestedLoopJoinNode(leftChild, rightChild,
                    fromClause.getJoinType(), fromClause.getOnExpression());
        }

        // Now, we need to check if our from clause has been renamed. This could happen after we select from
        // a derived table, for example.
        if (fromClause.isRenamed())
            fromPlan = new RenameNode(fromPlan, fromClause.getResultName());
        fromPlan.prepare();
        return fromPlan;
    }
//    public PlanNode completeFromClause(FromClause fromClause, SelectClause selClause,
//                                       Aggregate processor) throws IOException {
//        PlanNode fromPlan = null;
//        if (fromClause.isBaseTable()) {
//            // If we have this case, then our behavior is as before. Simple!
//            fromPlan = makeSimpleSelect(fromClause.getTableName(), selClause.getWhereExpr(), null);
//        } // Now we need to handle subqueries
//        else if (fromClause.isDerivedTable()){
//            // If we have this case, then we have to evaluate what's inside the select query first.
//            // We can do this by simply recursively calling our makePlan function on that sub-query.
//            fromPlan = makePlan(fromClause.getSelectClause(), null);
//        }
//
//        // We don't need to check if the table is a join table now because we would have done that
//        // using makeJoinPlan
//
//        // Now, we need to check if our from clause has been renamed. This could happen after we select from
//        // a derived table, for example.
//        if (fromClause.isRenamed())
//            fromPlan = new RenameNode(fromPlan, fromClause.getResultName());
//        fromPlan.prepare();
//        return fromPlan;
//    }

    /**
     * Given the top-level {@code FromClause} for a SELECT-FROM-WHERE block,
     * this helper generates an optimal join plan for the {@code FromClause}.
     *
     * @param fromClause the top-level {@code FromClause} of a
     *        SELECT-FROM-WHERE block.
     * @param extraConjuncts any extra conjuncts (e.g. from the WHERE clause,
     *        or HAVING clause)
     * @return a {@code JoinComponent} object that represents the optimal plan
     *         corresponding to the FROM-clause
     * @throws IOException if an IO error occurs during planning.
     */
    private JoinComponent makeJoinPlan(FromClause fromClause,
        Collection<Expression> extraConjuncts) throws IOException {

        // These variables receive the leaf-clauses and join conjuncts found
        // from scanning the sub-clauses.  Initially, we put the extra conjuncts
        // into the collection of conjuncts.
        HashSet<Expression> conjuncts = new HashSet<>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<>();

        collectDetails(fromClause, conjuncts, leafFromClauses);

        logger.debug("Making join-plan for " + fromClause);
        logger.debug("    Collected conjuncts:  " + conjuncts);
        logger.debug("    Collected FROM-clauses:  " + leafFromClauses);
        logger.debug("    Extra conjuncts:  " + extraConjuncts);

        if (extraConjuncts != null)
            conjuncts.addAll(extraConjuncts);

        // Make a read-only set of the input conjuncts, to avoid bugs due to
        // unintended side-effects.
        Set<Expression> roConjuncts = Collections.unmodifiableSet(conjuncts);

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        logger.debug("Generating plans for all leaves");
        ArrayList<JoinComponent> leafComponents = generateLeafJoinComponents(
            leafFromClauses, roConjuncts);

        // Print out the results, for debugging purposes.
        if (logger.isDebugEnabled()) {
            for (JoinComponent leaf : leafComponents) {
                logger.debug("    Leaf plan:\n" +
                    PlanNode.printNodeTreeToString(leaf.joinPlan, true));
            }
        }

        // Build up the full query-plan using a dynamic programming approach.

        JoinComponent optimalJoin =
            generateOptimalJoin(leafComponents, roConjuncts);

        PlanNode plan = optimalJoin.joinPlan;
        logger.info("Optimal join plan generated:\n" +
            PlanNode.printNodeTreeToString(plan, true));

        return optimalJoin;
    }


    /**
     * This helper method pulls the essential details for join optimization
     * out of a <tt>FROM</tt> clause. 
     *
     * This method considers base-tables,
     * subqueries, and outer-joins to be leaves. If the fromClause is a leaf,
     * then we add it to the list leafFromClauses. If fromClause is not a
     * leaf, only then can we collect conjuncts. We must also recursively
     * check the left and right children of fromClause (if it is a join
     * expression) because those children may be leaves, or they may
     * be more join expressions that we must collect conjuncts from as well.
     *
     * @param fromClause the from-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(FromClause fromClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses) {
        if (fromClause.isBaseTable() || fromClause.isDerivedTable() || 
            (fromClause.isOuterJoin() && fromClause.isJoinExpr()))
            leafFromClauses.add(fromClause);
        else {
            PredicateUtils.collectConjuncts(fromClause.getOnExpression(), conjuncts);
            PredicateUtils.collectConjuncts(fromClause.getComputedJoinExpr(), conjuncts);
            if(fromClause.getLeftChild() != null)
            {
                collectDetails(fromClause.getLeftChild(), conjuncts, leafFromClauses);
            }
            if(fromClause.getRightChild() != null)
            {
                collectDetails(fromClause.getRightChild(), conjuncts, leafFromClauses);
            }
        }
    }


    /**
     * This helper method performs the first step of the dynamic programming
     * process to generate an optimal join plan, by generating a plan for every
     * leaf from-clause identified from analyzing the query.  Leaf plans are
     * usually very simple; they are built either from base-tables or
     * <tt>SELECT</tt> subqueries.  The most complex detail is that any
     * conjuncts in the query that can be evaluated solely against a particular
     * leaf plan-node will be associated with the plan node.  <em>This is a
     * heuristic</em> that usually produces good plans (and certainly will for
     * the current state of the database), but could easily interfere with
     * indexes or other plan optimizations.
     *
     * @param leafFromClauses the collection of from-clauses found in the query
     *
     * @param conjuncts the collection of conjuncts that can be applied at this
     *                  level
     *
     * @return a collection of {@link JoinComponent} object containing the plans
     *         and other details for each leaf from-clause
     *
     * @throws IOException if a particular database table couldn't be opened or
     *         schema loaded, for some reason
     */
    private ArrayList<JoinComponent> generateLeafJoinComponents(
        Collection<FromClause> leafFromClauses, Collection<Expression> conjuncts)
        throws IOException {

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.
        ArrayList<JoinComponent> leafComponents = new ArrayList<>();
        for (FromClause leafClause : leafFromClauses) {
            HashSet<Expression> leafConjuncts = new HashSet<>();

            PlanNode leafPlan =
                makeLeafPlan(leafClause, conjuncts, leafConjuncts);

            JoinComponent leaf = new JoinComponent(leafPlan, leafConjuncts);
            leafComponents.add(leaf);
        }

        return leafComponents;
    }


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     * TODO:  COMPLETE THE DOCUMENTATION
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @param conjuncts additional conjuncts that can be applied when
     *        constructing the from-clause plan.
     *
     * @param leafConjuncts this is an output-parameter.  Any conjuncts
     *        applied in this plan from the <tt>conjuncts</tt> collection
     *        should be added to this out-param.
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     *
     * @throws IllegalArgumentException if the specified from-clause is a join
     *         expression that isn't an outer join, or has some other
     *         unrecognized type.
     */
    private PlanNode makeLeafPlan(FromClause fromClause,
        Collection<Expression> conjuncts, HashSet<Expression> leafConjuncts)
        throws IOException {

        // TODO:  IMPLEMENT.
        //        If you apply any conjuncts then make sure to add them to the
        //        leafConjuncts collection.
        //
        //        Don't forget that all from-clauses can specify an alias.
        //
        //        Concentrate on properly handling cases other than outer
        //        joins first, then focus on outer joins once you have the
        //        typical cases supported.

        PlanNode leafPlan = null;

        // We want to identify all "leaves" in the FROM expression. This includes
        // base-tables, subqueries, and outer-joins. We create an optimal plan for
        // each leaf identified, storing each optimal leaf plan, along with its cost.
        SelectClause selClause = fromClause.getSelectClause();
        if (fromClause.isBaseTable()) {
            // It would not be too expensive to call prepare() to get the conjuncts from a base table.
            leafPlan = makeSimpleSelect(fromClause.getTableName(), null, null);
            leafPlan.prepare();
            Schema baseTableSchema = leafPlan.getSchema();
//            PredicateUtils.collectConjuncts();
        }

        else if (fromClause.isDerivedTable()) {
            // It is probably too expensive and not worth it to call prepare() and collect
            // these conjuncts, so we leave this case as is
            leafPlan = makePlan(selClause, null);
        }
        else if (fromClause.isOuterJoin()) {
            // Here, we recognize that if we have a right outer join, then
            // we cannot pass on conjuncts that come from the left child
            // because the two resulting queries would not be equivalent.
            // Similarly, if we have a left outer join, then we cannot
            // pass on conjuncts that come from the right child. So, before
            // collecting conjuncts, we must check for these cases.
//            fromClause.prepare
            FromClause leftFromClause = fromClause.getLeftChild();
            FromClause rightFromClause = fromClause.getRightChild();

            Schema leftSchema = leftFromClause.getSchema();
            Schema rightSchema = rightFromClause.getSchema();
//            Schema leftSchema2 = leftFromClause.computeSchema(tableManager);
//            Schema rightSchema2 = rightFromClause.computeSchema();
            HashSet<Expression> rightConjuncts = null;
            HashSet<Expression> leftConjuncts = null;
            JoinComponent leftJoinComponent;
            JoinComponent rightJoinComponent;
            if (!fromClause.hasOuterJoinOnLeft()) {
                PredicateUtils.findExprsUsingSchemas(conjuncts, false,
                        leafConjuncts, rightSchema);
                rightConjuncts = leafConjuncts;
            } else if (!fromClause.hasOuterJoinOnRight()) {
                PredicateUtils.findExprsUsingSchemas(conjuncts, false,
                        leafConjuncts, leftSchema);
                leftConjuncts = leafConjuncts;
            }
            leftJoinComponent = makeJoinPlan(leftFromClause, leftConjuncts);
            rightJoinComponent = makeJoinPlan(rightFromClause, rightConjuncts);
            leafPlan = new NestedLoopJoinNode(leftJoinComponent.joinPlan,
                    rightJoinComponent.joinPlan, fromClause.getJoinType(),
                    fromClause.getComputedJoinExpr());
        }

        // If we need to rename node
        if (fromClause.isRenamed())
            leafPlan = new RenameNode(leafPlan, fromClause.getResultName());

        leafPlan.prepare();

        return leafPlan;
    }


    /**
     * This helper method builds up a full join-plan using a dynamic programming
     * approach.  The implementation maintains a collection of optimal
     * intermediate plans that join <em>n</em> of the leaf nodes, each with its
     * own associated cost, and then uses that collection to generate a new
     * collection of optimal intermediate plans that join <em>n+1</em> of the
     * leaf nodes.  This process completes when all leaf plans are joined
     * together; there will be <em>one</em> plan, and it will be the optimal
     * join plan (as far as our limited estimates can determine, anyway).
     *
     * @param leafComponents the collection of leaf join-components, generated
     *        by the {@link #generateLeafJoinComponents} method.
     *
     * @param conjuncts the collection of all conjuncts found in the query
     *
     * @return a single {@link JoinComponent} object that joins all leaf
     *         components together in an optimal way.
     */
    private JoinComponent generateOptimalJoin(
        ArrayList<JoinComponent> leafComponents, Set<Expression> conjuncts) {

        // This object maps a collection of leaf-plans (represented as a
        // hash-set) to the optimal join-plan for that collection of leaf plans.
        //
        // This collection starts out only containing the leaf plans themselves,
        // and on each iteration of the loop below, join-plans are grown by one
        // leaf.  For example:
        //   * In the first iteration, all plans joining 2 leaves are created.
        //   * In the second iteration, all plans joining 3 leaves are created.
        //   * etc.
        // At the end, the collection will contain ONE entry, which is the
        // optimal way to join all N leaves.  Go Go Gadget Dynamic Programming!
        HashMap<HashSet<PlanNode>, JoinComponent> joinPlans = new HashMap<>();

        // Initially populate joinPlans with just the N leaf plans.
        for (JoinComponent leaf : leafComponents)
            joinPlans.put(leaf.leavesUsed, leaf);

        while (joinPlans.size() > 1) {
            logger.debug("Current set of join-plans has " + joinPlans.size() +
                " plans in it.");

            // This is the set of "next plans" we will generate.  Plans only
            // get stored if they are the first plan that joins together the
            // specified leaves, or if they are better than the current plan.
            HashMap<HashSet<PlanNode>, JoinComponent> nextJoinPlans =
                new HashMap<>();

            for(JoinComponent plan : joinPlans.values())
            { 
                for(JoinComponent leaf : leafComponents)
                {
                    if(plan.leavesUsed.contains(leaf.joinPlan))
                    { //if the leaf is already in the plan
                        continue;
                    }

                    //getting the conjuncts
                    HashSet<Expression> usedconjuncts = new HashSet<Expression>();
                    PredicateUtils.findExprsUsingSchemas(conjuncts, false, usedconjuncts, plan.joinPlan.getSchema(), leaf.joinPlan.getSchema());
                    
                    //computing the set difference
                    usedconjuncts.removeAll(plan.conjunctsUsed);
                    usedconjuncts.removeAll(leaf.conjunctsUsed);

                    Expression predicate_p = PredicateUtils.makePredicate(usedconjuncts);

                    //starting up a new plan
                    PlanNode tempPlan = new NestedLoopJoinNode(plan.joinPlan, leaf.joinPlan, JoinType.INNER, predicate_p);
                    tempPlan.prepare();
                    PlanCost tempPlanCost = tempPlan.getCost();

                    //getting the leaves
                    HashSet<PlanNode> usedleafs = new HashSet<>();
                    usedleafs.addAll(plan.leavesUsed);
                    usedleafs.addAll(leaf.leavesUsed);

                    //computing the set union by adding in the conjuncts
                    usedconjuncts.addAll(plan.conjunctsUsed);
                    usedconjuncts.addAll(leaf.conjunctsUsed);

                    //dynamic programming part, storing the plan
                    //if we already have a plan that has all the leaves
                    JoinComponent tempJoin = new JoinComponent(tempPlan, usedleafs, usedconjuncts);
                    if(nextJoinPlans.containsKey(usedleafs))
                    {
                        PlanCost oldcost = nextJoinPlans.get(usedleafs).joinPlan.getCost();
                        if(tempPlanCost.cpuCost < oldcost.cpuCost)
                        { //replace the old plan with this new one
                            nextJoinPlans.put(usedleafs, tempJoin);
                        }
                    }
                    else
                    { //otherwise, adding the new plan to the map
                        nextJoinPlans.put(usedleafs, tempJoin);
                    }
                }
            }
            // Now that we have generated all plans joining N leaves, time to
            // create all plans joining N + 1 leaves.
            joinPlans = nextJoinPlans;
        }

        // At this point, the set of join plans should only contain one plan,
        // and it should be the optimal plan.

        assert joinPlans.size() == 1 : "There can be only one optimal join plan!";
        return joinPlans.values().iterator().next();
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
