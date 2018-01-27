package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


/**
 * This class performs some basic tests with INNER joins, LEFT-OUTER joins,
 * and RIGHT-OUTER joins.
 * These tests aren't exhaustive; they serve as a smoke-test to verify the
 * basic behaviors.
 */
@Test
public class TestSimpleJoins extends SqlTestCase {
    public TestSimpleJoins() {
        super("setup_testSimpleJoins");
    }

    /**
     * This test checks that at least one value was successfully inserted into
     * each of the test tables.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleTablesNotEmpty() throws Throwable {
        testTableNotEmpty("test_sj_t1");
        testTableNotEmpty("test_sj_t2");
        testTableNotEmpty("test_sj_t3");
        testTableNotEmpty("test_sj_t4");
    }

    /**
     * This test performs inner joins, left-outer joins, and right-outer joins
     * with two tables, verifying both the schema and the data that is returned.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinTwoTables() throws Throwable {
        CommandResult result;

        // INNER JOIN with only one common column:  A
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 JOIN test_sj_t3 t3 ON t1.a = t3.a", true);
        TupleLiteral[] expected1 = {
                new TupleLiteral(2, 20, 2, 200, 2000),
                new TupleLiteral(3, 30, 3, 300, 3000),
                new TupleLiteral(5, 50, 5, 500, 5000),
                new TupleLiteral(6, 60, 6, 600, 6000),
                new TupleLiteral(8, 80, 8, 800, 8000)
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T1.A", "T1.B", "T3.A", "T3.C", "T3.D");

        // LEFT-OUTER JOIN with only one common column:  A
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 LEFT JOIN test_sj_t3 t3 ON t1.a = t3.a", true);
        TupleLiteral[] expected2 = {
                new TupleLiteral(1, 10, null, null, null),
                new TupleLiteral(2, 20, 2, 200, 2000),
                new TupleLiteral(3, 30, 3, 300, 3000),
                new TupleLiteral(4, 40, null, null, null),
                new TupleLiteral(5, 50, 5, 500, 5000),
                new TupleLiteral(6, 60, 6, 600, 6000),
                new TupleLiteral(7, 70, null, null, null),
                new TupleLiteral(8, 80, 8, 800, 8000)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "T1.A", "T1.B", "T3.A", "T3.C", "T3.D");

        // RIGHT-OUTER JOIN with only one common column:  A
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 RIGHT JOIN test_sj_t3 t3 ON t1.a = t3.a", true);
        TupleLiteral[] expected3 = {
                new TupleLiteral(null, null, 0, 0, 0),
                new TupleLiteral(2, 20, 2, 200, 2000),
                new TupleLiteral(3, 30, 3, 300, 3000),
                new TupleLiteral(5, 50, 5, 500, 5000),
                new TupleLiteral(6, 60, 6, 600, 6000),
                new TupleLiteral(8, 80, 8, 800, 8000),
                new TupleLiteral(null, null, 9, 900, 9000),
                new TupleLiteral(null, null, 11, 1100, 11000)

        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);
        checkResultSchema(result, "T1.A", "T1.B", "T3.A", "T3.C", "T3.D");
    }

    /**
     * This test performs inner joins, left-outer joins, and right-outer joins
     * with two tables, verifying both the schema and the data that is returned.
     * The left table is empty and the right table is not.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinLeftEmpty() throws Throwable {
        CommandResult result;

        // INNER JOIN with only one common column:  A
        // result should be empty
        result = server.doCommand(
                "SELECT * FROM test_sj_t5 t5 JOIN test_sj_t1 t1 ON t5.a = t1.a", true);
        TupleLiteral[] expected1 = {};
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T5.A", "T5.C", "T5.D", "T1.A", "T1.B");

        // LEFT-OUTER JOIN with only one common column:  A
        // result should be empty
        result = server.doCommand(
                "SELECT * FROM test_sj_t5 t5 LEFT JOIN test_sj_t1 t1 ON t5.a = t1.a", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T5.A", "T5.C", "T5.D", "T1.A", "T1.B");

        // RIGHT-OUTER JOIN with only one common column:  A
        // result should not be empty
        result = server.doCommand(
                "SELECT * FROM test_sj_t5 t5 RIGHT JOIN test_sj_t1 t1 ON t5.a = t1.a", true);
        TupleLiteral[] expected2 = {
                new TupleLiteral(null, null, null, 1, 10),
                new TupleLiteral(null, null, null, 2, 20),
                new TupleLiteral(null, null, null, 3, 30),
                new TupleLiteral(null, null, null, 4, 40),
                new TupleLiteral(null, null, null, 5, 50),
                new TupleLiteral(null, null, null, 6, 60),
                new TupleLiteral(null, null, null, 7, 70),
                new TupleLiteral(null, null, null, 8, 80)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "T5.A", "T5.C", "T5.D", "T1.A", "T1.B");
    }

    /**
     * This test performs inner joins, left-outer joins, and right-outer joins
     * with two tables, verifying both the schema and the data that is returned.
     * The right table is empty and the left table is not.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinRightEmpty() throws Throwable {
        CommandResult result;

        // INNER JOIN with only one common column:  A
        // result should be empty
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 JOIN test_sj_t5 t5 ON t1.a = t5.a", true);
        TupleLiteral[] expected1 = {};
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T1.A", "T1.B", "T5.A", "T5.C", "T5.D");

        // RIGHT-OUTER JOIN with only one common column:  A
        // result should be empty
        result = server.doCommand(
                "SELECT * FROM test_sj_t5 t5 RIGHT JOIN test_sj_t1 t1 ON t5.a = t1.a", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T1.A", "T1.B", "T5.A", "T5.C", "T5.D");

        // LEFT-OUTER JOIN with only one common column:  A
        // result should not be empty
        result = server.doCommand(
                "SELECT * FROM test_sj_t5 t5 LEFT JOIN test_sj_t1 t1 ON t5.a = t1.a", true);
        TupleLiteral[] expected2 = {
                new TupleLiteral(1, 10, null, null, null),
                new TupleLiteral(2, 20, null, null, null),
                new TupleLiteral(3, 30, null, null, null),
                new TupleLiteral(4, 40, null, null, null),
                new TupleLiteral(5, 50, null, null, null),
                new TupleLiteral(6, 60, null, null, null),
                new TupleLiteral(7, 70, null, null, null),
                new TupleLiteral(8, 80, null, null, null)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "T1.A", "T1.B", "T5.A", "T5.C", "T5.D");
    }

    /**
     * This test performs inner joins, left-outer joins, and right-outer joins
     * with two tables, verifying both the schema and the data that is returned.
     * Both the right and left tables are empty.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinBothEmpty() throws Throwable {
        CommandResult result;

        // INNER JOIN with only one common column:  A
        // result should be empty
        result = server.doCommand(
                "SELECT * FROM test_sj_t5 t5 JOIN test_sj_t6 t6 ON t5.a = t6.a", true);
        TupleLiteral[] expected1 = {};
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T5.A", "T5.C", "T5.D", "T6.A", "T6.E");

        // RIGHT-OUTER JOIN with only one common column:  A
        // result should be empty
        result = server.doCommand(
                "SELECT * FROM test_sj_t5 t5 RIGHT JOIN test_sj_t6 t6 ON t5.a = t6.a", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T5.A", "T5.C", "T5.D", "T6.A", "T6.E");

        // LEFT-OUTER JOIN with only one common column:  A
        // result should be empty
        result = server.doCommand(
                "SELECT * FROM test_sj_t5 t5 LEFT JOIN test_sj_t6 t6 ON t5.a = t6.a", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T5.A", "T5.C", "T5.D", "T6.A", "T6.E");
    }


    /**
     * This test performs inner, left-outer, and right-outer joins
     * with three or more tables, verifying both the schema and the data
     * that is returned.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinMultiTables() throws Throwable {
        CommandResult result;

        // INNER JOIN three tables with a common column name A.
        // T1 and T4 are joined first, and then joined to the third table T3.
        result = server.doCommand(
                "SELECT * FROM (test_sj_t1 t1 JOIN test_sj_t4 t4 ON t1.a = t4.a) JOIN test_sj_t3 t3 ON t1.a = t3.a", true);
        TupleLiteral[] expected1 = {
                new TupleLiteral(2, 20, 2, 200, 2, 200, 2000),
                new TupleLiteral(5, 50, 5, 600, 5, 500, 5000),
                new TupleLiteral(6, 60, 6, 500, 6, 600, 6000)
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T1.A", "T1.B", "T4.A", "T4.C", "T3.A", "T3.C", "T3.D");

        // Making sure that the results are the same when the 2nd join uses t4.a instead of t1.a
        result = server.doCommand(
                "SELECT * FROM (test_sj_t1 t1 JOIN test_sj_t4 t4 ON t1.a = t4.a) JOIN test_sj_t3 t3 ON t4.a = t3.a", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T1.A", "T1.B", "T4.A", "T4.C", "T3.A", "T3.C", "T3.D");

        // INNER and RIGHT-OUTER JOIN three tables with a common column name A.
        // T1 and T3 are joined first, and then right joined to the third table T4.
        result = server.doCommand(
                "SELECT * FROM test_sj_t4 t4 RIGHT JOIN (test_sj_t1 t1 JOIN test_sj_t3 t3 ON t1.a = t3.a) ON t4.a = t1.a", true);
        TupleLiteral[] expected2 = {
                new TupleLiteral(2, 200, 2, 20, 2, 200, 2000),
                new TupleLiteral(null, null, 3, 30, 3, 300, 3000),
                new TupleLiteral(5, 600, 5, 50, 5, 500, 5000),
                new TupleLiteral(6, 500, 6, 60, 6, 600, 6000),
                new TupleLiteral(null, null, 8, 80, 8, 800, 8000)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "T4.A", "T4.C", "T1.A", "T1.B", "T3.A", "T3.C", "T3.D");
    }

    /**
     * This test performs inner joins, left-outer joins, and right-outer joins
     * with two tables, verifying both the schema and the data that is returned.
     * A row in one table joins with several rows in the other table, and vice versa.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinMultiRows() throws Throwable {
        CommandResult result;

        // INNER JOIN with two common columns:  A, B
        // Using both columns A and B in the predicate
        result = server.doCommand(
                "SELECT * FROM test_sj_t2 t2 JOIN test_sj_t7 t7 ON t2.a = t7.a AND t2.b = t7.b", true);
        TupleLiteral[] expected1 = {
                new TupleLiteral(3, 33, 300, 3, 33),
                new TupleLiteral(4, 30, 400, 4, 30)
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T2.A", "T2.B", "T2.C", "T7.A", "T7.B");

        // LEFT JOIN with two common columns:  A, B
        // Because A = 3 appears twice in t2 and t7, we should have 4 rows for A = 3
        // Because A = 4 appears twice in t7, we should have 2 rows for A = 4
        result = server.doCommand(
                "SELECT * FROM test_sj_t2 t2 LEFT JOIN test_sj_t7 t7 ON t2.a = t7.a", true);
        TupleLiteral[] expected2 = {
                new TupleLiteral(3, 33, 300, 3, 30),
                new TupleLiteral(3, 33, 300, 3, 33),
                new TupleLiteral(3, 40, 300, 3, 30),
                new TupleLiteral(3, 40, 300, 3, 33),
                new TupleLiteral(4, 30, 400, 4, 40),
                new TupleLiteral(4, 30, 400, 4, 44),
                new TupleLiteral(5, 60, 500, null, null),
                new TupleLiteral(6, 50, 600, 6, 60),
                new TupleLiteral(7, 70, 700, 7, 70),
                new TupleLiteral(8, 80, 800, 8, 80),
                new TupleLiteral(9, 100, 900, null, null),
                new TupleLiteral(10, 90, 1000, null, null)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "T2.A", "T2.B", "T2.C", "T7.A", "T7.B");

        // RIGHT JOIN with two common columns:  A, B
        // Because A = 3 appears twice in t2 and t7, we should have 4 rows for A = 3
        // Because A = 4 appears twice in t7, we should have 2 rows for A = 4
        result = server.doCommand(
                "SELECT * FROM test_sj_t2 t2 RIGHT JOIN test_sj_t7 t7 ON t2.a = t7.a", true);
        TupleLiteral[] expected3 = {
                new TupleLiteral(null, null, null, 1, 10),
                new TupleLiteral(null, null, null, 2, 20),
                new TupleLiteral(3, 33, 300, 3, 30),
                new TupleLiteral(3, 33, 300, 3, 33),
                new TupleLiteral(3, 40, 300, 3, 30),
                new TupleLiteral(3, 40, 300, 3, 33),
                new TupleLiteral(4, 30, 400, 4, 40),
                new TupleLiteral(4, 30, 400, 4, 44),
                new TupleLiteral(6, 50, 600, 6, 60),
                new TupleLiteral(7, 70, 700, 7, 70),
                new TupleLiteral(8, 80, 800, 8, 80)
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);
        checkResultSchema(result, "T2.A", "T2.B", "T2.C", "T7.A", "T7.B");
    }
}
