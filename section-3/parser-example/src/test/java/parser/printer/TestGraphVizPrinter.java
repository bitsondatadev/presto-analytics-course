/*
 * $Id: $
 * $Revision: $
 * $Author: $
 * $Date: $
 * Copyright (c) 2020 Trustwave Holdings, Inc.
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information
 * of Trustwave Holdings, Inc.  Use of this software is governed by
 * the terms and conditions of the license statement and limited
 * warranty furnished with the software.
 *
 * IN PARTICULAR, YOU WILL INDEMNIFY AND HOLD TRUSTWAVE HOLDINGS INC.,
 * ITS RELATED COMPANIES AND ITS SUPPLIERS, HARMLESS FROM AND AGAINST
 * ANY CLAIMS OR LIABILITIES ARISING OUT OF OR RESULTING FROM THE USE,
 * MODIFICATION, OR DISTRIBUTION OF PROGRAMS OR FILES CREATED FROM,
 * BASED ON, AND/OR DERIVED FROM THIS SOURCE CODE FILE.
 */
package parser.printer;

import com.google.common.collect.Maps;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Node;
import org.junit.Test;

public class TestGraphVizPrinter
{
    private static final Boolean PRINT = true;
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final ParsingOptions PARSING_OPTIONS = new ParsingOptions(
            ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE);

    @Test
    public void testSimpleQuery(){
        String sql = "select a, b, c from a.b.c";
        Node node = generateAST(sql);
        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }
    @Test
    public void testCreate(){
        String sql = "CREATE TABLE orders (orderkey bigint,orderstatus varchar,totalprice double, orderdate date) WITH (format = 'ORC')";
        Node node = generateAST(sql);
        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }

    @Test
    public void testWhereQuery(){
        String sql = "select a, b from a.b.c where a > b AND b = 24 order by a,b";
        Node node = generateAST(sql);
        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }

    @Test
    public void testWhereLikeQuery(){
        String sql = "select a, b from a.b.c where a like 'abc*' AND b = 24 order by a,b";
        Node node = generateAST(sql);
        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }

    @Test
    public void testOrderByQuery(){
        String sql = "select a, b from a.b.c where a > b AND b = 24 order by a,b";
        Node node = generateAST(sql);
        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }

    @Test
    public void testGroupByQuery(){
        String sql = "select a, b, sum(*) from a.b.c where a > b AND b = 24 group by a,b";
        Node node = generateAST(sql);
        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }

    @Test
    public void testGroupByCubeQuery(){
        String sql = "select a, b, sum(*) from a.b.c where a > b AND b = 24 group by CUBE(a,b)";
        Node node = generateAST(sql);
        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }

    @Test
    public void testGroupByRollupQuery(){
        String sql = "select a, b, sum(*) from a.b.c where a > b AND b = 24 group by ROLLUP(a,b)";
        Node node = generateAST(sql);
        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }

    @Test
    public void testGroupByGroupingSetsQuery(){
        String sql = "select a, b, sum(*) from a.b.c where a > b AND b = 24 group by GROUPING SETS ((a,b),(a),(b),())";
        Node node = generateAST(sql);
        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }

    @Test
    public void testHavingQuery(){
        String sql = "select a, b, sum(*) from a.b.c where a > b AND b = 24 group by a,b having sum(*) > 50";
        Node node = generateAST(sql);
        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }

    @Test
    public void testOverlappingLiteralsAndIdentifiers(){
        String sql = "select a, b, c from some.tab.abc cross join some.tab.abc where a > b and b = 24 OR a like '%24' order by a,b";
        Node node = generateAST(sql);
        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }

    @Test
    public void testAliasAndTargetSelectAllRow(){
        String sql = "SELECT (CAST(ROW(1, true) AS ROW(field1 bigint, field2 boolean))).* AS (alias1, alias2)";
        Node node = generateAST(sql);

        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }


    @Test
    public void testAliasAndTargetSelectAllRelation(){
        String sql = "SELECT test.* AS (alias1, alias2)";
        Node node = generateAST(sql);
        GraphVizPrinter printer = new GraphVizPrinter(Maps.newIdentityHashMap(), System.out);

        if(PRINT){
            printer.print(node, sql);
        } else {
            GraphVizPrinter.generateGraphViz(node, sql);
        }
    }


    private Node generateAST(String sql) {
        return SQL_PARSER.createStatement(sql, PARSING_OPTIONS);
    }

}
