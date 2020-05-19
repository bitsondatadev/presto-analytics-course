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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.prestosql.sql.tree.AddColumn;
import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.Analyze;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.ColumnDefinition;
import io.prestosql.sql.tree.Comment;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateTableAsSelect;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.Cube;
import io.prestosql.sql.tree.DefaultTraversalVisitor;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.DropColumn;
import io.prestosql.sql.tree.DropTable;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GroupBy;
import io.prestosql.sql.tree.GroupingElement;
import io.prestosql.sql.tree.GroupingSets;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.Join;
import io.prestosql.sql.tree.JoinOn;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.Limit;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.Property;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.RenameColumn;
import io.prestosql.sql.tree.RenameTable;
import io.prestosql.sql.tree.RenameView;
import io.prestosql.sql.tree.Rollup;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.SampledRelation;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.SimpleGroupBy;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SubqueryExpression;
import io.prestosql.sql.tree.Table;
import io.prestosql.sql.tree.TableElement;
import io.prestosql.sql.tree.TableSubquery;
import io.prestosql.sql.tree.Values;
import org.checkerframework.checker.units.qual.C;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.Maps.immutableEnumMap;
import static com.google.common.collect.Maps.newIdentityHashMap;
import static java.lang.String.format;

public class GraphVizPrinter
{
    private enum NodeType
    {
        QUERY,
        AGGREGATE,
        FILTER,
        PROJECT,
        TOPN,
        DEFAULT,
        LIMIT,
        TABLESCAN,
        VALUES,
        JOIN,
        SINK,
        WINDOW,
        UNION,
        SORT,
        SAMPLE,
        MARK_DISTINCT,
        TABLE_WRITER,
        TABLE_FINISH,
        INDEX_SOURCE,
        UNNEST,
        ANALYZE_FINISH,
    }

    private static final Map<NodeType, String> NODE_COLORS = immutableEnumMap(ImmutableMap.<NodeType, String>builder()
            .put(NodeType.QUERY, "gold")
            .put(NodeType.AGGREGATE, "chartreuse3")
            .put(NodeType.FILTER, "yellow")
            .put(NodeType.PROJECT, "bisque")
            .put(NodeType.TOPN, "darksalmon")
            .put(NodeType.DEFAULT, "white")
            .put(NodeType.LIMIT, "gray83")
            .put(NodeType.TABLESCAN, "deepskyblue")
            .put(NodeType.VALUES, "deepskyblue")
            .put(NodeType.JOIN, "orange")
            .put(NodeType.SORT, "aliceblue")
            .put(NodeType.SINK, "indianred1")
            .put(NodeType.WINDOW, "darkolivegreen4")
            .put(NodeType.UNION, "turquoise4")
            .put(NodeType.MARK_DISTINCT, "violet")
            .put(NodeType.TABLE_WRITER, "cyan")
            .put(NodeType.TABLE_FINISH, "hotpink")
            .put(NodeType.INDEX_SOURCE, "dodgerblue3")
            .put(NodeType.UNNEST, "crimson")
            .put(NodeType.SAMPLE, "goldenrod4")
            .put(NodeType.ANALYZE_FINISH, "plum")
            .build());

    static {
        Preconditions.checkState(NODE_COLORS.size() == NodeType.values().length);
    }

    private final IdentityHashMap<Expression, QualifiedName> resolvedNameReferences;
    private final PrintStream out;

    public GraphVizPrinter(PrintStream out)
    {
        this(Maps.newIdentityHashMap(), out);
    }

    public GraphVizPrinter(IdentityHashMap<Expression, QualifiedName> resolvedNameReferences, PrintStream out)
    {
        this.resolvedNameReferences = new IdentityHashMap<>(resolvedNameReferences);
        this.out = out;
    }

    public void print(Node root)
    {
        out.print(generateGraphViz(root));
    }

    public void print(Node root, String sql)
    {
        out.print(generateGraphViz(root, sql));
    }

    public static String generateGraphViz(Node root)
    {
        return generateGraphViz(root, null);
    }

    public static String generateGraphViz(Node root, String sql)
    {
        StringBuilder output = new StringBuilder();
        output.append("digraph ast {\n");

        if (sql != null) {
            output.append("graph [\n");
            output.append(" label = \"");
            output.append(sql);
            output.append("\"\n");
            output.append(" labelloc = t\n");
            output.append("]\n");
        }

        StringBuilder nodeOutput = new StringBuilder();
        StringBuilder edgeOutput = new StringBuilder();

        new GraphPrinterVisitor(nodeOutput, edgeOutput).process(root);
        output.append(nodeOutput);
        output.append(edgeOutput);
        output.append("}\n");

        return output.toString();
    }

    private static class GraphPrinterVisitor
            extends DefaultTraversalVisitor<Void, Void>
    {
        private static final int MAX_NAME_WIDTH = 100;
        private final StringBuilder nodeOutput;
        private final StringBuilder edgeOutput;
        private final NodeIdGenerator idGenerator;
        private final IdentityHashMap<Expression, QualifiedName> resolvedNameReferences;

        public GraphPrinterVisitor(StringBuilder nodeOutput, StringBuilder edgeOutput)
        {
            this(nodeOutput, edgeOutput, newIdentityHashMap());
        }

        public GraphPrinterVisitor(StringBuilder nodeOutput, StringBuilder edgeOutput, IdentityHashMap<Expression, QualifiedName> resolvedNameReferences)
        {
            this(nodeOutput, edgeOutput, resolvedNameReferences, new NodeIdGenerator());
        }

        public GraphPrinterVisitor(StringBuilder nodeOutput, StringBuilder edgeOutput, IdentityHashMap<Expression, QualifiedName> resolvedNameReferences, NodeIdGenerator idGenerator)
        {
            this.nodeOutput = nodeOutput;
            this.edgeOutput = edgeOutput;
            this.idGenerator = idGenerator;
            this.resolvedNameReferences = resolvedNameReferences;
        }

        @Override
        protected Void visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException(format("Node %s does not have a Graphviz visitor", node.getClass().getName()));
        }

        @Override
        protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            printNode(node, printLabel(node, node.getOperator().toString()), NODE_COLORS.get(NodeType.DEFAULT));
            if (node.getLeft() != null) {
                process(node.getLeft());
                printEdge(node, node.getLeft());
            }

            if (node.getRight() != null) {
                process(node.getRight());
                printEdge(node, node.getRight());
            }

            return null;
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, Void context)
        {
            printNode(node, printLabel(node, node.getOperator().toString()), NODE_COLORS.get(NodeType.DEFAULT));
            if (node.getLeft() != null) {
                process(node.getLeft());
                printEdge(node, node.getLeft());
            }

            if (node.getRight() != null) {
                process(node.getRight());
                printEdge(node, node.getRight());
            }

            return null;
        }

        @Override
        protected Void visitQuery(Query node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));
            process(node.getQueryBody());
            printEdge(node, node.getQueryBody());

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get());
                printEdge(node, node.getOrderBy().get());
            }

            if (node.getLimit().isPresent()) {
                process(node.getLimit().get());
                printEdge(node, node.getLimit().get());
            }

            return null;
        }

        @Override
        protected Void visitSelect(Select node, Void context)
        {
            if (node.isDistinct()) {
                printNode(node, printLabel(node, "DISTINCT"), NODE_COLORS.get(NodeType.DEFAULT));
            }
            else {
                printNode(node, NODE_COLORS.get(NodeType.DEFAULT));
            }

            for (SelectItem item : node.getSelectItems()) {
                process(item, context);
                printEdge(node, item);
            }

            return null;
        }

        @Override
        protected Void visitOrderBy(OrderBy node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            for (SortItem sortItem : node.getSortItems()) {
                process(sortItem);
                printEdge(node, sortItem);
            }

            return null;
        }

        @Override
        protected Void visitLimit(Limit node, Void context)
        {
            printNode(node, printLabel(node, node.getLimit()), NODE_COLORS.get(NodeType.DEFAULT));
            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            process(node.getSelect());
            printEdge(node, node.getSelect());

            //TODO implement type other than Table
            if (node.getFrom().isPresent()) {
                process(node.getFrom().get());
                printEdge(node, node.getFrom().get());
            }

            if (node.getWhere().isPresent()) {
                process(node.getWhere().get());
                printEdge(node, node.getWhere().get());
            }

            if (node.getGroupBy().isPresent()) {
                process(node.getGroupBy().get());
                printEdge(node, node.getGroupBy().get());
            }

            if (node.getHaving().isPresent()) {
                process(node.getHaving().get());
                printEdge(node, node.getHaving().get());
            }

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get());
                printEdge(node, node.getOrderBy().get());
            }

            if (node.getLimit().isPresent()) {
                process(node.getLimit().get());
                printEdge(node, node.getLimit().get());
            }

            return null;
        }

        @Override
        protected Void visitInPredicate(InPredicate node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            process(node.getValue());
            printEdge(node, node.getValue());

            process(node.getValueList());
            printEdge(node, node.getValueList());

            return null;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, Void context)
        {
            String name = Joiner.on('.').join(node.getName().getParts());
            printNode(node, printLabel(node, name), NODE_COLORS.get(NodeType.DEFAULT));

            for (Expression argument : node.getArguments()) {
                process(argument);
                printEdge(node, argument);
            }

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get());
                printEdge(node, node.getOrderBy().get());
            }

            if (node.getWindow().isPresent()) {
                process(node.getWindow().get());
                printEdge(node, node.getWindow().get());
            }

            if (node.getFilter().isPresent()) {
                process(node.getFilter().get());
                printEdge(node, node.getFilter().get());
            }

            return null;
        }

        @Override
        protected Void visitStringLiteral(StringLiteral node, Void context)
        {
            printNode(node, printLabel(node, node.getValue()), NODE_COLORS.get(NodeType.DEFAULT));
            return null;
        }

        @Override
        protected Void visitCharLiteral(CharLiteral node, Void context)
        {
            printNode(node, printLabel(node, node.getValue()), NODE_COLORS.get(NodeType.DEFAULT));
            return null;
        }

        @Override
        protected Void visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            printNode(node, printLabel(node, node.toHexString()), NODE_COLORS.get(NodeType.DEFAULT));
            return null;
        }

        @Override
        protected Void visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            printNode(node, printLabel(node, Boolean.toString(node.getValue())), NODE_COLORS.get(NodeType.DEFAULT));
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, Void context)
        {
            QualifiedName resolved = resolvedNameReferences.get(node);
            String resolvedName = "";
            if (resolved != null) {
                resolvedName = "=>" + resolved.toString();
            }
            printNode(node, printLabel(node, node.getValue() + resolvedName), NODE_COLORS.get(NodeType.DEFAULT));
            return null;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            QualifiedName resolved = resolvedNameReferences.get(node);
            String resolvedName = "";
            if (resolved != null) {
                resolvedName = "=>" + resolved.toString();
            }
            printNode(node, printLabel(node, node + resolvedName), NODE_COLORS.get(NodeType.DEFAULT));
            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Void context)
        {
            if (node.getAlias().isPresent()) {
                StringBuilder sb = new StringBuilder();
                sb.append("Alias: ");
                sb.append(node.getAlias().get());
                printNode(node, printLabel(node, sb.toString()), NODE_COLORS.get(NodeType.DEFAULT));
            } else {
                printNode(node, NODE_COLORS.get(NodeType.DEFAULT));
            }

            process(node.getExpression());
            printEdge(node, node.getExpression());
            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Void context)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(node.getClass().getSimpleName());
            if (!node.getAliases().isEmpty()) {
                sb.append(" [Aliases: ");
                Joiner.on(", ").appendTo(sb, node.getAliases());
                sb.append("]");
            }

            printNode(node, sb.toString(), NODE_COLORS.get(NodeType.DEFAULT));

            node.getTarget().ifPresent(value -> {
                process(value);
                printEdge(node, node.getTarget().get());
            });

            return null;
        }

        @Override
        protected Void visitLikePredicate(LikePredicate node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            process(node.getValue(), context);
            printEdge(node, node.getValue());

            process(node.getPattern(), context);
            printEdge(node, node.getPattern());

            node.getEscape().ifPresent(value -> process(value, context));

            return null;
        }

        @Override
        protected Void visitLongLiteral(LongLiteral node, Void context)
        {
            printNode(node, printLabel(node, Long.toString(node.getValue())), NODE_COLORS.get(NodeType.DEFAULT));
            return null;
        }

        @Override
        protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            printNode(node, printLabel(node, node.getOperator().toString()), NODE_COLORS.get(NodeType.DEFAULT));
            if (node.getLeft() != null) {
                process(node.getLeft());
                printEdge(node, node.getLeft());
            }

            if (node.getRight() != null) {
                process(node.getRight());
                printEdge(node, node.getRight());
            }
            return null;
        }

        @Override
        protected Void visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            process(node.getQuery());
            printEdge(node, node.getQuery());

            return null;
        }

        @Override
        protected Void visitSortItem(SortItem node, Void context)
        {
            printNode(node, printLabel(node, node.getSortKey().toString()), NODE_COLORS.get(NodeType.DEFAULT));
            process(node.getSortKey());
            printEdge(node, node.getSortKey());

            return null;
        }

        @Override
        protected Void visitTable(Table node, Void context)
        {
            String name = Joiner.on('.').join(node.getName().getParts());
            printNode(node, printLabel(node, name), NODE_COLORS.get(NodeType.DEFAULT));

            return null;
        }

        @Override
        protected Void visitValues(Values node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            for (Expression row : node.getRows()) {
                process(row);
                printEdge(node, row);
            }
            return null;
        }

        @Override
        protected Void visitRow(Row node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            for (Expression expression : node.getItems()) {
                process(expression);
                printEdge(node, expression);
            }
            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            process(node.getQuery());
            printEdge(node, node.getQuery());

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Void context)
        {
            printNode(node, printLabel(node, node.getAlias().toString()), NODE_COLORS.get(NodeType.DEFAULT));

            process(node.getRelation());
            printEdge(node, node.getRelation());

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Void context)
        {
            printNode(node, printLabel(node,node.getType() + " (" + node.getSamplePercentage() + ")"), NODE_COLORS.get(NodeType.DEFAULT));

            process(node.getRelation());
            printEdge(node, node.getRelation());

            process(node.getSamplePercentage());
            printEdge(node, node.getSamplePercentage());
            return null;
        }

        @Override
        protected Void visitJoin(Join node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            process(node.getLeft());
            printEdge(node, node.getLeft());

            process(node.getRight());
            printEdge(node, node.getRight());

            node.getCriteria()
                    .filter(criteria -> criteria instanceof JoinOn)
                    .map(criteria -> process(((JoinOn) criteria).getExpression()));

            return null;
        }

        @Override
        protected Void visitCast(Cast node, Void context)
        {
            printNode(node, printLabel(node, node.getType().toString()), NODE_COLORS.get(NodeType.DEFAULT));

            process(node.getExpression());
            printEdge(node, node.getExpression());

            return null;
        }

        @Override
        protected Void visitCreateTable(CreateTable node, Void context)
        {
            String name = Joiner.on('.').join(node.getName().getParts());
            printNode(node, printLabel(node, name), NODE_COLORS.get(NodeType.DEFAULT));

            for (TableElement tableElement : node.getElements()) {
                process(tableElement);
                printEdge(node, tableElement);
            }

            for (Property property : node.getProperties()) {
                process(property);
                printEdge(node, property);
            }

            return null;
        }

        protected Void visitColumnDefinition(ColumnDefinition node, Void context)
        {
            String name = node.getName().toString();
            String type = node.getType().toString();
            printNode(node, printLabel(node, format("%s(%s)", name, type)), NODE_COLORS.get(NodeType.DEFAULT));

            for (Property property : node.getProperties()) {
                process(property);
                printEdge(node, property);
            }
            return null;
        }

        @Override
        protected Void visitCreateTableAsSelect(CreateTableAsSelect node, Void context)
        {
            String name = Joiner.on('.').join(node.getName().getParts());
            printNode(node, printLabel(node, name), NODE_COLORS.get(NodeType.DEFAULT));

            process(node.getQuery());
            printEdge(node, node.getQuery());

            for (Property property : node.getProperties()) {
                process(property);
                printEdge(node, property);
            }

            return null;
        }

        @Override
        protected Void visitProperty(Property node, Void context)
        {
            String key = node.getName().toString();
            String value = node.getValue().toString();
            printNode(node, printLabel(node, format("%s:%s", key, value)), NODE_COLORS.get(NodeType.DEFAULT));

            return null;
        }

        protected Void visitDropTable(DropTable node, Void context)
        {
            return visitStatement(node, context);
        }

        protected Void visitRenameTable(RenameTable node, Void context)
        {
            return visitStatement(node, context);
        }

        protected Void visitRenameView(RenameView node, Void context)
        {
            return visitStatement(node, context);
        }

        protected Void visitComment(Comment node, Void context)
        {
            return visitStatement(node, context);
        }

        protected Void visitRenameColumn(RenameColumn node, Void context)
        {
            return visitStatement(node, context);
        }

        protected Void visitDropColumn(DropColumn node, Void context)
        {
            return visitStatement(node, context);
        }

        @Override
        protected Void visitAddColumn(AddColumn node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            process(node.getColumn(), context);
            printEdge(node, node.getColumn());

            return null;
        }

        @Override
        protected Void visitAnalyze(Analyze node, Void context)
        {
            String name = Joiner.on('.').join(node.getTableName().getParts());
            printNode(node, printLabel(node, name), NODE_COLORS.get(NodeType.DEFAULT));

            for (Property property : node.getProperties()) {
                process(property);
                printEdge(node, property);
            }

            return null;
        }

        @Override
        protected Void visitCreateView(CreateView node, Void context)
        {
            String name = Joiner.on('.').join(node.getName().getParts());
            printNode(node, printLabel(node, name), NODE_COLORS.get(NodeType.DEFAULT));

            process(node.getQuery());
            printEdge(node, node.getQuery());

            return null;
        }


        @Override
        protected Void visitGroupBy(GroupBy node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            for (GroupingElement groupingElement : node.getGroupingElements()) {
                process(groupingElement);
                printEdge(node, groupingElement);
            }

            return null;
        }

        @Override
        protected Void visitCube(Cube node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            for (Expression column : node.getExpressions()) {
                process(column);
                printEdge(node, column);
            }
            return null;
        }

        @Override
        protected Void visitGroupingSets(GroupingSets node, Void context)
        {
            StringBuilder labelBuilder = new StringBuilder();
            labelBuilder.append("(");

            labelBuilder.append(node.getSets().stream().map(set -> {
                StringBuilder sb = new StringBuilder();
                sb.append("(");
                sb.append(set.stream().map(Expression::toString).collect(Collectors.joining(",")));
                sb.append(")");
                return sb.toString();
            }).collect(Collectors.joining(",")));

            labelBuilder.append(")");

            printNode(node, printLabel(node, labelBuilder.toString()), NODE_COLORS.get(NodeType.DEFAULT));

            return null;
        }

        @Override
        protected Void visitRollup(Rollup node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            for (Expression column : node.getExpressions()) {
                process(column);
                printEdge(node, column);
            }
            return null;
        }

        @Override
        protected Void visitSimpleGroupBy(SimpleGroupBy node, Void context)
        {
            printNode(node, NODE_COLORS.get(NodeType.DEFAULT));

            for (Expression column : node.getExpressions()) {
                process(column);
                printEdge(node, column);
            }

            return null;
        }

        private void printEdge(Node from, Node to)
        {
            String fromId = idGenerator.getNodeId(from);
            String toId = idGenerator.getNodeId(to);

            edgeOutput.append(fromId)
                    .append(" -> ")
                    .append(toId)
                    .append(';')
                    .append('\n');
        }

        private void printNode(Node node, String color)
        {
            printNode(node, printLabel(node), color);
        }

        private void printNode(Node node, String label, String color)
        {
            printNode(node, label, null, color);
        }

        private void printNode(Node node, String label, String details, String color)
        {
            String nodeId = idGenerator.getNodeId(node);
            label = escapeSpecialCharacters(label);

            if (details == null || details.isEmpty()) {
                nodeOutput.append(nodeId)
                        .append(format("[label=\"{%s}\", style=\"rounded, filled\", shape=record, fillcolor=%s]", label, color))
                        .append(';')
                        .append('\n');
            }
            else {
                details = escapeSpecialCharacters(details);
                nodeOutput.append(nodeId)
                        .append(format("[label=\"{%s|%s}\", style=\"rounded, filled\", shape=record, fillcolor=%s]", label, details, color))
                        .append(';')
                        .append('\n');
            }
        }

        private String printLabel(Node node)
        {
            return format("%s", node.getClass().getSimpleName());
        }

        private String printLabel(Node node, String value)
        {
            return format("%s[%s]", node.getClass().getSimpleName(), value);
        }

        /**
         * Escape characters that are special to graphviz.
         */
        private static String escapeSpecialCharacters(String label)
        {
            return label
                    .replace("<", "\\<")
                    .replace(">", "\\>")
                    .replace("\"", "\\\"");
        }
    }

    private static class NodeIdGenerator
    {
        private final Map<String, Integer> nodeIds;
        private int idCount;

        public NodeIdGenerator()
        {
            nodeIds = new HashMap<>();
        }

        public String getNodeId(Node from)
        {
            String key = String.format("%d:%d",  from.hashCode(), System.identityHashCode(from));
            int nodeId;

            if (nodeIds.containsKey(key)) {
                nodeId = nodeIds.get(key);
            }
            else {
                idCount++;
                nodeIds.put(key, idCount);
                nodeId = idCount;
            }
            return ("node_" + nodeId);
        }
    }
}
