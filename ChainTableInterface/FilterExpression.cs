// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using Irony.Parsing;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Protocol;
using System;
using System.Collections.Generic;

namespace ChainTableInterface
{
    class FilterStringGrammar : Grammar
    {
        internal readonly NonTerminal comparison, booleanOpExpr;
        FilterStringGrammar()
        {
            var propertyName = new IdentifierTerminal("propertyName");
            var compareOp = new NonTerminal("compareOp");
            compareOp.Rule = ToTerm(QueryComparisons.Equal) | ToTerm(QueryComparisons.NotEqual)
                | ToTerm(QueryComparisons.LessThan) | ToTerm(QueryComparisons.LessThanOrEqual)
                | ToTerm(QueryComparisons.GreaterThan) | ToTerm(QueryComparisons.GreaterThanOrEqual);
            var stringLiteral = new StringLiteral("stringLiteral", "'", StringOptions.AllowsDoubledQuote);
            var booleanLiteral = new ConstantTerminal("booleanLiteral");
            booleanLiteral.Add("true", true);
            booleanLiteral.Add("false", false);
            var literal = new NonTerminal("literal");
            literal.Rule = stringLiteral | booleanLiteral;
            comparison = new NonTerminal("comparison");
            comparison.Rule = propertyName + compareOp + literal;
            var booleanOp = new NonTerminal("booleanOp");
            booleanOp.Rule = ToTerm(TableOperators.And) | ToTerm(TableOperators.Or);
            var booleanExpr = new NonTerminal("booleanExpr");
            booleanOpExpr = new NonTerminal("booleanOpExpr");
            booleanOpExpr.Rule = "(" + booleanExpr + ")" + booleanOp + "(" + booleanExpr + ")";
            booleanExpr.Rule = comparison | booleanOpExpr;
            Root = booleanExpr;
        }
        // Do not construct your own LanguageData from grammar; doing so may not be thread-safe.
        internal static readonly FilterStringGrammar grammar = new FilterStringGrammar();
        internal static readonly LanguageData languageData = new LanguageData(grammar);
    }

    public abstract class FilterExpression
    {
        // ITableEntity does not provide random access to properties. :(
        public abstract bool Evaluate(string partitionKey, string rowKey, IDictionary<string, EntityProperty> properties);
        public bool Evaluate(ITableEntity entity)
        {
            return Evaluate(entity.PartitionKey, entity.RowKey, entity.WriteEntity(null));
        }
        public abstract string ToFilterString();
    }
    public class EmptyFilterExpression : FilterExpression
    {
        public override bool Evaluate(string partitionKey, string rowKey, IDictionary<string, EntityProperty> properties)
        {
            return true;
        }

        public override string ToFilterString()
        {
            return "";
        }
    }
    public class ComparisonExpression : FilterExpression
    {
        public string PropertyName { get; }
        public string Operator { get; }
        public IComparable Value { get; }
        internal ComparisonExpression(string propertyName, string theOperator, IComparable value)
        {
            PropertyName = propertyName;
            Operator = theOperator;
            Value = value;
        }

        int CompareOrdinalIfStrings(IComparable x, IComparable y)
        {
            string xStr = x as string, yStr = y as string;
            return (xStr != null & yStr != null) ? string.CompareOrdinal(xStr, yStr) : x.CompareTo(y);
        }
        public override bool Evaluate(string partitionKey, string rowKey, IDictionary<string, EntityProperty> properties)
        {
            IComparable valueFromEntity;
            EntityProperty entityProperty;
            if (PropertyName == TableConstants.PartitionKey)
                valueFromEntity = partitionKey;
            else if (PropertyName == TableConstants.RowKey)
                valueFromEntity = rowKey;
            else if (properties.TryGetValue(PropertyName, out entityProperty))
                // I think this should work.  Not certain the comparison semantics
                // are the same as real Azure for the more complex property types.
                valueFromEntity = (IComparable)entityProperty.PropertyAsObject;
            else
                // Several tests suggest the behavior of real Azure is to
                // rewrite the expression by pushing all "not" operators into
                // comparison operators (for example,
                // "not (A eq 'A' or B ne 'B')" becomes "A ne 'A' and B eq 'B'")
                // and then treat any comparison of an undefined property as
                // false.  Since we don't support "not" at all, we can simply
                // treat comparisons of undefined properties as false.
                return false;
            try
            {
                switch (Operator)
                {
                    case QueryComparisons.Equal:
                        return Equals(valueFromEntity, Value);
                    case QueryComparisons.NotEqual:
                        return !Equals(valueFromEntity, Value);
                    case QueryComparisons.LessThan:
                        return CompareOrdinalIfStrings(valueFromEntity, Value) < 0;
                    case QueryComparisons.LessThanOrEqual:
                        return CompareOrdinalIfStrings(valueFromEntity, Value) <= 0;
                    case QueryComparisons.GreaterThan:
                        return CompareOrdinalIfStrings(valueFromEntity, Value) > 0;
                    case QueryComparisons.GreaterThanOrEqual:
                        return CompareOrdinalIfStrings(valueFromEntity, Value) >= 0;
                    default:
                        throw new NotImplementedException();  // Should not be reached.
                }
            }
            catch (ArgumentException)
            {
                // Operands have different types.
                // After several tests, I was unable to come up with a coherent
                // theory of the behavior of real Azure.
                return false;
            }
        }

        public override string ToFilterString()
        {
            string stringValue;
            bool? boolValue;
            if ((stringValue = Value as string) != null)
                return TableQuery.GenerateFilterCondition(PropertyName, Operator, stringValue);
            else if ((boolValue = Value as bool?) != null)
                return TableQuery.GenerateFilterConditionForBool(PropertyName, Operator, boolValue.Value);
            else
                throw new NotImplementedException();  // Should not be reached.
        }
    }
    public class BooleanOperatorExpression : FilterExpression
    {
        public FilterExpression Left { get; }
        public string Operator { get; }
        public FilterExpression Right { get; }
        internal BooleanOperatorExpression(FilterExpression left, string theOperator, FilterExpression right)
        {
            Left = left;
            Operator = theOperator;
            Right = right;
        }

        public override bool Evaluate(string partitionKey, string rowKey, IDictionary<string, EntityProperty> properties)
        {
            switch (Operator)
            {
                case TableOperators.And:
                    return Left.Evaluate(partitionKey, rowKey, properties) && Right.Evaluate(partitionKey, rowKey, properties);
                case TableOperators.Or:
                    return Left.Evaluate(partitionKey, rowKey, properties) || Right.Evaluate(partitionKey, rowKey, properties);
                default:
                    throw new NotImplementedException();  // Should not be reached.
            }
        }

        public override string ToFilterString()
        {
            return TableQuery.CombineFilters(Left.ToFilterString(), Operator, Right.ToFilterString());
        }
    }

    public static partial class ChainTableUtils
    {
        internal static FilterExpression MakeFilterExpression(ParseTreeNode node)
        {
            node = node.ChildNodes[0];
            if (node.Term == FilterStringGrammar.grammar.comparison)
            {
                return new ComparisonExpression(
                    node.ChildNodes[0].FindTokenAndGetText(),
                    node.ChildNodes[1].FindTokenAndGetText(),
                    (IComparable)node.ChildNodes[2].FindToken().Value);
            }
            else if (node.Term == FilterStringGrammar.grammar.booleanOpExpr)
            {
                return new BooleanOperatorExpression(
                    MakeFilterExpression(node.ChildNodes[1]),
                    node.ChildNodes[3].FindTokenAndGetText(),
                    MakeFilterExpression(node.ChildNodes[5]));
            }
            else
                throw new NotImplementedException();  // Should not be reached
        }

        // This is intended to support filter strings generated with
        // TableQuery.CombineFilters and TableQuery.GenerateFilterCondition
        // (currently string and bool only), plus the empty string.
        public static FilterExpression ParseFilterString(string filterString)
        {
            if (string.IsNullOrEmpty(filterString))
                return new EmptyFilterExpression();
            ParseTree parseTree = new Parser(FilterStringGrammar.languageData).Parse(filterString);
            if (parseTree.Status != ParseTreeStatus.Parsed)
                throw new ArgumentException("Failed to parse filter string");
            return MakeFilterExpression(parseTree.Root);
        }
    }
}
