// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Protocol;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace ChainTableInterface
{
    public class PrimaryKey : IEquatable<PrimaryKey>, IComparable<PrimaryKey>
    {
        public string PartitionKey { get; }
        public string RowKey { get; }
        public PrimaryKey(string PartitionKey, string RowKey)
        {
            if (PartitionKey == null) throw new ArgumentNullException(nameof(PartitionKey));
            if (RowKey == null) throw new ArgumentNullException(nameof(RowKey));
            this.PartitionKey = PartitionKey;
            this.RowKey = RowKey;
        }
        internal static PrimaryKey Of(ITableEntity entity)
        {
            return new PrimaryKey(entity.PartitionKey, entity.RowKey);
        }

        public int CompareTo(PrimaryKey other)
        {
            // XXX Framework please
            int cmp = string.CompareOrdinal(PartitionKey, other.PartitionKey);
            if (cmp != 0) return cmp;
            return string.CompareOrdinal(RowKey, other.RowKey);
        }

        public bool Equals(PrimaryKey that)
        {
            return Equals(PartitionKey, that.PartitionKey) && Equals(RowKey, that.RowKey);
        }
        public override bool Equals(object o)
        {
            return Equals(o as PrimaryKey);
        }

        public override int GetHashCode()
        {
            // Avoid a dependency on Migration.Utils for now.
            unchecked
            {
                return 31 * PartitionKey.GetHashCode() + RowKey.GetHashCode();
            }
        }

        public override string ToString()
        {
            return string.Format("PrimaryKey({0}, {1})", PartitionKey, RowKey);
        }
    }

    public static partial class ChainTableUtils
    {
        // No extension properties. :(
        // It would be even more awesome if extension properties could be used
        // in the property constructor syntax.
        public static PrimaryKey GetPrimaryKey(this ITableEntity entity)
        {
            return new PrimaryKey(entity.PartitionKey, entity.RowKey);
        }
        public static void SetPrimaryKey(this ITableEntity entity, PrimaryKey primaryKey)
        {
            entity.PartitionKey = primaryKey.PartitionKey;
            entity.RowKey = primaryKey.RowKey;
        }

        // From STable
        public static StorageException GenerateException(HttpStatusCode code)
        {
            RequestResult res = new RequestResult();
            res.HttpStatusCode = (int)code;
            return new StorageException(res, string.Format("ChainTable status code {0}", (int)code), null);
        }
        public static ChainTableBatchException GenerateBatchException(HttpStatusCode cause, int opId)
        {
            var underlyingEx = GenerateException(cause);
            return new ChainTableBatchException(opId, underlyingEx);
        }
        public static StorageException GenerateInternalException(StorageException ex)
        {
            return new StorageException("Unexpected exception on ChainTable internal operation", ex);
        }
        public static HttpStatusCode GetHttpStatusCode(this StorageException ex)
        {
            return (HttpStatusCode)ex.RequestInformation.HttpStatusCode;
        }

        // Currently used by InMemoryTable.
        public static TElement CopyEntity<TElement>(ITableEntity entity) where TElement : ITableEntity, new()
        {
            // Copy mutable data in case both entities are DynamicTableEntity.
            // https://github.com/Azure/azure-storage-net/issues/154
            // We could equally well do "new TElement" and use the setters directly.
            return (TElement)AzureTableAccessors.GetRetrieveResolver(TableOperation.Retrieve<TElement>("1", "1"))(
                entity.PartitionKey, entity.RowKey, entity.Timestamp, CopyPropertyDict(entity.WriteEntity(null)), entity.ETag);
        }
        public static Dictionary<string, EntityProperty> CopyPropertyDict(IDictionary<string, EntityProperty> dict)
        {
            return dict.ToDictionary(kvp => kvp.Key, kvp => CopyProperty(kvp.Value));
        }
        public static EntityProperty CopyProperty(EntityProperty ep)
        {
            return EntityProperty.CreateEntityPropertyFromObject(ep.PropertyAsObject);
        }

        public static TableQuery<TElement2> CopyQuery<TElement,TElement2>(TableQuery<TElement> query)
        {
            // XXX: Probably does not work with LINQ-generated queries.  Decide
            // whether IChainTable2 should allow such queries.
            return new TableQuery<TElement2>
            {
                FilterString = query.FilterString,
                SelectColumns = query.SelectColumns,
                TakeCount = query.TakeCount,
            };
        }

        // XXX: Preserve the entity type without the caller having to specify it?
        public static TableOperation CopyOperation<TEntity>(TableOperation op)
            where TEntity : ITableEntity, new()
        {
            ITableEntity newEntity = CopyEntity<TEntity>(op.GetEntity());
            switch (op.GetOperationType())
            {
                case TableOperationType.Insert:
                    return TableOperation.Insert(newEntity);
                case TableOperationType.Replace:
                    return TableOperation.Replace(newEntity);
                case TableOperationType.Merge:
                    return TableOperation.Merge(newEntity);
                case TableOperationType.Delete:
                    return TableOperation.Delete(newEntity);
                case TableOperationType.InsertOrReplace:
                    return TableOperation.InsertOrReplace(newEntity);
                case TableOperationType.InsertOrMerge:
                    return TableOperation.InsertOrMerge(newEntity);
                default:
                    throw new NotImplementedException();
            }
        }
        public static TableBatchOperation CopyBatch<TEntity>(TableBatchOperation batch)
            where TEntity : ITableEntity, new()
        {
            var batch2 = new TableBatchOperation();
            foreach (TableOperation op in batch)
                batch2.Add(CopyOperation<TEntity>(op));
            return batch2;
        }

        public static string GetBatchPartitionKey(TableBatchOperation batch)
        {
            try
            {
                return (from op in batch select op.GetEntity().PartitionKey).Distinct().Single();
            }
            catch (InvalidOperationException)
            {
                throw new ArgumentException("Invalid batch: empty or targets multiple partitions.");
            }
        }

        public static PrimaryKey GetRetrievePrimaryKey(this TableOperation op)
        {
            return new PrimaryKey(op.GetRetrievePartitionKey(), op.GetRetrieveRowKey());
        }

        // Smarter version of TableQuery.CombineFilters that accepts empty strings, meaning "match all".
        public static string CombineFilters(string filterA, string operatorString, string filterB)
        {
            string singleNonemptyFilter;
            if (string.IsNullOrEmpty(filterA))
                singleNonemptyFilter = filterB;
            else if (string.IsNullOrEmpty(filterB))
                singleNonemptyFilter = filterA;
            else
                return TableQuery.CombineFilters(filterA, operatorString, filterB);
            switch (operatorString)
            {
                case TableOperators.And:
                    return singleNonemptyFilter;
                case TableOperators.Or:
                    return "";
                default:
                    throw new ArgumentException(nameof(operatorString));
            }
        }

        public static string GeneratePointRetrievalFilterCondition(PrimaryKey primaryKey)
        {
            return TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition(TableConstants.PartitionKey, QueryComparisons.Equal, primaryKey.PartitionKey),
                TableOperators.And,
                TableQuery.GenerateFilterCondition(TableConstants.RowKey, QueryComparisons.Equal, primaryKey.RowKey)
                );
        }

        public static string GenerateContinuationFilterCondition(PrimaryKey nextPrimaryKey)
        {
            return TableQuery.CombineFilters(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(TableConstants.PartitionKey, QueryComparisons.Equal, nextPrimaryKey.PartitionKey),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(TableConstants.RowKey, QueryComparisons.GreaterThanOrEqual, nextPrimaryKey.RowKey)
                    ),
                TableOperators.Or,
                TableQuery.GenerateFilterCondition(TableConstants.PartitionKey, QueryComparisons.GreaterThan, nextPrimaryKey.PartitionKey)
                );
        }

        static string TryGetPartitionKeyFromComparison(FilterExpression filterExpr)
        {
            ComparisonExpression cmp = filterExpr as ComparisonExpression;
            if (cmp != null
                && cmp.PropertyName == TableConstants.PartitionKey
                && cmp.Operator == QueryComparisons.Equal)
                return cmp.Value as string;  // null if wrong type: OK
            else
                return null;
        }
        // Intended for ExecuteQueryAtomicAsync validation.
        public static string GetSingleTargetedPartitionKey(FilterExpression filterExpr)
        {
            string partitionKey;
            BooleanOperatorExpression boe;
            if ((partitionKey = TryGetPartitionKeyFromComparison(filterExpr)) != null)
                return partitionKey;
            else if ((boe = filterExpr as BooleanOperatorExpression) != null
                && (partitionKey = TryGetPartitionKeyFromComparison(boe.Left)) != null)
                return partitionKey;
            else
            {
                throw new ArgumentException(
                    "Unable to verify syntactically that the filter string targets a single partition.  " +
                    "It must be of the form `PartitionKey eq 'foo'` or `(PartitionKey eq 'foo') and (...)`.");
            }
        }

        // Believe it or not!  Tested with the Visual Studio Azure table editor.
        public static readonly PrimaryKey FirstValidPrimaryKey = new PrimaryKey("", "");

        public static PrimaryKey NextValidPrimaryKeyAfter(PrimaryKey key)
        {
            // Row key is a UTF-16 string, and U+0000 through U+001F are not
            // allowed, so appending U+0020 gives the next valid key.
            // https://msdn.microsoft.com/en-us/library/azure/dd179338.aspx
            return new PrimaryKey(key.PartitionKey, key.RowKey + " ");
        }
    }

    // XXX: What's the proper naming convention for this type of class in C#?
    public abstract class AbstractChainTable2 : IChainTable2
    {
        // Essential operations.
        public abstract Task<IList<TableResult>> ExecuteBatchAsync(TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null);
        public abstract Task<IList<TElement>> ExecuteQueryAtomicAsync<TElement>(TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null) where TElement : ITableEntity, new();
        public abstract Task<IQueryStream<TElement>> ExecuteQueryStreamedAsync<TElement>(TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null) where TElement : ITableEntity, new();

        // Single in terms of batch
        // FIXME: IChainTable2 (following IChainTable) allows Retrieve in
        // ExecuteAsync but not ExecuteBatchAsync.  What do we want to do?
        public virtual async Task<TableResult> ExecuteAsync(TableOperation operation,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            if (operation.GetOperationType() == TableOperationType.Retrieve)
            {
                // Retrieve supports custom entity resolvers but query doesn't.
                // Work around this by querying as DynamicTableEntity and then
                // running the custom resolver.  XXX: Fix IChainTable2 so we can
                // skip this step and avoid the cost.
                var query = new TableQuery<DynamicTableEntity> {
                    FilterString = ChainTableUtils.GeneratePointRetrievalFilterCondition(operation.GetRetrievePrimaryKey())
                };
                DynamicTableEntity entity = (await ExecuteQueryAtomicAsync(query, requestOptions, operationContext)).SingleOrDefault();
                if (entity == null)
                    return new TableResult { HttpStatusCode = (int)HttpStatusCode.NotFound };
                else
                    return new TableResult
                    {
                        HttpStatusCode = (int)HttpStatusCode.OK,
                        Result = AzureTableAccessors.GetRetrieveResolver(operation)(
                            entity.PartitionKey, entity.RowKey, entity.Timestamp, entity.WriteEntity(null), entity.ETag),
                        Etag = entity.ETag
                    };
            }

            var batch = new TableBatchOperation();
            batch.Add(operation);
            try
            {
                IList<TableResult> resultList = await ExecuteBatchAsync(batch, requestOptions, operationContext);
                Debug.Assert(resultList.Count == 1);
                return resultList[0];
            }
            catch (ChainTableBatchException e)
            {
                // XXX: Does this lose the stack trace?
                throw new StorageException(e.RequestInformation, e.Message, e.InnerException);
            }
        }

        // Stubs for operations that many chain tables may not need to implement if callers don't use them.
        public string GetTableID()
        {
            return null;
        }
        public bool IsReadOnly(OperationContext operationContext)
        {
            return false;
        }
        public bool CreateIfNotExists()
        {
            throw new NotImplementedException();
        }
        public bool DeleteIfExists()
        {
            throw new NotImplementedException();
        }
    }
}
