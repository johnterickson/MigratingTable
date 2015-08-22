// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ChainTableInterface;
using System.Linq;
using System.Net;

namespace Migration
{
    class InMemoryTable : AbstractMirrorChainTable2
    {

        int nextEtag = 1;
        SortedDictionary<PrimaryKey, DynamicTableEntity> table = new SortedDictionary<PrimaryKey, DynamicTableEntity>();

        public SortedDictionary<PrimaryKey, DynamicTableEntity> Dump()
        {
            var dump = new SortedDictionary<PrimaryKey, DynamicTableEntity>();
            foreach (KeyValuePair<PrimaryKey, DynamicTableEntity> kvp in table)
                dump.Add(kvp.Key, ChainTableUtils.CopyEntity<DynamicTableEntity>(kvp.Value));
            return dump;
        }

        private void Merge(DynamicTableEntity to, ITableEntity from)
        {
            foreach (KeyValuePair<string,EntityProperty> kvp in from.WriteEntity(null))
            {
                to.Properties[kvp.Key] = ChainTableUtils.CopyProperty(kvp.Value);
            }
        }

        public override Task<IList<TableResult>> ExecuteMirrorBatchAsync(
            TableBatchOperation originalBatch, IList<TableResult> originalResponse,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            ChainTableUtils.GetBatchPartitionKey(originalBatch);  // For validation; result ignored

            // Copy the table.  Entities are aliased to the original table, so don't mutate them.
            var tmpTable = new SortedDictionary<PrimaryKey, DynamicTableEntity>(table);
            int tmpNextEtag = nextEtag;
            var results = new List<TableResult>();
            for (int i = 0; i < originalBatch.Count; i++)
            {
                TableOperation op = originalBatch[i];
                TableOperationType opType = op.GetOperationType();
                ITableEntity passedEntity = op.GetEntity();
                PrimaryKey key = passedEntity.GetPrimaryKey();
                DynamicTableEntity oldEntity = tmpTable.GetValueOrDefault(key);

                DynamicTableEntity newEntity = null;
                HttpStatusCode statusCode = HttpStatusCode.NoContent;

                if (opType == TableOperationType.Insert)
                {
                    if (oldEntity != null)
                    {
                        throw ChainTableUtils.GenerateBatchException(HttpStatusCode.Conflict, i);
                    }
                    else
                    {
                        newEntity = ChainTableUtils.CopyEntity<DynamicTableEntity>(passedEntity);
                        statusCode = HttpStatusCode.Created;
                    }
                }
                else if (opType == TableOperationType.InsertOrReplace)
                {
                    newEntity = ChainTableUtils.CopyEntity<DynamicTableEntity>(passedEntity);
                }
                else if (opType == TableOperationType.InsertOrMerge)
                {
                    if (oldEntity == null)
                    {
                        newEntity = ChainTableUtils.CopyEntity<DynamicTableEntity>(passedEntity);
                    }
                    else
                    {
                        newEntity = ChainTableUtils.CopyEntity<DynamicTableEntity>(oldEntity);
                        Merge(newEntity, passedEntity);
                    }
                }
                else if (opType == TableOperationType.Delete
                    && passedEntity.ETag == ChainTable2Constants.ETAG_DELETE_IF_EXISTS)
                {
                    tmpTable.Remove(key);
                }
                else if (oldEntity == null)
                {
                    throw ChainTableUtils.GenerateBatchException(HttpStatusCode.NotFound, i);
                }
                else if (string.IsNullOrEmpty(passedEntity.ETag))
                {
                    // Enforce this because real Azure table will.
                    // XXX Ideally do this up front.
                    throw new ArgumentException(string.Format("Operation {0} requires an explicit ETag.", i));
                }
                else if (passedEntity.ETag != ChainTable2Constants.ETAG_ANY
                    && oldEntity.ETag != passedEntity.ETag)
                {
                    throw ChainTableUtils.GenerateBatchException(HttpStatusCode.PreconditionFailed, i);
                }
                else if (opType == TableOperationType.Delete)
                {
                    tmpTable.Remove(key);
                }
                else if (opType == TableOperationType.Replace)
                {
                    newEntity = ChainTableUtils.CopyEntity<DynamicTableEntity>(passedEntity);
                }
                else if (opType == TableOperationType.Merge)
                {
                    newEntity = ChainTableUtils.CopyEntity<DynamicTableEntity>(oldEntity);
                    Merge(newEntity, passedEntity);
                }
                else
                {
                    // IChainTable2 does not allow Retrieve in a batch.
                    throw new NotImplementedException();
                }

                if (newEntity != null)
                {
                    newEntity.ETag = (originalResponse != null) ? originalResponse[i].Etag : (tmpNextEtag++).ToString();
                    newEntity.Timestamp = DateTimeOffset.MinValue;  // Arbitrary, deterministic
                    tmpTable[key] = newEntity;
                }
                results.Add(new TableResult {
                    Result = passedEntity,
                    HttpStatusCode = (int)statusCode,
                    Etag = (newEntity != null) ? newEntity.ETag : null,
                });
            }

            // If we got here, commit.
            table = tmpTable;
            nextEtag = tmpNextEtag;
            for (int i = 0; i < originalBatch.Count; i++)
            {
                if (results[i].Etag != null)  // not delete
                    originalBatch[i].GetEntity().ETag = results[i].Etag;
            }
            return Task.FromResult((IList<TableResult>)results);
        }

        public override Task<IList<TElement>> ExecuteQueryAtomicAsync<TElement>(
            TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            FilterExpression filterExpr = ChainTableUtils.ParseFilterString(query.FilterString);
            ChainTableUtils.GetSingleTargetedPartitionKey(filterExpr);  // validation, ignore result
            if (query.SelectColumns != null)
                throw new NotImplementedException("select");
            if (query.TakeCount != null)
                throw new NotImplementedException("top");
            return Task.FromResult(
                (IList<TElement>)(from kvp in table where filterExpr.Evaluate(kvp.Value)
                                  select ChainTableUtils.CopyEntity<TElement>(kvp.Value)).ToList());
        }

        class QueryStream<TElement> : IQueryStream<TElement>
            where TElement : ITableEntity, new()
        {
            readonly FilterExpression filterExpr;
            IEnumerator<KeyValuePair<PrimaryKey, DynamicTableEntity>> enumerator;
            internal QueryStream(FilterExpression filterExpr, IEnumerator<KeyValuePair<PrimaryKey, DynamicTableEntity>> enumerator)
            {
                this.filterExpr = filterExpr;
                this.enumerator = enumerator;
                MoveNext();
            }

            void MoveNext()
            {
                do
                {
                    if (!enumerator.MoveNext())
                    {
                        enumerator = null;
                        return;
                    }
                } while (!filterExpr.Evaluate(enumerator.Current.Value));
            }

            public Task<PrimaryKey> GetContinuationPrimaryKeyAsync()
            {
                CheckDisposed();
                return Task.FromResult(enumerator == null ? null : enumerator.Current.Key);
            }

            public Task<TElement> ReadRowAsync()
            {
                CheckDisposed();
                if (enumerator == null)
                    return Task.FromResult(default(TElement));
                TElement entity = ChainTableUtils.CopyEntity<TElement>(enumerator.Current.Value);
                MoveNext();
                return Task.FromResult(entity);
            }

            private bool disposed = false;
            private void CheckDisposed()
            {
                if (disposed)
                    throw new ObjectDisposedException("InMemoryTable QueryStream is disposed");
            }
            public void Dispose() {
                disposed = true;
                // Callers could conceivably care about freeing a large snapshot.
                enumerator = null;
            }
        }

        public override Task<IQueryStream<TElement>> ExecuteQueryStreamedAsync<TElement>(
            TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            FilterExpression filterExpr = ChainTableUtils.ParseFilterString(query.FilterString);
            if (query.SelectColumns != null)
                throw new NotImplementedException("select");
            if (query.TakeCount != null)
                throw new NotImplementedException("top");
            // Easy deterministic implementation compliant with IChainTable2
            // API: scan a snapshot.  InMemoryTableWithHistory has the
            // nondeterministic implementation.
            // XXX: Nobody calls this any more.  We could delete the code and
            // throw NotImplementedException.
            return Task.FromResult((IQueryStream<TElement>)new QueryStream<TElement>(filterExpr, table.GetEnumerator()));
        }
    }
}
