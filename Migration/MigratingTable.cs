// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using ChainTableInterface;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Table.Protocol;
using System.Net;
using System.Diagnostics;

namespace Migration
{
    public enum MTableOptionalBug
    {
        // These were all made at some point during actual development.
        // (I'm not 100% sure they're 100% equivalent to the original versions, but close enough.)
        QueryAtomicFilterShadowing,
        QueryStreamedFilterShadowing,
        QueryStreamedLock,
        QueryStreamedBackUpNewStream,
        QueryStreamedSaveNewConfig,
        DeleteNoLeaveTombstonesETag,
        DeletePrimaryKey,
        EnsurePartitionSwitchedFromPopulated,
        TombstoneOutputETag,

        // Some other "bugs", which a developer might not realistically make in this
        // form but represent some ways we can break the protocol guarantees.
        MigrateSkipPreferOld,
        MigrateSkipUseNewWithTombstones,
        InsertBehindMigrator,

        NumBugs,
    }

    enum TableClientState
    {
        // In order to push configuration without synchronizing all clients,
        // clients in each pair of successive states must be able to coexist.
        // Similarly, to transition from direct access to the old table to
        // MigratingTable and ultimately to direct access to the new table,
        // clients in the first (last) state must be able to coexist with
        // clients directly accessing the old (respectively, new) table.

        // Use oldTable only but strip MTable metadata from query results.
        USE_OLD_HIDE_METADATA,

        // Read the overlaid view.  Writes mark the partition as populated
        // and then go to the old table unless the partition is already
        // switched by a PREFER_NEW client.
        PREFER_OLD,

        // Main state: read the composite view, and always ensure a
        // partition is switched before writing so as not to insert data
        // behind the migrator.  (Migrator is copying rows.)
        PREFER_NEW,

        // Use newTable only (now that all copying is done), but continue
        // to insert tombstones for the benefit of readers still in
        // PREFER_NEW.
        USE_NEW_WITH_TOMBSTONES,

        // Use newTable only.  Don't insert new partition meta rows or
        // tombstones, and strip existing ones from query results.
        // Writes may have to cope with existing tombstones.
        // (Migrator is deleting the meta rows and tombstones.)
        USE_NEW_HIDE_METADATA,
    }

    public class MTableConfiguration
    {
        // In the future, we could assign these states at finer granularity than
        // the entire table, or alternatively just layer a sharded table on top
        // of the MigratingTable.
        readonly internal TableClientState state;

        internal MTableConfiguration(TableClientState state)
        {
            this.state = state;
        }
    }

    public class MigratingTable : AbstractChainTable2
    {
        readonly MTableOptionalBug? enabledBug;
        internal bool IsBugEnabled(MTableOptionalBug bug)
        {
            return enabledBug == bug;
        }

        // XXX Happy with this?
        internal const string ROW_KEY_NEVER_EXISTS = "_mtable_never_exists";
        internal const string ROW_KEY_PARTITION_META = "_mtable_partition_meta";
        internal const string ROW_KEY_PARTITION_POPULATED_ASSERTION = "_mtable_partition_populated_assertion";

        internal static bool RowKeyIsInternal(string rowKey)
        {
            return rowKey == ROW_KEY_PARTITION_META || rowKey == ROW_KEY_PARTITION_POPULATED_ASSERTION;
        }

        // Currently we keep no subscription at rest.
        // If that changes, we'll need to implement IDisposable.
        internal readonly IReadOnlyConfigurationService<MTableConfiguration> configService;
        internal readonly IChainTable2 oldTable, newTable;
        internal readonly IChainTableMonitor monitor;

        public MigratingTable(IReadOnlyConfigurationService<MTableConfiguration> configService,
            IChainTable2 oldTable, IChainTable2 newTable, IChainTableMonitor monitor, MTableOptionalBug? bugToEnable = null)
        {
            this.enabledBug = bugToEnable;
            this.configService = configService;
            this.oldTable = oldTable;
            this.newTable = newTable;
            this.monitor = monitor;
        }

        // Marks the partition populated unless it is already switched.
        async Task TryMarkPartitionPopulatedAsync(string partitionKey, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            var markPopulatedBatch = new TableBatchOperation();
            markPopulatedBatch.Insert(new MTableEntity
            {
                PartitionKey = partitionKey,
                RowKey = ROW_KEY_PARTITION_META,
                partitionState = MTablePartitionState.POPULATED
            });
            markPopulatedBatch.Insert(new DynamicTableEntity
            {
                PartitionKey = partitionKey,
                RowKey = ROW_KEY_PARTITION_POPULATED_ASSERTION
            });
            try
            {
                await oldTable.ExecuteBatchAsync(markPopulatedBatch, requestOptions, operationContext);
            }
            // XXX: Optimization opportunity: if we swap the order of the
            // inserts, we can tell here if the partition is already switched.
            catch (StorageException ex) {
                if (ex.GetHttpStatusCode() != HttpStatusCode.Conflict)
                    throw ChainTableUtils.GenerateInternalException(ex);
            }
            await monitor.AnnotateLastBackendCallAsync();
        }

        internal async Task EnsurePartitionSwitchedAsync(string partitionKey, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            var metaQuery = new TableQuery<MTableEntity>
            {
                FilterString = ChainTableUtils.GeneratePointRetrievalFilterCondition(
                    new PrimaryKey(partitionKey, ROW_KEY_PARTITION_META))
            };
            Recheck:
            MTablePartitionState? state;
            if (IsBugEnabled(MTableOptionalBug.EnsurePartitionSwitchedFromPopulated))
                state = null;
            else
            {
                state =
                    (from r in (await oldTable.ExecuteQueryAtomicAsync(metaQuery, requestOptions, operationContext))
                     select r.partitionState).SingleOrDefault();
                await monitor.AnnotateLastBackendCallAsync();
            }
            switch (state)
            {
                case null:
                    try
                    {
                        await oldTable.ExecuteAsync(TableOperation.Insert(new MTableEntity
                        {
                            PartitionKey = partitionKey,
                            RowKey = ROW_KEY_PARTITION_META,
                            partitionState = MTablePartitionState.SWITCHED
                        }), requestOptions, operationContext);
                    }
                    catch (StorageException ex)
                    {
                        if (ex.GetHttpStatusCode() != HttpStatusCode.Conflict)
                            throw ChainTableUtils.GenerateInternalException(ex);
                        if (!IsBugEnabled(MTableOptionalBug.EnsurePartitionSwitchedFromPopulated))
                        {
                            await monitor.AnnotateLastBackendCallAsync();
                            // We could now be in POPULATED or SWITCHED.
                            // XXX: In production, what's more likely?  Is it faster
                            // to recheck first or just try the case below?
                            goto Recheck;
                        }
                    }
                    await monitor.AnnotateLastBackendCallAsync();
                    return;
                case MTablePartitionState.POPULATED:
                    try
                    {
                        var batch = new TableBatchOperation();
                        batch.Replace(new MTableEntity
                        {
                            PartitionKey = partitionKey,
                            RowKey = ROW_KEY_PARTITION_META,
                            ETag = ChainTable2Constants.ETAG_ANY,
                            partitionState = MTablePartitionState.SWITCHED
                        });
                        batch.Delete(new MTableEntity
                        {
                            PartitionKey = partitionKey,
                            RowKey = ROW_KEY_PARTITION_POPULATED_ASSERTION,
                            ETag = ChainTable2Constants.ETAG_ANY,
                        });
                        await oldTable.ExecuteBatchAsync(batch, requestOptions, operationContext);
                    }
                    catch (ChainTableBatchException ex)
                    {
                        // The only way this can fail (within the semantics) is
                        // if someone else moved the partition to SWITCHED.
                        if (!(ex.FailedOpIndex == 1 && ex.GetHttpStatusCode() == HttpStatusCode.NotFound))
                            throw ChainTableUtils.GenerateInternalException(ex);
                    }
                    await monitor.AnnotateLastBackendCallAsync();
                    return;
                case MTablePartitionState.SWITCHED:
                    // Nothing to do
                    return;
            }
        }

        // NOTE: Mutates entity
        internal async Task TryCopyEntityToNewTableAsync(MTableEntity entity, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            try
            {
                await newTable.ExecuteAsync(TableOperation.Insert(entity), requestOptions, operationContext);
            }
            catch (StorageException ex)
            {
                if (ex.GetHttpStatusCode() != HttpStatusCode.Conflict)
                    throw ChainTableUtils.GenerateInternalException(ex);
                await monitor.AnnotateLastBackendCallAsync();
                return;
            }
            await monitor.AnnotateLastBackendCallAsync(
                spuriousETagChanges: new List<SpuriousETagChange> {
                            new SpuriousETagChange(entity.PartitionKey, entity.RowKey, entity.ETag) });
        }

        /*
         * FIXME: This will not work against real Azure table because a batch
         * can affect up to 100 rows but a filter string only allows 15
         * comparisons.  The only other thing we can really do in general is
         * query the whole partition, but that might be slow.  We can try an
         * atomic query of the whole partition (maybe even with a timeout lower
         * than the default) and if that fails, fall back to breaking the query
         * into up to 8 partial queries.
         */
        static TableQuery<MTableEntity> GenerateQueryForAffectedRows(TableBatchOperation batch)
        {
            string rowKeyFilterString = null;
            foreach (TableOperation op in batch)
            {
                string comparison = TableQuery.GenerateFilterCondition(
                    TableConstants.RowKey, QueryComparisons.Equal, op.GetEntity().RowKey);
                rowKeyFilterString = (rowKeyFilterString == null) ? comparison
                    : TableQuery.CombineFilters(rowKeyFilterString, TableOperators.Or, comparison);
            }

            string filterString = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition(
                    TableConstants.PartitionKey, QueryComparisons.Equal, ChainTableUtils.GetBatchPartitionKey(batch)),
                TableOperators.And,
                rowKeyFilterString);

            return new TableQuery<MTableEntity> { FilterString = filterString };
        }

        async Task EnsureAffectedRowsMigratedAsync(TableBatchOperation batch, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            string partitionKey = ChainTableUtils.GetBatchPartitionKey(batch);

            var query = GenerateQueryForAffectedRows(batch);

            IList<MTableEntity> oldRows = await oldTable.ExecuteQueryAtomicAsync(query, requestOptions, operationContext);
            await monitor.AnnotateLastBackendCallAsync();

            IList<MTableEntity> newRows = await newTable.ExecuteQueryAtomicAsync(query, requestOptions, operationContext);
            await monitor.AnnotateLastBackendCallAsync();

            Dictionary<string, MTableEntity> oldDict = oldRows.ToDictionary(ent => ent.RowKey);
            Dictionary<string, MTableEntity> newDict = newRows.ToDictionary(ent => ent.RowKey);

            // Migrate any affected rows not already migrated.
            foreach (TableOperation op in batch)
            {
                string targetedRowKey = op.GetEntity().RowKey;
                MTableEntity oldEntity;
                if (oldDict.TryGetValue(targetedRowKey, out oldEntity) && !newDict.ContainsKey(targetedRowKey))
                    await TryCopyEntityToNewTableAsync(oldEntity, requestOptions, operationContext);
            }
        }

        HttpStatusCode? CheckExistingEntity(ITableEntity passedEntity, MTableEntity existingEntity)
        {
            if (existingEntity == null || existingEntity.deleted)
                return HttpStatusCode.NotFound;
            else if (passedEntity.ETag != ChainTable2Constants.ETAG_ANY
                && existingEntity.ETag != passedEntity.ETag)
                return HttpStatusCode.PreconditionFailed;
            else
                return null;
        }
        MTableEntity ImportWithIfMatch(ITableEntity passedEntity, string ifMatch)
        {
            MTableEntity newEntity = ChainTableUtils.CopyEntity<MTableEntity>(passedEntity);
            newEntity.ETag = ifMatch;
            return newEntity;
        }

        void TranslateOperationForNewTable(
            TableOperation op, MTableEntity existingEntity, bool leaveTombstones,
            ref TableOperation newOp, ref HttpStatusCode? errorCode)
        {
            ITableEntity passedEntity = op.GetEntity();
            TableOperationType opType = op.GetOperationType();
            switch (opType)
            {
                case TableOperationType.Insert:
                    if (existingEntity == null)
                        newOp = TableOperation.Insert(ChainTableUtils.CopyEntity<MTableEntity>(passedEntity));
                    else if (existingEntity.deleted)
                        newOp = TableOperation.Replace(ImportWithIfMatch(passedEntity, existingEntity.ETag));
                    else
                        errorCode = HttpStatusCode.Conflict;
                    break;
                case TableOperationType.Replace:
                    if ((errorCode = CheckExistingEntity(passedEntity, existingEntity)) == null)
                        newOp = TableOperation.Replace(ImportWithIfMatch(passedEntity, existingEntity.ETag));
                    break;
                case TableOperationType.Merge:
                    if ((errorCode = CheckExistingEntity(passedEntity, existingEntity)) == null)
                        newOp = TableOperation.Merge(ImportWithIfMatch(passedEntity, existingEntity.ETag));
                    break;
                case TableOperationType.Delete:
                    string buggablePartitionKey, buggableRowKey;
                    if (IsBugEnabled(MTableOptionalBug.DeletePrimaryKey))
                        buggablePartitionKey = buggableRowKey = null;
                    else
                    {
                        buggablePartitionKey = passedEntity.PartitionKey;
                        buggableRowKey = passedEntity.RowKey;
                    }
                    if (leaveTombstones)
                    {
                        if (passedEntity.ETag == ChainTable2Constants.ETAG_DELETE_IF_EXISTS)
                            newOp = TableOperation.InsertOrReplace(new MTableEntity {
                                PartitionKey = buggablePartitionKey, RowKey = buggableRowKey, deleted = true });
                        else if ((errorCode = CheckExistingEntity(passedEntity, existingEntity)) == null)
                            newOp = TableOperation.Replace(new MTableEntity {
                                PartitionKey = buggablePartitionKey, RowKey = buggableRowKey,
                                deleted = true, ETag = existingEntity.ETag });
                    }
                    else
                    {
                        if (passedEntity.ETag == ChainTable2Constants.ETAG_DELETE_IF_EXISTS)
                        {
                            if (existingEntity != null)
                                newOp = TableOperation.Delete(new MTableEntity {
                                    PartitionKey = buggablePartitionKey, RowKey = buggableRowKey,
                                    // It's OK to delete the entity and return success whether or not
                                    // the entity is a tombstone by the time it is actually deleted.
                                    ETag = IsBugEnabled(MTableOptionalBug.DeleteNoLeaveTombstonesETag) ? null : ChainTable2Constants.ETAG_ANY });
                            // Otherwise generate nothing.
                            // FIXME: This is not linearizable!  It can also generate empty batches.
                        }
                        else if ((errorCode = CheckExistingEntity(passedEntity, existingEntity)) == null)
                            // Another client in USE_NEW_WITH_TOMBSTONES could concurrently replace the
                            // entity with a tombstone, in which case we need to return 404 to the caller,
                            // hence this needs to be conditioned on the existing ETag.
                            newOp = TableOperation.Delete(new MTableEntity {
                                PartitionKey = buggablePartitionKey, RowKey = buggableRowKey,
                                ETag = IsBugEnabled(MTableOptionalBug.DeleteNoLeaveTombstonesETag) ? null : existingEntity.ETag });
                    }
                    break;
                case TableOperationType.InsertOrReplace:
                    newOp = TableOperation.InsertOrReplace(ChainTableUtils.CopyEntity<MTableEntity>(passedEntity));
                    break;
                case TableOperationType.InsertOrMerge:
                    newOp = TableOperation.InsertOrMerge(ChainTableUtils.CopyEntity<MTableEntity>(passedEntity));
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        async Task<IList<TableResult>> PassthroughBatchToOldTableAsync(TableBatchOperation batch, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            // XXX Make sure the caller is not trying to touch any of our internal rows.
            IList<TableResult> results = null;
            try
            {
                results = await oldTable.ExecuteBatchAsync(batch, requestOptions, operationContext);
                return results;
            }
            finally
            {
                await monitor.AnnotateLastBackendCallAsync(wasLinearizationPoint: true, successfulBatchResult: results);
            }
        }

        async Task<IList<TableResult>> AttemptBatchOnOldTableAsync(TableBatchOperation batch, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            string partitionKey = ChainTableUtils.GetBatchPartitionKey(batch);

            await TryMarkPartitionPopulatedAsync(partitionKey, requestOptions, operationContext);

            var oldBatch = new TableBatchOperation();
            oldBatch.Merge(new DynamicTableEntity {
                PartitionKey = partitionKey,
                RowKey = ROW_KEY_PARTITION_POPULATED_ASSERTION,
                ETag = ChainTable2Constants.ETAG_ANY,
            });
            // No AddRange? :(
            foreach (TableOperation op in batch)
                oldBatch.Add(op);
            IList<TableResult> oldResults;
            try
            {
                oldResults = await oldTable.ExecuteBatchAsync(oldBatch, requestOptions, operationContext);
            }
            catch (ChainTableBatchException ex)
            {
                if (ex.FailedOpIndex == 0)
                {
                    // This must mean the partition is switched.
                    await monitor.AnnotateLastBackendCallAsync();
                    return null;
                }
                else
                {
                    await monitor.AnnotateLastBackendCallAsync(wasLinearizationPoint: true);
                    throw ChainTableUtils.GenerateBatchException(ex.GetHttpStatusCode(), ex.FailedOpIndex - 1);
                }
            }
            oldResults.RemoveAt(0);
            await monitor.AnnotateLastBackendCallAsync(wasLinearizationPoint: true, successfulBatchResult: oldResults);
            return oldResults;
        }

        async Task<IList<TableResult>> ExecuteBatchOnNewTableAsync(MTableConfiguration config,
            TableBatchOperation batch, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            string partitionKey = ChainTableUtils.GetBatchPartitionKey(batch);

            await EnsurePartitionSwitchedAsync(partitionKey, requestOptions, operationContext);

            if (config.state <= TableClientState.PREFER_NEW)
                await EnsureAffectedRowsMigratedAsync(batch, requestOptions, operationContext);

            Attempt:
            // Batch on new table.
            var query = GenerateQueryForAffectedRows(batch);
            IList<MTableEntity> newRows = await newTable.ExecuteQueryAtomicAsync(query, requestOptions, operationContext);
            Dictionary<string, MTableEntity> newDict = newRows.ToDictionary(ent => ent.RowKey);
            // NOTE!  At this point, the read has not yet been annotated.  It is annotated below.

            var newBatch = new TableBatchOperation();
            var inputToNewTableIndexMapping = new List<int?>();
            for (int i = 0; i < batch.Count; i++)
            {
                TableOperation op = batch[i];
                ITableEntity passedEntity = op.GetEntity();
                MTableEntity existingEntity = newDict.GetValueOrDefault(passedEntity.RowKey);
                TableOperation newOp = null;
                HttpStatusCode? errorCode = null;

                TranslateOperationForNewTable(
                    op, existingEntity, config.state <= TableClientState.USE_NEW_WITH_TOMBSTONES,
                    ref newOp, ref errorCode);

                if (errorCode != null)
                {
                    Debug.Assert(newOp == null);
                    await monitor.AnnotateLastBackendCallAsync(wasLinearizationPoint: true);
                    throw ChainTableUtils.GenerateBatchException(errorCode.Value, i);
                }
                if (newOp != null)
                {
                    inputToNewTableIndexMapping.Add(newBatch.Count);
                    newBatch.Add(newOp);
                }
                else
                {
                    inputToNewTableIndexMapping.Add(null);
                }
            }
            await monitor.AnnotateLastBackendCallAsync();

            IList<TableResult> newResults;
            try
            {
                newResults = await newTable.ExecuteBatchAsync(newBatch, requestOptions, operationContext);
            }
            catch (ChainTableBatchException)
            {
                // XXX: Try to distinguish expected concurrency exceptions from unexpected exceptions?
                await monitor.AnnotateLastBackendCallAsync();
                goto Attempt;
            }

            // We made it!
            var results = new List<TableResult>();
            for (int i = 0; i < batch.Count; i++)
            {
                ITableEntity passedEntity = batch[i].GetEntity();
                int? newTableIndex = inputToNewTableIndexMapping[i];
                string newETag =
                    (IsBugEnabled(MTableOptionalBug.TombstoneOutputETag)
                    ? newTableIndex != null
                    : batch[i].GetOperationType() == TableOperationType.Delete)
                    ? null : newResults[newTableIndex.Value].Etag;
                if (newETag != null)
                    passedEntity.ETag = newETag;
                results.Add(new TableResult
                {
                    HttpStatusCode = (int)(
                        (batch[i].GetOperationType() == TableOperationType.Insert) ? HttpStatusCode.Created : HttpStatusCode.NoContent),
                    Etag = newETag,
                    Result = passedEntity,
                });
            }
            await monitor.AnnotateLastBackendCallAsync(wasLinearizationPoint: true, successfulBatchResult: results);
            return results;
        }

        public override async Task<IList<TableResult>> ExecuteBatchAsync(TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            MTableConfiguration config;
            using (configService.Subscribe(FixedSubscriber<MTableConfiguration>.Instance, out config))
            {

                if (config.state == TableClientState.USE_OLD_HIDE_METADATA)
                    return await PassthroughBatchToOldTableAsync(batch, requestOptions, operationContext);

                if (config.state == TableClientState.PREFER_OLD)
                {
                    // Either a non-null return or an exception means we completed the batch using the old table.
                    IList<TableResult> oldResults = await AttemptBatchOnOldTableAsync(batch, requestOptions, operationContext);
                    if (oldResults != null)
                        return oldResults;
                    // Otherwise fall through.
                }

                return await ExecuteBatchOnNewTableAsync(config, batch, requestOptions, operationContext);
            }
        }

        public override async Task<IList<TElement>> ExecuteQueryAtomicAsync<TElement>(TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            if (query.SelectColumns != null)
                throw new NotImplementedException("select");
            if (query.TakeCount != null)
                throw new NotImplementedException("top");
            FilterExpression origFilterExpr = ChainTableUtils.ParseFilterString(query.FilterString);
            string partitionKey = ChainTableUtils.GetSingleTargetedPartitionKey(origFilterExpr);
            TableQuery<MTableEntity> mtableQuery = ChainTableUtils.CopyQuery<TElement, MTableEntity>(query);

            MTableConfiguration config;
            using (configService.Subscribe(FixedSubscriber<MTableConfiguration>.Instance, out config))
            {
                IEnumerable<MTableEntity> results;
                if (config.state <= TableClientState.USE_OLD_HIDE_METADATA)
                {
                    results = await oldTable.ExecuteQueryAtomicAsync(mtableQuery, requestOptions, operationContext);
                    await monitor.AnnotateLastBackendCallAsync(wasLinearizationPoint: true);
                }
                else if (config.state >= TableClientState.USE_NEW_WITH_TOMBSTONES)
                {
                    results = await newTable.ExecuteQueryAtomicAsync(mtableQuery, requestOptions, operationContext);
                    await monitor.AnnotateLastBackendCallAsync(wasLinearizationPoint: true);
                }
                else
                {
                    // Modify the filter to make sure it matches the meta row.
                    if (origFilterExpr is ComparisonExpression)
                    {
                        // filterExpr must be "PartitionKey eq A", which already matches the meta row; nothing to do.
                    }
                    else
                    {
                        // filterExpr must be "(PartitionKey eq A) and (B)".
                        // Rewrite to "(PartitionKey eq A) and ((RowKey eq ROW_KEY_PARTITION_META) or (B))".
                        var boe = (BooleanOperatorExpression)origFilterExpr;
                        mtableQuery.FilterString =
                            TableQuery.CombineFilters(boe.Left.ToFilterString(), TableOperators.And,
                                TableQuery.CombineFilters(
                                    TableQuery.GenerateFilterCondition(TableConstants.RowKey, QueryComparisons.Equal, ROW_KEY_PARTITION_META),
                                    TableOperators.Or, boe.Right.ToFilterString()));
                    }
                    IList<MTableEntity> oldRows = await oldTable.ExecuteQueryAtomicAsync(mtableQuery, requestOptions, operationContext);
                    MTablePartitionState? state =
                        (from r in oldRows where r.RowKey == ROW_KEY_PARTITION_META select r.partitionState).SingleOrDefault();
                    IList<MTableEntity> newRows;
                    if (state == MTablePartitionState.SWITCHED)
                    {
                        await monitor.AnnotateLastBackendCallAsync();

                        // If the filter string includes conditions on user-defined properties,
                        // a row in the old table can be shadowed by a row in the new table
                        // that doesn't satisfy those conditions.  To make sure we retrieve all
                        // potential shadowing rows, retrieve the entire partition.
                        // XXX: At a minimum, we should try to keep conditions on the
                        // primary key, but how clever do we want to get with query rewriting?
                        if (!IsBugEnabled(MTableOptionalBug.QueryAtomicFilterShadowing))
                        {
                            mtableQuery.FilterString = TableQuery.GenerateFilterCondition(
                                TableConstants.PartitionKey, QueryComparisons.Equal, partitionKey);
                        }

                        newRows = await newTable.ExecuteQueryAtomicAsync(mtableQuery, requestOptions, operationContext);
                        await monitor.AnnotateLastBackendCallAsync(wasLinearizationPoint: true);
                    }
                    else
                    {
                        await monitor.AnnotateLastBackendCallAsync(wasLinearizationPoint: true);
                        newRows = new List<MTableEntity>();
                    }
                    // Merge lists.  Hopefully this is pretty clear.  Walking the lists
                    // in lockstep might be faster but is a lot more code.
                    var merged = new SortedDictionary<string, MTableEntity>(StringComparer.Ordinal);
                    foreach (MTableEntity ent in oldRows)
                    {
                        merged[ent.RowKey] = ent;
                    }
                    foreach (MTableEntity ent in newRows)
                    {
                        merged[ent.RowKey] = ent;
                    }
                    results = merged.Values;
                }
                return (from ent in results where !RowKeyIsInternal(ent.RowKey)
                        && origFilterExpr.Evaluate(ent) && !ent.deleted
                        select ent.Export<TElement>()).ToList();
            }
        }

        class QueryStream<TElement> : IQueryStream<TElement>
            where TElement : ITableEntity, new()
        {
            readonly MigratingTable outer;
            readonly FilterExpression origFilterExpr;
            readonly TableQuery<MTableEntity> mtableQuery;
            readonly TableRequestOptions requestOptions;
            readonly OperationContext operationContext;
            readonly AsyncLock theLock = new AsyncLock();
            IDisposable configSubscription;
            MTableConfiguration currentConfig;
            IQueryStream<MTableEntity> oldTableStream = null, newTableStream = null;
            MTableEntity oldTableNext = null, newTableNext = null;

            internal QueryStream(MigratingTable outer, TableQuery<TElement> query, TableRequestOptions requestOptions, OperationContext operationContext)
            {
                this.outer = outer;
                origFilterExpr = ChainTableUtils.ParseFilterString(query.FilterString);
                mtableQuery = ChainTableUtils.CopyQuery<TElement, MTableEntity>(query);
                this.requestOptions = requestOptions;
                this.operationContext = operationContext;
            }

            class DummyDisposable : IDisposable
            {
                public void Dispose() { }
            }

            Task<IDisposable> LockAsyncBuggable()
            {
                if (outer.IsBugEnabled(MTableOptionalBug.QueryStreamedLock))
                {
                    return Task.FromResult((IDisposable)new DummyDisposable());
                }
                else
                {
                    return theLock.LockAsync();
                }
            }

            // Boilerplate wrapper to prevent someone from downcasting the
            // IQueryStream to IConfigurationSubscriber. :/
            class Subscriber : IConfigurationSubscriber<MTableConfiguration>
            {
                readonly QueryStream<TElement> outer;
                internal Subscriber(QueryStream<TElement> outer)
                {
                    this.outer = outer;
                }
                public Task ApplyConfigurationAsync(MTableConfiguration newConfig)
                {
                    return outer.ApplyConfigurationAsync(newConfig);
                }
            }

            internal async Task StartAsync()
            {
                using (await LockAsyncBuggable())
                {
                    configSubscription = outer.configService.Subscribe(new Subscriber(this), out currentConfig);
                    if (currentConfig.state < TableClientState.USE_NEW_WITH_TOMBSTONES)
                    {
                        oldTableStream = await outer.oldTable.ExecuteQueryStreamedAsync(mtableQuery, requestOptions, operationContext);
                        oldTableNext = await oldTableStream.ReadRowAsync();
                    }
                    if (currentConfig.state > TableClientState.USE_OLD_HIDE_METADATA)
                    {
                        TableQuery<MTableEntity> newTableQuery =
                            outer.IsBugEnabled(MTableOptionalBug.QueryStreamedFilterShadowing) ? mtableQuery
                            // Yes, match everything (!).  See newTableContinuationQuery in ApplyConfigurationAsync.
                            : new TableQuery<MTableEntity>();
                        newTableStream = await outer.newTable.ExecuteQueryStreamedAsync(newTableQuery, requestOptions, operationContext);
                        newTableNext = await newTableStream.ReadRowAsync();
                    }
                }
            }

            // This isn't a true comparison because one stream could be inactive
            // now (meaning *TableNext == null) but become active later
            // depending on currentConfig.state.
            // 0: both sides.
            // >0: new table.
            // <0: old table.
            // null: end of stream.
            int? DetermineNextSide()
            {
                return (oldTableNext == null && newTableNext == null) ? (int?)null
                    : (oldTableNext == null) ? 1
                    : (newTableNext == null) ? -1
                    : oldTableNext.GetPrimaryKey().CompareTo(newTableNext.GetPrimaryKey());
            }

            PrimaryKey InternalGetContinuationPrimaryKey()
            {
                int? cmp = DetermineNextSide();
                return (cmp == null) ? null
                    : (cmp <= 0) ? oldTableNext.GetPrimaryKey()
                    : newTableNext.GetPrimaryKey();
            }

            public async Task<PrimaryKey> GetContinuationPrimaryKeyAsync()
            {
                using (await LockAsyncBuggable())
                {
                    CheckDisposed();
                    return InternalGetContinuationPrimaryKey();
                }
            }

            // XXX: This could loop a long time.  Do we want to make it cancelable?
            public async Task<TElement> ReadRowAsync()
            {
                using (await LockAsyncBuggable())
                {
                    CheckDisposed();
                    for (;;)
                    {
                        // Figure out which side has the next primary key to return (or
                        // both) and pull the row(s) with that key from one or both sides.
                        int? cmp = DetermineNextSide();
                        if (cmp == null) return default(TElement);
                        MTableEntity nextInOld = null, nextInNew = null;
                        if (cmp <= 0)
                        {
                            nextInOld = oldTableNext;
                            oldTableNext = await oldTableStream.ReadRowAsync();
                        }
                        if (cmp >= 0)
                        {
                            nextInNew = newTableNext;
                            newTableNext = await newTableStream.ReadRowAsync();
                        }
                        MTableEntity ent = (nextInNew != null) ? nextInNew : nextInOld;
                        // Compare to end of ExecuteQueryAtomicAsync.
                        if (!RowKeyIsInternal(ent.RowKey)
                            && origFilterExpr.Evaluate(ent) && !ent.deleted)
                            return ent.Export<TElement>();
                        // Otherwise keep going.
                    }
                }
            }

            async Task ApplyConfigurationAsync(MTableConfiguration newConfig)
            {
                using (await LockAsyncBuggable())
                {
                    PrimaryKey continuationKey = InternalGetContinuationPrimaryKey();
                    // ExecuteQueryStreamedAsync validated that mtableQuery has no select or top.
                    TableQuery<MTableEntity> newTableContinuationQuery =
                        (continuationKey == null) ? null : new TableQuery<MTableEntity>
                        {
                            // As in ExecuteQueryAtomicAsync, we have to retrieve all
                            // potential shadowing rows.
                            // XXX: This pays even a bigger penalty for not keeping
                            // conditions on the primary key.  But if we were to simply
                            // keep all such conditions, there's a potential to add
                            // more and more continuation filter conditions due to
                            // layering of IChainTable2s, which would lead to some extra
                            // overhead and push us closer to the limit on number of
                            // comparisons.  Either try to parse for this or change
                            // ExecuteQueryStreamedAsync to always take a continuation
                            // primary key?
                            FilterString =
                                outer.IsBugEnabled(MTableOptionalBug.QueryStreamedFilterShadowing)
                                ? ChainTableUtils.CombineFilters(
                                    ChainTableUtils.GenerateContinuationFilterCondition(continuationKey),
                                    TableOperators.And,
                                    mtableQuery.FilterString  // Could be empty.
                                    )
                                : ChainTableUtils.GenerateContinuationFilterCondition(continuationKey),
                        };
                    bool justStartedNewStream = false;

                    // Actually, if the query started in state
                    // USE_OLD_HIDE_METADATA, it is API-compliant to continue
                    // returning data from the old table until the migrator starts
                    // deleting it, at which point we switch to the new table.  But
                    // some callers may benefit from fresher data, even if it is
                    // never guaranteed, so we go ahead and start the new stream.
                    // XXX: Is this the right decision?
                    if (newConfig.state > TableClientState.USE_OLD_HIDE_METADATA
                        && currentConfig.state <= TableClientState.USE_OLD_HIDE_METADATA
                        && newTableContinuationQuery != null)
                    {
                        newTableStream = await outer.newTable.ExecuteQueryStreamedAsync(newTableContinuationQuery, requestOptions, operationContext);
                        newTableNext = await newTableStream.ReadRowAsync();
                        justStartedNewStream = true;
                    }

                    if (newConfig.state >= TableClientState.USE_NEW_HIDE_METADATA
                        && currentConfig.state < TableClientState.USE_NEW_HIDE_METADATA)
                    {
                        oldTableStream.Dispose();
                        oldTableStream = null;
                        oldTableNext = null;  // Stop DetermineNextSide from trying to read the old stream.
                        if (!outer.IsBugEnabled(MTableOptionalBug.QueryStreamedBackUpNewStream)
                            && newTableContinuationQuery != null && !justStartedNewStream)
                        {
                            // The new stream could have gotten ahead of the old
                            // stream if rows had not yet been migrated.  This
                            // was OK as long as we still planned to read those
                            // rows from the old stream, but now we have to back
                            // up the new stream to where the old stream was.
                            newTableStream.Dispose();
                            newTableStream = await outer.newTable.ExecuteQueryStreamedAsync(newTableContinuationQuery, requestOptions, operationContext);
                            newTableNext = await newTableStream.ReadRowAsync();
                        }
                    }
                    if (!outer.IsBugEnabled(MTableOptionalBug.QueryStreamedSaveNewConfig))
                        currentConfig = newConfig;
                }
            }

            private bool disposed = false;
            private void CheckDisposed()
            {
                if (disposed)
                    throw new ObjectDisposedException("MigratingTable QueryStream is disposed");
            }
            public async void Dispose()
            {
                // Given that I'm not willing to add the ability to break out of
                // ApplyConfigurationAsync in arbitrary places right now, I
                // don't think we can do any better than this.  I still wish I
                // were more confident in "asynchronous dispose" as a general
                // pattern that callers could learn to cope with.
                // XXX Prevent indefinite postponement?
                using (await LockAsyncBuggable())
                {
                    if (!disposed)
                    {
                        disposed = true;
                        configSubscription.Dispose();
                        if (oldTableStream != null)
                            oldTableStream.Dispose();
                        if (newTableStream != null)
                            newTableStream.Dispose();
                    }
                }
            }
        }

        public override async Task<IQueryStream<TElement>> ExecuteQueryStreamedAsync<TElement>(
            TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            if (query.SelectColumns != null)
                throw new NotImplementedException("select");
            if (query.TakeCount != null)
                throw new NotImplementedException("top");
            var stream = new QueryStream<TElement>(this, query, requestOptions, operationContext);
            await stream.StartAsync();
            return stream;
        }
    }
}
