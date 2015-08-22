// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using ChainTableInterface;
using System.Net;
using Microsoft.WindowsAzure.Storage.Table.Protocol;

namespace Migration
{
    public class MasterMigratingTable : MigratingTable
    {
        public static MTableConfiguration INITIAL_CONFIGURATION =
            new MTableConfiguration(TableClientState.USE_OLD_HIDE_METADATA);

        new IConfigurationService<MTableConfiguration> configService;

        public MasterMigratingTable(IConfigurationService<MTableConfiguration> configService,
            IChainTable2 oldTable, IChainTable2 newTable, IChainTableMonitor monitor,
            MTableOptionalBug? bugToEnable = null)
            : base(configService, oldTable, newTable, monitor, bugToEnable)
        {
            this.configService = configService;
        }

        async Task CopyAsync(TableRequestOptions requestOptions, OperationContext operationContext)
        {
            // Query all the entities!
            IQueryStream<MTableEntity> oldTableStream = await oldTable.ExecuteQueryStreamedAsync(
                new TableQuery<MTableEntity>(), requestOptions, operationContext);
            MTableEntity oldEntity;
            string previousPartitionKey = null;

            while ((oldEntity = await oldTableStream.ReadRowAsync()) != null)
            {
                if (RowKeyIsInternal(oldEntity.RowKey))
                    continue;

                if (oldEntity.PartitionKey != previousPartitionKey)
                {
                    previousPartitionKey = oldEntity.PartitionKey;
                    await EnsurePartitionSwitchedAsync(oldEntity.PartitionKey, requestOptions, operationContext);
                    if (IsBugEnabled(MTableOptionalBug.InsertBehindMigrator))
                    {
                        // More reasonable formulation of this bug.  If we're going
                        // to allow CopyAsync while some clients are still writing
                        // to the old table, give the developer credit for realizing
                        // that the stream might be stale by the time the partition
                        // is switched.
                        oldTableStream = await oldTable.ExecuteQueryStreamedAsync(
                            new TableQuery<MTableEntity> {
                                FilterString = TableQuery.GenerateFilterCondition(
                                    TableConstants.PartitionKey,
                                    QueryComparisons.GreaterThanOrEqual,
                                    oldEntity.PartitionKey)
                            }, requestOptions, operationContext);
                        continue;
                    }
                }

                // Can oldEntity be stale by the time we EnsurePartitionSwitchedAsync?
                // Currently no, because we don't start the copy until all clients have
                // entered PREFER_NEW state, meaning that all writes go to the new
                // table.  When we implement the Kstart/Kdone optimization (or similar),
                // then clients will be able to write to the old table in parallel with
                // our scan and we'll need to rescan after EnsurePartitionSwitchedAsync.

                // Should we also stream from the new table and copy only the
                // entities not already copied?
                await TryCopyEntityToNewTableAsync(oldEntity, requestOptions, operationContext);
            }
        }

        async Task CleanupAsync(TableRequestOptions requestOptions, OperationContext operationContext)
        {
            // Clean up MTable-specific data from new table.
            // Future: Query only ones with properties we need to clean up.
            IQueryStream<MTableEntity> newTableStream = await newTable.ExecuteQueryStreamedAsync(
                new TableQuery<MTableEntity>(), requestOptions, operationContext);
            MTableEntity newEntity;

            while ((newEntity = await newTableStream.ReadRowAsync()) != null)
            {
                // XXX: Consider factoring out this "query and retry" pattern
                // into a separate method.
                Attempt:
                TableOperation cleanupOp = newEntity.deleted ? TableOperation.Delete(newEntity)
                    : TableOperation.Replace(newEntity.Export<DynamicTableEntity>());
                TableResult cleanupResult;
                try
                {
                    cleanupResult = await newTable.ExecuteAsync(cleanupOp, requestOptions, operationContext);
                }
                catch (StorageException ex)
                {
                    if (ex.GetHttpStatusCode() == HttpStatusCode.NotFound)
                    {
                        // Someone else deleted it concurrently.  Nothing to do.
                        await monitor.AnnotateLastBackendCallAsync();
                        continue;
                    }
                    else if (ex.GetHttpStatusCode() == HttpStatusCode.PreconditionFailed)
                    {
                        await monitor.AnnotateLastBackendCallAsync();

                        // Unfortunately we can't assume that anyone who concurrently modifies
                        // the row while the table is in state USE_NEW_HIDE_METADATA will
                        // clean it up, because of InsertOrMerge.  (Consider redesign?)

                        // Re-retrieve row.
                        TableResult retrieveResult = await newTable.ExecuteAsync(
                            TableOperation.Retrieve<MTableEntity>(newEntity.PartitionKey, newEntity.RowKey),
                            requestOptions, operationContext);
                        await monitor.AnnotateLastBackendCallAsync();
                        if ((HttpStatusCode)retrieveResult.HttpStatusCode == HttpStatusCode.NotFound)
                            continue;
                        else
                        {
                            newEntity = (MTableEntity)retrieveResult.Result;
                            goto Attempt;
                        }
                    }
                    else
                    {
                        throw ChainTableUtils.GenerateInternalException(ex);
                    }
                }
                await monitor.AnnotateLastBackendCallAsync(
                    spuriousETagChanges:
                    cleanupResult.Etag == null ? null : new List<SpuriousETagChange> {
                        new SpuriousETagChange(newEntity.PartitionKey, newEntity.RowKey, cleanupResult.Etag) });
            }

            // Delete everything from old table!  No one should be modifying it concurrently.
            IQueryStream<MTableEntity> oldTableStream = await oldTable.ExecuteQueryStreamedAsync(
                new TableQuery<MTableEntity>(), requestOptions, operationContext);
            MTableEntity oldEntity;
            while ((oldEntity = await oldTableStream.ReadRowAsync()) != null)
            {
                await oldTable.ExecuteAsync(TableOperation.Delete(oldEntity), requestOptions, operationContext);
                await monitor.AnnotateLastBackendCallAsync();
            }
        }

        // In theory, if the migrator dies, you should be able to call this
        // again (using the same configuration service!) to resume.
        // XXX: Remember how far we got through a copy or cleanup pass.
        public async Task MigrateAsync(TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            MTableConfiguration config;
            configService.Subscribe(FixedSubscriber<MTableConfiguration>.Instance, out config).Dispose();
            // XXX: Ideally we'd lock out other masters somehow.

            StateSwitch:
            switch (config.state)
            {
                case TableClientState.USE_OLD_HIDE_METADATA:
                    if (IsBugEnabled(MTableOptionalBug.MigrateSkipPreferOld))
                        goto case TableClientState.PREFER_OLD;
                    config = new MTableConfiguration(TableClientState.PREFER_OLD);
                    await configService.PushConfigurationAsync(config);
                    goto StateSwitch;

                case TableClientState.PREFER_OLD:
                    if (IsBugEnabled(MTableOptionalBug.InsertBehindMigrator))
                        goto case TableClientState.PREFER_NEW;
                    config = new MTableConfiguration(TableClientState.PREFER_NEW);
                    await configService.PushConfigurationAsync(config);
                    goto StateSwitch;

                case TableClientState.PREFER_NEW:
                    await CopyAsync(requestOptions, operationContext);
                    if (IsBugEnabled(MTableOptionalBug.MigrateSkipUseNewWithTombstones))
                        goto case TableClientState.USE_NEW_WITH_TOMBSTONES;
                    config = new MTableConfiguration(TableClientState.USE_NEW_WITH_TOMBSTONES);
                    await configService.PushConfigurationAsync(config);
                    goto StateSwitch;

                case TableClientState.USE_NEW_WITH_TOMBSTONES:
                    config = new MTableConfiguration(TableClientState.USE_NEW_HIDE_METADATA);
                    await configService.PushConfigurationAsync(config);
                    goto StateSwitch;

                case TableClientState.USE_NEW_HIDE_METADATA:
                    await CleanupAsync(requestOptions, operationContext);
                    break;
            }
        }
    }
}
