// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using ChainTableInterface;

namespace Migration
{
    enum MTablePartitionState
    {
        //POPULATING, // Used by Kstart optimization, not yet implemented.
        POPULATED,
        SWITCHED
    }

    class MTableEntity : TableEntity
    {
        // Extra fields for the partition meta row (only)
        internal const string KEY_PARTITION_STATE = "_mtable_partition_state";
        internal MTablePartitionState? partitionState = null;

        // Extra fields for ordinary rows
        internal const string KEY_DELETED = "_mtable_deleted";
        internal bool deleted = false;

        internal IDictionary<string, EntityProperty> userProperties = new Dictionary<string, EntityProperty>();

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            EntityProperty propDeleted;
            // N.B. The cast will throw an exception if BooleanValue is null,
            // but I think such properties are not supposed to be passed to ReadEntity.
            deleted = properties.TryGetValue(KEY_DELETED, out propDeleted) ? (bool)propDeleted.BooleanValue : false;

            EntityProperty propPartitionState;
            // XXX Validate?  Use the name of the enumeration constant as the serialized form?
            partitionState = properties.TryGetValue(KEY_PARTITION_STATE, out propPartitionState)
                ? (MTablePartitionState?)propPartitionState.Int32Value : null;

            // If the rest of the code is prepared for DynamicTableEntity.ReadEntity to
            // mutate the passed dictionary, we may as well do it too.
            properties.Remove(KEY_DELETED);
            properties.Remove(KEY_PARTITION_STATE);
            userProperties = properties;
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            // Partial aliasing, no worse than DynamicTableEntity.WriteEntity.
            var properties = new Dictionary<string, EntityProperty>(userProperties);
            // TODO: Optimization: don't output "deleted: false" except in InsertOrMerge case
            // (to reduce the number of rows that have to be modified in post-migration cleanup).
            properties.Add(KEY_DELETED, new EntityProperty(deleted));
            if (partitionState != null)
                properties.Add(KEY_PARTITION_STATE, new EntityProperty((int)partitionState.Value));
            return properties;
        }

        internal TElement Export<TElement>() where TElement : ITableEntity, new()
        {
            var ent2 = new TElement()
            {
                PartitionKey = PartitionKey,
                RowKey = RowKey,
                ETag = ETag,
                Timestamp = Timestamp,
            };
            // This copy probably isn't needed in the common case,
            // but I'd rather not even have to think about it.
            ent2.ReadEntity(ChainTableUtils.CopyPropertyDict(userProperties), null);
            return ent2;
        }
    }
}
