// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migration
{
    public class SpuriousETagChange
    {
        public readonly string partitionKey;
        public readonly string rowKey;
        public readonly string newETag;
        public SpuriousETagChange(string partitionKey, string rowKey, string newETag)
        {
            this.partitionKey = partitionKey;
            this.rowKey = rowKey;
            this.newETag = newETag;
        }
    }

    public interface IChainTableMonitor
    {
        /*
         * The backend and monitor calls by verifiable table layers should follow this sequence
         * (this does not preclude other local work from being done in parallel):
         *
         *  (
         *    await backend call
         *    await AnnotateLastOutgoingCallAsync
         *  )*
         *
         * This applies even if the backend call throws an exception that is
         * part of the semantics.  The expected handling of unexpected
         * exceptions is not clearly defined.
         *
         * XXX: Have the verification framework enforce this automatically?
         */

        /*
         * If the current incoming call is an ExecuteBatchAsync that is going to
         * succeed, then successfulBatchResult must be its result, including the
         * new ETags.  This allows the verification harness to mirror the batch on
         * the reference table using the same new ETags before unlocking the
         * TablesMachine for other ServiceMachines.  (The verification does not
         * use ExecuteAsync.)
         *
         * In all other cases, successfulBatchResult must be null.
         *
         * For now, the verification does not deal with ExecuteQueryStreamed at
         * all.  ExecuteQueryStreamed backend calls should not be annotated.  A
         * verifiable table layer's implementation of ExecuteQueryStreamed
         * should not report a linearization point, but it should annotate
         * backend calls (other than ExecuteQueryStreamed) as normal.
         *
         * Passing one or more spurious ETag changes in combination with
         * wasLinearizationPoint = true is disallowed until we have a use case
         * to decide what its semantics should be.
         */
        Task AnnotateLastBackendCallAsync(
            bool wasLinearizationPoint = false,
            IList<TableResult> successfulBatchResult = null,
            IList<SpuriousETagChange> spuriousETagChanges = null);
    }
}
