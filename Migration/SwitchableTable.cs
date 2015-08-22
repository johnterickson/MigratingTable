// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using ChainTableInterface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading;

namespace Migration
{
    // Because C# doesn't support delegates with type parameters on the _method_
    // (as opposed to the delegate type).
    public interface QueryStreamMapper
    {
        IQueryStream<TElement> Map<TElement>(IQueryStream<TElement> oldStream, TableQuery<TElement> query,
            TableRequestOptions requestOptions, OperationContext operationContext) where TElement : ITableEntity, new();
    }

    public class SwitchableTable : AbstractChainTable2
    {
        // Public get on this doesn't seem any more harmful than public
        // SwitchAsync.  If you really want to defend against clients
        // downcasting things, you need a dedicated wrapper.
        public IChainTable2 backend { get; internal set; }

        public SwitchableTable(IChainTable2 backend)
        {
            this.backend = backend;
        }

        public override Task<IList<TableResult>> ExecuteBatchAsync(TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public override Task<IList<TElement>> ExecuteQueryAtomicAsync<TElement>(TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public override Task<IQueryStream<TElement>> ExecuteQueryStreamedAsync<TElement>(TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        /* This may be called concurrently with other async operations, but not with itself.
         * It doesn't return until all in-flight operations on the old backend have finished.
         * TODO: Add cancellation support to IChainTable2 so we can cancel long-running
         * operations on the old table and complete the switch faster?  (We'd stipulate that
         * a batch that is cancelled but takes effect must return the success rather than the
         * cancellation, at least until we find a way to safely retry a batch that ended in
         * an unknown state.) */
        public Task SwitchAsync(IChainTable2 newBackend, QueryStreamMapper queryStreamMapper)
        {
            throw new NotImplementedException();
        }
    }
}
