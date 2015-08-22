// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace ChainTableInterface
{
    public static class ChainTable2Constants
    {
        public const string ETAG_ANY = "*";  // Azure client library doesn't use a constant for this.
        public const string ETAG_DELETE_IF_EXISTS = "**";
    }

    // Interface used by the migration library, intended to converge with IChainTable eventually.
    public interface IChainTable2
    {
        /*
         * WARNING: Like IChainTable, this interface uses optional parameters.
         * Strange behavior that I don't fully understand can occur if a method
         * overrides a superclass method or implements an interface method and
         * doesn't specify the same optional parameters, or in certain separate
         * compilation scenarios.  See
         * http://stackoverflow.com/questions/8909811/c-sharp-optional-parameters-on-overridden-methods
         * for some information (not verified to be up to date).  Duplicating
         * the default values in subclasses is still less work than defining
         * different overloads.
         */

        // XXX: Existing practice (at least in STable) seems to be to pass the
        // same requestOptions and operationContext down to all the outgoing
        // calls and not worry about them for ReadEntity/WriteEntity.  Is this
        // actually sensible?  Should we consider stashing them in the
        // ExecutionContext to reduce boilerplate?

        // All methods can also throw StorageException if they get an unexpected
        // result or exception from a backend table.

        /*
         * Execute API:
         *   (1) Retrieve
         *          On success, return TableResult, code = 200 (OK);
         *          If the row does not exist, return TableResult, code = 404 (NotFound);
         *          Otherwise, throw storage exception with corresponding http error code.
         *   (2) Insert
         *          On success, return TableResult, code = 201 (Created);
         *          If the row already exists, throw storage exception, code = 409 (Conflict);
         *          Otherwise, throw storage exception with corresponding http error code.
         *   (3) Replace / Merge / Delete
         *          On success, return TableResult, code = 204 (NoContent);
         *          If the row does not exist, throw storage exception, code = 404 (NotFound);
         *          If ETag mismatches, throw storage exception, code = 412 (Precondition);
         *          Otherwise, throw storage exception with corresponding http error code.
         *   (4) InsertOrReplace / InsertOrMerge
         *          On success, return TableResult, code = 204 (NoContent);
         *          Otherwise, throw storage exception with corresponding http error code.
         *          XXX: Does real Azure return 201?  If applications rely on it,
         *          emulating it would have some cost for some table layers.
         *   (5) DeleteIfExists (represented as Delete with ETag = ETAG_DELETE_IF_EXISTS)
         *          On success, return TableResult, code = 204 (NoContent);
         *          Otherwise, throw storage exception with corresponding http error code.
         *          XXX: Do we expect implementations that don't use tombstones to emulate
         *          this (inefficiently) or just throw NotImplementedException?
         */
        Task<TableResult> ExecuteAsync(TableOperation operation,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null);

        /*
         * Batch Execute API:
         *   Caller should not put retrieve operations inside a batch, so any error will lead to
         *   an Exception.
         *   
         *   On success, return a list of TableResult. Each TableResult should follow the API
         *   defined in Execute operation.  Also update the ETags of the ITableEntity objects
         *   passed in (except for Delete).
         *   
         *   Otherwise, throw a ChainTableBatchException, with failedOpIndex marks to the first
         *   operation that causes the failure, and storageEx being the corresponding exception
         *   defined in the Execute API.  None of the ETags of the ITableEntity objects are
         *   updated.
         */
        // XXX Factor out code to check for multiple operations against the same key?
        // (Some of the validation is already available in ChainTableUtils.GetBatchPartitionKey.)
        Task<IList<TableResult>> ExecuteBatchAsync(TableBatchOperation batch,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null);

        /*
         * Query interface:
         */

        /*
         * Must target a single partition as per GetSingleTargetedPartitionKey (if necessary,
         * we can make GetSingleTargetedPartitionKey smarter).  Fails if the query takes too
         * long or generates too large a result set to process atomically.  The types of
         * queries that can be assumed to be under the limits are implementation-defined.
         *
         * The verification framework is currently relying on the result being an instance of
         * IReadOnlyList<TableResult> so that BetterComparer knows to do an ordered comparison.
         * If there are stronger arguments for another return type if/when IChainTable2 merges
         * with IChainTable, we can find a workaround for the verification framework.
         *
         * XXX: Document how to tell whether this failed because the query exceeded the limits.
         */
        Task<IList<TElement>> ExecuteQueryAtomicAsync<TElement>(TableQuery<TElement> query,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            where TElement : ITableEntity, new();
        /*
         * Each row may be read at a different time between the start of the
         * query and when the row is returned.  These times are not guaranteed
         * to be an increasing function of the primary key.
         *
         * It's unclear whether we're better off having another "Async" here.
         * It might allow some implementations to extend useful guarantees that
         * certain kinds of errors will be caught on the initial
         * ExecuteQueryStreamed, but it might make some caller code more
         * complex.  See what I think when I implement this in MigratingTable.
         * The model doesn't call this at all for the time being.
         *
         * The caller should dispose the returned stream.  This unfortunate(?)
         * requirement is passed up from MigratingTable, whose stream holds a
         * subscription to an IConfigurationService.  XXX: Look for a better
         * solution when merging with IChainTable.
         */
        Task<IQueryStream<TElement>> ExecuteQueryStreamedAsync<TElement>(TableQuery<TElement> query,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            where TElement : ITableEntity, new();

        // Holdovers from IChainTable that we don't implement or use.
        bool IsReadOnly(OperationContext operationContext);
        string GetTableID();
        bool CreateIfNotExists();
        bool DeleteIfExists();
    }

    // XXX: Use IAsyncEnumerable from http://asyncenum.codeplex.com/ or similar?
    // A big dependency to take for a small thing.
    public interface IQueryStream<TElement> : IDisposable
        where TElement : ITableEntity, new()
    {
        // Task result is null if no more.  Don't make more than one concurrent call.
        Task<TElement> ReadRowAsync();

        /*
         * A primary key from which the query can be restarted (see
         * ChainTableUtils.GenerateContinuationFilterCondition) to get a
         * complete result set.  It is always greater than the primary key of
         * the last row returned, if any.  This query stream may have buffered
         * data beyond the returned primary key, so restarting the query may
         * have a cost compared to continuing to read the same stream.
         * XXX: Queries with top are not as trivial to restart as I thought...
         *
         * Null if the result set is done (i.e., the next task result from
         * GetNextRowAsync would be null).
         */
        Task<PrimaryKey> GetContinuationPrimaryKeyAsync();

        // Dispose should not be called concurrently with the other methods.
        // XXX: Find a good general way to control concurrency of async methods?
    }
}
