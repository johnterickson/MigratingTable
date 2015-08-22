// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ChainTableInterface
{
    public static class AzureTableAccessors
    {
        // C# doesn't support extension properties.  Defining an extension method named
        // "get_OperationType" does not work either because it doesn't have the "specialname"
        // MSIL attribute, and even so, I don't know if the method lookup done by a property
        // lookup supports extension methods.

        private static readonly PropertyInfo TableOperation_OperationType =
            typeof(TableOperation).GetProperty("OperationType",
                BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.NonPublic);

        public static TableOperationType GetOperationType(this TableOperation operation)
        {
            return (TableOperationType)TableOperation_OperationType.GetValue(operation, null);
        }

        private static readonly PropertyInfo TableOperation_Entity =
            typeof(TableOperation).GetProperty("Entity",
                BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.NonPublic);

        public static ITableEntity GetEntity(this TableOperation operation)
        {
            return (ITableEntity)TableOperation_Entity.GetValue(operation, null);
        }

        private static readonly PropertyInfo TableOperation_RetrievePartitionKey =
            typeof(TableOperation).GetProperty("RetrievePartitionKey",
                BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.NonPublic);
        public static string GetRetrievePartitionKey(this TableOperation operation)
        {
            return (string)TableOperation_RetrievePartitionKey.GetValue(operation, null);
        }

        private static readonly PropertyInfo TableOperation_RetrieveRowKey =
            typeof(TableOperation).GetProperty("RetrieveRowKey",
                BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.NonPublic);
        public static string GetRetrieveRowKey(this TableOperation operation)
        {
            return (string)TableOperation_RetrieveRowKey.GetValue(operation, null);
        }

        private static readonly PropertyInfo TableOperation_RetrieveResolver =
            typeof(TableOperation).GetProperty("RetrieveResolver",
                BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.NonPublic);
        public static Func<string, string, DateTimeOffset, IDictionary<string, EntityProperty>, string, object> GetRetrieveResolver(TableOperation operation)
        {
            return (Func<string, string, DateTimeOffset, IDictionary<string, EntityProperty>, string, object>)
                TableOperation_RetrieveResolver.GetValue(operation, null);
        }
    }
}
