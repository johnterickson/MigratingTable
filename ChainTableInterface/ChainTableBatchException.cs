// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace ChainTableInterface
{
    public class ChainTableBatchException : StorageException
    {
        public int FailedOpIndex { get; private set; }

        public ChainTableBatchException(int failedOpIndex, StorageException storageEx)
            : base(storageEx.RequestInformation,
                  string.Format("{0} at operation {1}", storageEx.Message, failedOpIndex),
                  storageEx.InnerException)
        {
            this.FailedOpIndex = failedOpIndex;
        }
    }
}
