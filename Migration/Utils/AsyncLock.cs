// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Migration
{
    // Compare to http://blogs.msdn.com/b/pfxteam/archive/2012/02/12/10266988.aspx.
    // We could consider incorporating some of the optimizations from there or
    // looking for another library to adopt.
    public class AsyncLock
    {
        // Alternatively, we could use null when the lock is un-held.
        Task holdTask = Task.CompletedTask;

        class Releaser : IDisposable
        {
            TaskCompletionSource<object> tcs;
            internal Releaser(TaskCompletionSource<object> tcs)
            {
                this.tcs = tcs;
            }
            public void Dispose()
            {
                if (tcs != null)
                {
                    tcs.SetResult(null);
                    tcs = null;
                }
            }
        }

        // WARNING: Task is disposable so if the caller forgets "await", it will
        // still compile but won't actually wait for the lock!
        // XXX Pass through a CancellationToken when we're ready to add
        // cancellation support to MigratingTable.
        public async Task<IDisposable> LockAsync()
        {
            await holdTask;
            var tcs = new TaskCompletionSource<object>();
            holdTask = tcs.Task;
            return new Releaser(tcs);
        }
    }
}
