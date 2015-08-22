// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using System;
using System.Threading;

namespace Migration
{
    class SynchronizationContextUse : IDisposable
    {
        SynchronizationContext sc, previous;
        internal SynchronizationContextUse(SynchronizationContext sc)
        {
            if (sc == null) throw new ArgumentNullException(nameof(sc));
            this.sc = sc;
            previous = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(sc);
        }

        public void Dispose()
        {
            if (sc != null)  // detect redundant calls
            {
                if (SynchronizationContext.Current != sc)
                    throw new InvalidOperationException("The SynchronizationContext we are trying to deactivate is no longer current.");
                SynchronizationContext.SetSynchronizationContext(previous);
                sc = previous = null;
            }
        }
    }

    public static class SynchronizationContextUseStatics
    {
        // Intended for use in a using statement.
        public static IDisposable AsCurrent(this SynchronizationContext sc)
        {
            return new SynchronizationContextUse(sc);
        }
    }
}
