// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using System;
using Microsoft.PSharp;
using System.Threading;

namespace Migration
{
    // Common interface for payloads so they can be handled by a single machine action.
    // We could use a delegate, but it may be nice to introspect the fields.
    interface IDispatchable
    {
        void Dispatch();
    }

    class SynchronizationContextWorkUnit : IDispatchable
    {
        readonly SendOrPostCallback callback;
        readonly object arg;
        internal SynchronizationContextWorkUnit(SendOrPostCallback callback, object arg)
        {
            this.callback = callback;
            this.arg = arg;
        }
        public void Dispatch()
        {
            callback(arg);
        }
    }
    // Hint: [OnEventDoAction(typeof(GenericDispatchableEvent), nameof(DispatchPayload))]
    // "Generic" in the common sense, not C# "generic".
    class GenericDispatchableEvent : Event { }
    // Hint: Create only one per machine so that the TPL runs work units inline
    // if the same synchronization context is already active.
    class MachineSynchronizationContext : SynchronizationContext
    {
        readonly MachineId machineId;
        public MachineSynchronizationContext(MachineId machineId)
        {
            this.machineId = machineId;
        }
        public override void Send(SendOrPostCallback d, object state)
        {
            // Not supported.  Apparently Send is only used for cancellation callbacks.
            throw new NotSupportedException();
        }
        public override void Post(SendOrPostCallback d, object state)
        {
            PSharpRuntime.SendEvent(machineId, new GenericDispatchableEvent(), new SynchronizationContextWorkUnit(d, state));
        }
    }
}
