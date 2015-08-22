// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using System;
using System.Threading.Tasks;
using Microsoft.PSharp;

namespace Migration
{
    class ReplyTarget<TResult>
    {
        readonly object replyId;
        readonly MachineId machineId;
        internal readonly TaskCompletionSource<TResult> tcs;
        bool replied = false;
        public ReplyTarget(object replyId, MachineId machineId)
        {
            this.replyId = replyId;
            this.machineId = machineId;
            tcs = new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        public void SetOutcome(Outcome<TResult, Exception> outcome)
        {
            if (replied) throw new InvalidOperationException();
            PSharpRuntime.SendEvent(machineId, new GenericDispatchableEvent(), new ReplyPayload<TResult>(tcs, outcome));
            replied = true;
        }
    }

    class ReplyPayload<TResult> : IDispatchable
    {
        readonly TaskCompletionSource<TResult> tcs;
        readonly Outcome<TResult, Exception> outcome;

        internal ReplyPayload(TaskCompletionSource<TResult> tcs, Outcome<TResult, Exception> outcome)
        {
            this.tcs = tcs;
            this.outcome = outcome;
        }

        public void Dispatch()
        {
            tcs.SetOutcome(outcome);
        }
    }
}
