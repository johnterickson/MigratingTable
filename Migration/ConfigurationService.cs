// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migration
{
    // Configuration service with the ability to block on subscribers to
    // acknowledge a new configuration.
    public interface IReadOnlyConfigurationService<TConfig>
    {
        // Warning!  Currently the configuration service keeps a strong
        // reference to the subscriber, so the object owning the subscriber must
        // know its own intended lifetime (e.g., by implementing IDisposable
        // itself) and dispose the subscription accordingly to avoid leaking
        // subscriptions.  I looked into making subscribers weakly referenced,
        // but it quickly became complicated.  XXX: Reconsider as a low
        // priority.
        IDisposable Subscribe(IConfigurationSubscriber<TConfig> subscriber, out TConfig currentConfig);
    }

    public interface IConfigurationService<TConfig> : IReadOnlyConfigurationService<TConfig>
    {
        // No concurrent calls please.  (For now; we could add support later.)
        Task PushConfigurationAsync(TConfig newConfig);
    }

    // We could use IObserver if the data item itself included a way for the
    // recipient to acknowledge it, but I don't see the benefit outweighing the
    // awkwardness.  We could use a delegate, which would save clients some
    // boilerplate at the expense of clarity.
    public interface IConfigurationSubscriber<TConfig>
    {
        // It's OK if the subscriber unsubscribes first, in which case it
        // doesn't matter if the task returned by this method ever completes.
        Task ApplyConfigurationAsync(TConfig newConfig);
    }

    // Subscriber that never accepts updates, so updates can only be unblocked
    // by its unsubscription.
    public class FixedSubscriber<TConfig> : IConfigurationSubscriber<TConfig>
    {
        private FixedSubscriber() { }
        public static readonly FixedSubscriber<TConfig> Instance = new FixedSubscriber<TConfig>();
        private static readonly Task eternalTask = new TaskCompletionSource<object>().Task;

        public Task ApplyConfigurationAsync(TConfig newConfig)
        {
            return eternalTask;
        }
    }

    // For now, this is for modeling only.
    // XXX: Make this extensible so the configuration can be persisted?
    public class InMemoryConfigurationService<TConfig> : IConfigurationService<TConfig>
    {
        class Subscription : IDisposable
        {
            internal readonly InMemoryConfigurationService<TConfig> service;
            internal readonly IConfigurationSubscriber<TConfig> subscriber;
            // http://stackoverflow.com/questions/11969208/non-generic-taskcompletionsource-or-alternative
            // http://referencesource.microsoft.com/#mscorlib/system/void.cs,0325269a62139eb0
            internal readonly TaskCompletionSource<object> unsubscribeTcs = new TaskCompletionSource<object>();

            internal Subscription(InMemoryConfigurationService<TConfig> service, IConfigurationSubscriber<TConfig> subscriber)
            {
                this.service = service;
                this.subscriber = subscriber;
            }

            public void Dispose()
            {
                // Safe for redundant calls.
                service.subscriptions.Remove(this);
                unsubscribeTcs.TrySetResult(null);
            }
        }

        TConfig currentConfig;
        readonly HashSet<Subscription> subscriptions = new HashSet<Subscription>();

        public InMemoryConfigurationService(TConfig initialConfig)
        {
            currentConfig = initialConfig;
        }

        public Task PushConfigurationAsync(TConfig newConfig)
        {
            currentConfig = newConfig;
            return Task.WhenAll(
                from s in subscriptions.ToList()  // Make sure to take a snapshot.
                select Task.WhenAny(s.subscriber.ApplyConfigurationAsync(newConfig), s.unsubscribeTcs.Task));
        }

        public IDisposable Subscribe(IConfigurationSubscriber<TConfig> subscriber, out TConfig currentConfig)
        {
            if (subscriber == null) throw new ArgumentNullException(nameof(subscriber));
            var subscription = new Subscription(this, subscriber);
            subscriptions.Add(subscription);
            currentConfig = this.currentConfig;
            return subscription;
        }
    }
}
