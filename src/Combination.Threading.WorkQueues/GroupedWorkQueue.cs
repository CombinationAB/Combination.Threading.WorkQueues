using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Combination.Threading.WorkQueues
{
    /// <summary>
    /// Options used with the GroupWorkQueue.
    /// </summary>
    public sealed class GroupedWorkQueueOptions
    {
        /// <summary>
        /// Number of groups that can be processed in parallel.
        /// </summary>
        public int Parallelism { get; set; } = 1;

        /// <summary>
        /// For how long no new updates are to be sent to a group for considering processing it. Set to zero or negative for no cooldown (a group is always eligible).
        /// </summary>
        public TimeSpan Cooldown { get; set; } = TimeSpan.FromMilliseconds(100);

        /// <summary>
        /// Cooldown timeout. If this timeout passes, a group is processed even if cooldown keeps happening. Set to zero or negative for no timeout.
        /// </summary>
        public TimeSpan Timeout { get; set; } = TimeSpan.Zero;
    }

    public static class GroupedWorkQueue
    {
        public static GroupedWorkQueue<TKey, TValue, TAgg> Create<TKey, TValue, TAgg>(
            GroupedWorkQueue<TKey, TValue, TAgg>.ProcessorCallback processor,
            Func<TAgg, TValue, TAgg> aggregation,
            Func<TAgg> initial,
            GroupedWorkQueueOptions options = null)
            => new GroupedWorkQueue<TKey, TValue, TAgg>(processor, aggregation, initial,
                options ?? new GroupedWorkQueueOptions());

        public static GroupedWorkQueue<TKey, TValue, (bool, TValue)> First<TKey, TValue>(
            GroupedWorkQueue<TKey, TValue, TValue>.ProcessorCallback processor,
            GroupedWorkQueueOptions options = null)
            => new GroupedWorkQueue<TKey, TValue, (bool, TValue)>(
                (k, v) => processor(k, v.Item2), 
                (prev, val) => prev.Item1 ? prev : (true, val), 
                () => (false, default),
                options ?? new GroupedWorkQueueOptions());

        public static GroupedWorkQueue<TKey, TValue, TValue> Last<TKey, TValue>(
            GroupedWorkQueue<TKey, TValue, TValue>.ProcessorCallback processor,
            GroupedWorkQueueOptions options = null)
            => new GroupedWorkQueue<TKey, TValue, TValue>(processor, (_, val) => val, () => default,
                options ?? new GroupedWorkQueueOptions());

        public static GroupedWorkQueue<TKey, TValue> All<TKey, TValue>(
            GroupedWorkQueue<TKey, TValue>.ProcessorCallback processor,
            GroupedWorkQueueOptions options = null)
            => new GroupedWorkQueue<TKey, TValue>(processor, options ?? new GroupedWorkQueueOptions());
    }

    public sealed class GroupedWorkQueue<TKey, TValue> : GroupedWorkQueue<TKey, TValue, IEnumerable<TValue>>
    {
        private volatile int currentLength;

        public GroupedWorkQueue(ProcessorCallback processor)
            : base(processor, EnumAdd, NewList)
        {
        }

        public GroupedWorkQueue(ProcessorCallback processor, int parallelism)
            : base(processor, EnumAdd, NewList, parallelism)
        {
        }

        public GroupedWorkQueue(ProcessorCallback processor, GroupedWorkQueueOptions options)
            : base(processor, EnumAdd, NewList, options)
        {
        }

        private static IEnumerable<TValue> EnumAdd(IEnumerable<TValue> previous, TValue value)
        {
            ((List<TValue>) previous).Add(value);
            return previous;
        }

        private static IEnumerable<TValue> NewList()
            => new List<TValue>();

        /// <summary>
        /// Number of values currently in the queue.
        /// </summary>
        public int Count => currentLength;

        protected override void OnRemoved(Entry entry)
        {
            Interlocked.Add(ref currentLength, -((List<TValue>) entry.Aggregate).Count);
        }

        protected override void OnAdded(Entry entry)
        {
            Interlocked.Increment(ref currentLength);
        }
    }

    public class GroupedWorkQueue<TKey, TValue, TAgg>
    {
        private readonly Dictionary<TKey, Entry> pending = new Dictionary<TKey, Entry>();
        private readonly ProcessorCallback processor;
        private readonly Func<TAgg, TValue, TAgg> aggregation;
        private readonly Func<TAgg> initial;
        private volatile bool stopped;
        private readonly Stopwatch timer = Stopwatch.StartNew();
        private readonly Task runLoopTask;
        private readonly BufferBlock<Entry> queue = new BufferBlock<Entry>();
        private readonly TimeSpan timeout, cooldown, idleDelay;
        private readonly CancellationTokenSource cancel = new CancellationTokenSource();

        public delegate Task ProcessorCallback(TKey key, TAgg aggregation);

        public GroupedWorkQueue(ProcessorCallback processor, Func<TAgg, TValue, TAgg> aggregation, Func<TAgg> initial)
            : this(processor, aggregation, initial, new GroupedWorkQueueOptions())
        {
        }

        public GroupedWorkQueue(ProcessorCallback processor, Func<TAgg, TValue, TAgg> aggregation, Func<TAgg> initial,
            int parallelism)
            : this(processor, aggregation, initial, new GroupedWorkQueueOptions {Parallelism = parallelism})
        {
        }

        public GroupedWorkQueue(ProcessorCallback processor, Func<TAgg, TValue, TAgg> aggregation, Func<TAgg> initial,
            GroupedWorkQueueOptions options)
        {
            if (options == null) options = new GroupedWorkQueueOptions();
            this.processor = processor;
            this.aggregation = aggregation;
            this.initial = initial;
            cooldown = options.Cooldown;
            idleDelay = TimeSpan.FromMilliseconds(cooldown.TotalMilliseconds >= 100 ? 10 : 1);

            timeout = options.Timeout;
            runLoopTask = Task.WhenAll(Enumerable.Range(0, options.Parallelism).Select(_ => Task.Run(ProcessLoop)));
        }

        private async Task ProcessLoop()
        {
            while (!stopped || queue.Count > 0)
            {
                try
                {
                    await queue.OutputAvailableAsync(cancel.Token).ConfigureAwait(false);
                }
                catch (TaskCanceledException)
                {
                }

                if (stopped && queue.Count == 0) break;
                var now = timer.Elapsed;
                Entry entry;
                var didFindEntry = false;
                lock (pending)
                {
                    if (queue.TryReceive(x => x.ReadyTime <= now, out entry))
                    {
                        pending.Remove(entry.Key);
                        didFindEntry = true;
                    }
                }

                if (didFindEntry)
                {
                    OnRemoved(entry);
                    await processor(entry.Key, entry.Aggregate).ConfigureAwait(false);
                }
                else
                {
                    if (stopped && queue.Count == 0) break;
                    await Task.Delay(idleDelay);
                }
            }
        }

        protected virtual void OnRemoved(Entry entry)
        {
        }

        protected virtual void OnAdded(Entry entry)
        {
        }

        /// <summary>
        /// Number of groups currently in the queue.
        /// </summary>
        public int GroupCount => pending.Count;

        /// <summary>
        /// Enqueues a new value.
        /// </summary>
        /// <param name="key">The group key</param>
        /// <param name="value">The value</param>
        /// <returns>True if the group key was added, false if it already existed in the queue</returns>
        public bool Enqueue(TKey key, TValue value)
        {
            if (stopped)
                return false;
            var didAdd = false;
            var now = timer.Elapsed;
            lock (pending)
            {
                if (stopped)
                    return false;
                if (pending.TryGetValue(key, out var entry))
                {
                    entry.Add(value, now + cooldown);
                }
                else
                {
                    entry = new Entry(
                        key,
                        value,
                        timeout > TimeSpan.Zero ? now + timeout : TimeSpan.MaxValue, now + cooldown,
                        aggregation,
                        initial());
                    pending.Add(key, entry);
                    didAdd = true;
                    queue.Post(entry);
                }

                OnAdded(entry);
            }

            return didAdd;
        }

        /// <summary>
        /// Stops processing new works.
        /// </summary>
        /// <returns>A task that is completed when all queued work is done.</returns>
        public Task Stop()
        {
            stopped = true;
            cancel.Cancel();
            return runLoopTask.ContinueWith(task => { Stopped?.Invoke(); });
        }

        public event Action Stopped;

        protected class Entry
        {
            public TKey Key { get; }
            public TAgg Aggregate { get; private set; }
            private readonly TimeSpan timeout;
            private TimeSpan cooldown;
            private readonly Func<TAgg, TValue, TAgg> aggregation;

            public Entry(TKey key, TValue value, TimeSpan timeout, TimeSpan cooldown,
                Func<TAgg, TValue, TAgg> aggregation, TAgg initial)
            {
                Key = key;
                Aggregate = initial;
                this.timeout = timeout;
                this.cooldown = cooldown;
                this.aggregation = aggregation;
                Add(value, cooldown);
            }

            public void Add(TValue value, TimeSpan newCooldown)
            {
                lock (this)
                {
                    cooldown = newCooldown;
                    Aggregate = aggregation(Aggregate, value);
                }
            }

            public TimeSpan ReadyTime => cooldown > timeout ? timeout : cooldown;
        }
    }
}