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

    public sealed class GroupedWorkQueue<TKey, TValue>
    {
        private readonly Dictionary<TKey, Entry> pending = new Dictionary<TKey, Entry>();
        private readonly Func<TKey, IEnumerable<TValue>, Task> processor;
        private volatile bool stopped;
        private readonly Stopwatch timer = Stopwatch.StartNew();
        private readonly Task runLoopTask;
        private readonly BufferBlock<Entry> queue = new BufferBlock<Entry>();
        private readonly TimeSpan timeout, cooldown, idleDelay;
        private volatile int currentLength;
        private readonly CancellationTokenSource cancel = new CancellationTokenSource();

        public GroupedWorkQueue(Func<TKey, IEnumerable<TValue>, Task> processor)
            : this(processor, new GroupedWorkQueueOptions())
        {
        }

        public GroupedWorkQueue(Func<TKey, IEnumerable<TValue>, Task> processor, int parallelism)
            : this(processor, new GroupedWorkQueueOptions { Parallelism = parallelism })
        {
        }
        public GroupedWorkQueue(Func<TKey, IEnumerable<TValue>, Task> processor, GroupedWorkQueueOptions options)
        {
            if (options == null) options = new GroupedWorkQueueOptions();
            this.processor = processor;
            cooldown = options.Cooldown;
            if (cooldown.TotalMilliseconds >= 100)
            {
                idleDelay = TimeSpan.FromMilliseconds(10);
            }
            else
            {
                idleDelay = TimeSpan.FromMilliseconds(1);
            }
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
                        Interlocked.Add(ref currentLength, -entry.Count);
                    }
                }
                if (didFindEntry)
                {
                    await processor(entry.Key, entry.Values).ConfigureAwait(false);
                }
                else
                {
                    if (stopped && queue.Count == 0) break;
                    await Task.Delay(idleDelay);
                }
            }
        }

        /// <summary>
        /// Number of values currently in the queue.
        /// </summary>
        public int Count => currentLength;

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
                    entry = new Entry(key, value, timeout > TimeSpan.Zero ? now + timeout : TimeSpan.MaxValue, now + cooldown);
                    pending.Add(key, entry);
                    didAdd = true;
                    queue.Post(entry);
                }
                Interlocked.Increment(ref currentLength);
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
            return runLoopTask;
        }

        private class Entry
        {
            public TKey Key { get; }
            private readonly List<TValue> values = new List<TValue>();
            public IEnumerable<TValue> Values => values;
            private readonly TimeSpan timeout;
            private TimeSpan cooldown;

            public Entry(TKey key, TValue value, TimeSpan timeout, TimeSpan cooldown)
            {
                Key = key;
                this.timeout = timeout;
                this.cooldown = cooldown;
                Add(value, cooldown);
            }

            public void Add(TValue value, TimeSpan newCooldown)
            {
                lock (values)
                {
                    cooldown = newCooldown;
                    values.Add(value);
                }
            }

            public TimeSpan ReadyTime => cooldown > timeout ? timeout : cooldown;

            public int Count => values.Count;
        }
    }
}
