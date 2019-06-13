using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Combination.Threading.WorkQueues
{
    public sealed class GroupedWorkQueue<TKey, TValue>
    {
        private readonly Dictionary<TKey, Entry> pending = new Dictionary<TKey, Entry>();
        private readonly Func<TKey, IEnumerable<TValue>, Task> processor;
        private volatile bool stopped;
        private readonly TaskCompletionSource<object> runTask = new TaskCompletionSource<object>();
        private readonly SemaphoreSlim parallelismLock;
        private readonly TimeSpan timeout, cooldown;
        private volatile int parallelCount;
        private volatile int currentLength;

        public GroupedWorkQueue(Func<TKey, IEnumerable<TValue>, Task> processor, int paralellism = 1)
            : this(processor, TimeSpan.FromMilliseconds(100), paralellism)
        {
        }
        public GroupedWorkQueue(Func<TKey, IEnumerable<TValue>, Task> processor, TimeSpan cooldown, int paralellism = 1)
            : this(processor, cooldown, TimeSpan.Zero, paralellism)
        {
        }
        public GroupedWorkQueue(Func<TKey, IEnumerable<TValue>, Task> processor, TimeSpan cooldown, TimeSpan timeout, int paralellism = 1)
        {
            this.processor = processor;
            this.cooldown = cooldown;
            this.timeout = timeout;
            parallelismLock = new SemaphoreSlim(paralellism, paralellism);
        }
        public int Count => currentLength;

        public int GroupCount => pending.Count;

        public void Enqueue(TKey key, TValue value)
        {
            if (stopped)
                return;
            Entry entry;
            lock (pending)
            {
                if (stopped)
                    return;
                if (pending.TryGetValue(key, out entry))
                {
                    entry.Add(value);
                }
                else
                {
                    entry = new Entry(key, value, timeout, cooldown);
                    pending.Add(key, entry);
                }
                Interlocked.Increment(ref currentLength);
            }
            Task.Run(() => ProcessOnce(entry));
        }

        public Task Stop()
        {
            lock (pending)
            {
                stopped = true;
                if (parallelCount == 0 && pending.Count == 0) return Task.CompletedTask;
            }
            return runTask.Task;
        }

        private async Task ProcessOnce(Entry entry)
        {
            Interlocked.Increment(ref parallelCount);
            try
            {
                for (; ; )
                {
                    await entry.GetTask();

                    if (!entry.ReadyToFlush)
                        return;

                    if (await parallelismLock.WaitAsync(1))
                    {
                        try
                        {
                            lock (pending)
                            {
                                if (pending.TryGetValue(entry.Key, out var oent) && oent == entry)
                                {
                                    Interlocked.Add(ref currentLength, -entry.Count);
                                    pending.Remove(entry.Key);
                                }
                                else
                                {
                                    return;
                                }
                            }
                            await processor(entry.Key, entry.Values).ConfigureAwait(false);
                            return;
                        }
                        finally
                        {
                            parallelismLock.Release();
                        }
                    }
                }
            }
            finally
            {
                if (Interlocked.Decrement(ref parallelCount) == 0 && stopped && pending.Count == 0) runTask.SetResult(null);
            }
        }

        private class Entry
        {
            public TKey Key { get; }
            private readonly List<TValue> values = new List<TValue>();
            public IEnumerable<TValue> Values => values;
            private Task waitTask;
            private readonly TimeSpan timeout;
            private readonly Stopwatch timeoutTimer;
            private readonly TimeSpan cooldown;
            private Stopwatch cooldownTimer;

            public Entry(TKey key, TValue value, TimeSpan timeout, TimeSpan cooldown)
            {
                Key = key;
                this.timeout = timeout;
                this.cooldown = cooldown;
                if (timeout > TimeSpan.Zero && cooldown > TimeSpan.Zero) timeoutTimer = Stopwatch.StartNew();
                Add(value);
            }

            public void Add(TValue value)
            {
                waitTask = null;
                lock (values)
                {
                    if (cooldown > TimeSpan.Zero) cooldownTimer = Stopwatch.StartNew();
                    values.Add(value);
                }
            }

            public bool ReadyToFlush => GetTask().IsCompleted;

            public int Count => values.Count;

            public Task GetTask()
            {
                if (waitTask != null) return waitTask;
                lock (values)
                {
                    if (waitTask != null) return waitTask;
                    Task cooldownDelay = null;
                    if (cooldownTimer != null)
                    {
                        var delay = cooldown - cooldownTimer.Elapsed;
                        if (delay > TimeSpan.Zero)
                        {
                            cooldownDelay = Task.Delay(delay);
                        }
                    }
                    Task timeoutDelay = null;
                    if (timeoutTimer != null)
                    {
                        var delay = timeout - timeoutTimer.Elapsed;
                        if (delay > TimeSpan.Zero)
                        {
                            timeoutDelay = Task.Delay(delay);
                        }
                    }
                    if (cooldownDelay != null)
                    {
                        if (timeoutDelay != null)
                        {
                            waitTask = Task.WhenAny(cooldownDelay, timeoutDelay);
                        }
                        else
                        {
                            waitTask = cooldownDelay;
                        }
                    }
                    else if (timeoutDelay != null)
                    {
                        waitTask = timeoutDelay;
                    }
                    else
                    {
                        waitTask = Task.CompletedTask;
                    }
                    return waitTask;
                }
            }
        }
    }
}
