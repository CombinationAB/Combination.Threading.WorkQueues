using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Combination.Threading.WorkQueues
{
    public sealed class WorkQueue<T>
    {
        private readonly BufferBlock<T> queue = new BufferBlock<T>();
        private readonly Func<T, Task> processor;
        private volatile bool stopped;
        private readonly Task runLoopTask;

        public WorkQueue(Func<T, Task> processor, int parallelism = 1)
        {
            this.processor = processor;
            runLoopTask = Task.WhenAll(Enumerable.Range(0, parallelism).Select(_ => Task.Run(ProcessLoop)));
        }

        public void Enqueue(T item)
        {
            if (!stopped)
                queue.Post(item);
        }

        public Task Stop()
        {
            stopped = true;
            return runLoopTask;
        }

        private async Task ProcessLoop()
        {
            while (!stopped || queue.Count > 0)
            {
                var item = await queue.ReceiveAsync().ConfigureAwait(false);
                await processor(item).ConfigureAwait(false);
            }
        }

        public int Count => queue.Count;
    }
}
