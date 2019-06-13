using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Combination.Threading.WorkQueues.Tests
{
    public class WorkQueueTests
    {
        [Fact]
        public async Task Single_Item_Scheduled()
        {
            var l = new List<string>();
            async Task Mupp(string k)
            {
                await Task.Delay(100);
                l.Add(k);
            }
            var q = new WorkQueue<string>(Mupp);
            q.Enqueue("Hello");
            await q.Stop();
            var result = l.Single();
            Assert.Equal("Hello", result);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(10)]
        [InlineData(100)]
        public async Task Parallelism_Respected(int numParallel)
        {
            var numItems = numParallel * 2;
            var lockRoot = new object();
            int numTotal = 0, currentParallel = 0, maxParallel = 0;
            async Task Mupp(string k)
            {
                Interlocked.Increment(ref numTotal);
                Interlocked.Increment(ref currentParallel);
                await Task.Delay(200);
                lock (lockRoot) maxParallel = Math.Max(currentParallel, maxParallel);
                Interlocked.Decrement(ref currentParallel);
            }
            var q = new WorkQueue<string>(Mupp, numParallel);
            for (var i = 0; i < numItems; ++i)
                q.Enqueue("Hello " + i);
            await q.Stop();
            Assert.Equal(numItems, numTotal);
            Assert.Equal(0, currentParallel);
            Assert.InRange(maxParallel, numParallel == 1 ? 1 : 2, numParallel);
        }
    }
}
