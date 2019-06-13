using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Combination.Threading.WorkQueues.Tests
{
    public class GroupedWorkQueueTests
    {
        [Fact]
        public async Task Single_Item_Scheduled()
        {
            var l = new List<string[]>();
            async Task TestProcessor(int key, IEnumerable<string> k)
            {
                await Task.Delay(100);
                l.Add(k.ToArray());
            }
            var q = new GroupedWorkQueue<int, string>(TestProcessor);
            q.Enqueue(0, "Hello");
            await q.Stop();
            var result = l.Single().Single();
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
            async Task TestProcessor(int key, IEnumerable<string> k)
            {
                Interlocked.Increment(ref numTotal);
                Interlocked.Increment(ref currentParallel);
                await Task.Delay(200);
                lock (lockRoot) maxParallel = Math.Max(currentParallel, maxParallel);
                Interlocked.Decrement(ref currentParallel);
            }
            var q = new GroupedWorkQueue<int, string>(TestProcessor, numParallel);
            for (var i = 0; i < numItems; ++i)
                q.Enqueue(i, "Hello " + i);
            await q.Stop();
            Assert.Equal(numItems, numTotal);
            Assert.Equal(0, currentParallel);
            Assert.InRange(maxParallel, numParallel == 1 ? 1 : 2, numParallel);
        }

        [Fact]
        public async Task Batch_Cooldown_Respected()
        {
            var lockRoot = new object();
            int numTotal = 0, currentParallel = 0, maxParallel = 0;
            async Task TestProcessor(int key, IEnumerable<string> k)
            {
                Interlocked.Increment(ref numTotal);
                Interlocked.Increment(ref currentParallel);
                await Task.Delay(200);
                lock (lockRoot) maxParallel = Math.Max(currentParallel, maxParallel);
                Interlocked.Decrement(ref currentParallel);
            }
            var q = new GroupedWorkQueue<int, string>(TestProcessor, TimeSpan.FromMilliseconds(100), 2);
            q.Enqueue(0, "Hello1");
            await Task.Delay(1);
            q.Enqueue(1, "Hello2");
            await Task.Delay(1);
            q.Enqueue(0, "Hello3");
            await q.Stop();
            Assert.Equal(2, numTotal);
            Assert.Equal(0, currentParallel);
        }

        [Fact]
        public async Task Batch_Zero_Cooldown_Respected()
        {
            var lockRoot = new object();
            int numTotal = 0, currentParallel = 0, maxParallel = 0;
            async Task TestProcessor(int key, IEnumerable<string> k)
            {
                Interlocked.Increment(ref numTotal);
                Interlocked.Increment(ref currentParallel);
                await Task.Delay(200);
                lock (lockRoot) maxParallel = Math.Max(currentParallel, maxParallel);
                Interlocked.Decrement(ref currentParallel);
            }
            var q = new GroupedWorkQueue<int, string>(TestProcessor, TimeSpan.Zero, 2);
            q.Enqueue(0, "Hello1");
            await Task.Delay(1);
            q.Enqueue(1, "Hello2");
            await Task.Delay(1);
            q.Enqueue(0, "Hello3");
            await q.Stop();
            Assert.Equal(3, numTotal);
            Assert.Equal(0, currentParallel);
        }

        [Fact]
        public async Task Batch_Timeout_Respected()
        {
            var lockRoot = new object();
            int numTotal = 0, currentParallel = 0, maxParallel = 0;
            TimeSpan maxElapsed = TimeSpan.Zero;
            async Task TestProcessor(int key, IEnumerable<Stopwatch> k)
            {
                var maxElapsedNow = k.Select(x => x.Elapsed).Max();
                Interlocked.Increment(ref numTotal);
                Interlocked.Increment(ref currentParallel);
                await Task.Delay(200);
                lock (lockRoot)
                {
                    maxParallel = Math.Max(currentParallel, maxParallel);
                    maxElapsed = maxElapsed > maxElapsedNow ? maxElapsed : maxElapsedNow;
                }
                Interlocked.Decrement(ref currentParallel);
            }
            var q = new GroupedWorkQueue<int, Stopwatch>(TestProcessor, TimeSpan.FromMilliseconds(1000), TimeSpan.FromMilliseconds(500), 2);
            for (var i = 0; i < 10; ++i)
            {
                q.Enqueue(0, Stopwatch.StartNew());
                await Task.Delay(100);
            }
            await q.Stop();
            Assert.InRange(numTotal, 2, 10);
            Assert.InRange(maxElapsed, TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(1500));
            Assert.Equal(0, currentParallel);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(100)]
        [InlineData(1000)]
        [InlineData(1000000)]
        public async Task Add_Process_Items_Without_Yield(int numItems)
        {
            var numTotal = 0;
            var counts = new HashSet<int>();
            Task TestProcessor(int key, IEnumerable<int> k)
            {
                Interlocked.Increment(ref numTotal);
                lock (counts) counts.Add(key);
                return Task.CompletedTask;
            }
            var q = new GroupedWorkQueue<int, int>(TestProcessor, 2);
            var numKeys = Math.Min(numItems, 10);
            for (var i = 0; i < numItems; ++i)
            {
                q.Enqueue(i % 10, i);
            }
            await q.Stop();

            Assert.InRange(numTotal, numKeys, numItems);
            Assert.Equal(numKeys, counts.Count);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(100)]
        public async Task Add_Process_Items_With_Yield(int numItems)
        {
            var numTotal = 0;
            var counts = new HashSet<int>();
            Task TestProcessor(int key, IEnumerable<int> k)
            {
                Interlocked.Increment(ref numTotal);
                lock (counts) counts.Add(key);
                return Task.CompletedTask;
            }
            var q = new GroupedWorkQueue<int, int>(TestProcessor, 2);
            var numKeys = Math.Min(numItems, 10);
            for (var i = 0; i < numItems; ++i)
            {
                q.Enqueue(i % 10, i);
                if (i < 10 || (i % 20 == 0)) await Task.Delay(100);
            }
            await q.Stop();

            Assert.InRange(numTotal, numKeys, numItems);
            Assert.Equal(numKeys, counts.Count);
        }
    }
}
