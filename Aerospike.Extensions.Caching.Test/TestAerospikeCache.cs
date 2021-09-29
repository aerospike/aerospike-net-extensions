/* 
 * Copyright 2021 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Extensions.Caching.Test
{
    [TestClass]
    public class TestAerospikeCache
    {
		public static Args args = Args.Instance;
		public static AerospikeCache cache = args.cache;
		public static TimeSpan expiration = args.expiration;

		[TestMethod]
        public void TestSyncSetGetRemove()
        {
			var message = "Hello, Sync!";
			byte[] value = Encoding.UTF8.GetBytes(message);
			string key = "k1";

			DistributedCacheEntryOptions options = new DistributedCacheEntryOptions
			{
				SlidingExpiration = expiration
			};
			cache.Set(key, value, options);

			byte[] returnVal = cache.Get(key);

			Assert.IsNotNull(returnVal);
			Assert.IsTrue(Util.ByteArrayEquals(value, returnVal));

			cache.Remove(key);

			returnVal = cache.Get(key);

			Assert.IsNull(returnVal);
		}

		[TestMethod]
		public void TestSyncExpire()
		{
			var message = "Hello, Expire!";
			byte[] value = Encoding.UTF8.GetBytes(message);
			string key = "k2";

			DistributedCacheEntryOptions options = new DistributedCacheEntryOptions
			{
				SlidingExpiration = expiration
			};
			cache.Set(key, value, options);

			byte[] returnVal = cache.Get(key);

			Assert.IsNotNull(returnVal);
			Assert.IsTrue(Util.ByteArrayEquals(value, returnVal));

			Util.Sleep((int)expiration.TotalMilliseconds + 1000);

			returnVal = cache.Get(key);

			Assert.IsNull(returnVal);
		}

		[TestMethod]
		public void TestSyncRefresh()
		{
			var message = "Hello, Refresh!";
			byte[] value = Encoding.UTF8.GetBytes(message);
			string key = "k3";

			DistributedCacheEntryOptions options = new DistributedCacheEntryOptions
			{
				SlidingExpiration = expiration
			};
			cache.Set(key, value, options);

			Util.Sleep((int)expiration.TotalMilliseconds - 1000);

			cache.Refresh(key);

			Util.Sleep((int)expiration.TotalMilliseconds - 1000);

			byte[] returnVal = cache.Get(key);

			Assert.IsNotNull(returnVal);
			Assert.IsTrue(Util.ByteArrayEquals(value, returnVal));

			Util.Sleep(3000);

			returnVal = cache.Get(key);

			Assert.IsNull(returnVal);
		}

		[TestMethod]
		public void TestSyncNotFound()
		{
			string key = "UNKNOWN";
			byte[] returnVal = cache.Get(key);
			Assert.IsNull(returnVal);
		}

		[TestMethod]
		public void TestSyncSlidingAndAbsolute()
		{
			var message = "SlidingAndAbsolute";
			byte[] value = Encoding.UTF8.GetBytes(message);
			string key = "k4";

			cache.Remove(key);

			DistributedCacheEntryOptions options = new DistributedCacheEntryOptions
			{
				SlidingExpiration = new TimeSpan(0, 0, 10),
				AbsoluteExpirationRelativeToNow = new TimeSpan(0, 0, 20)
			};
			cache.Set(key, value, options);

			Util.Sleep(7000);

			byte[] returnVal = cache.Get(key);
			Assert.IsNotNull(returnVal);
			Assert.IsTrue(Util.ByteArrayEquals(value, returnVal));

			Util.Sleep(7000);

			byte[] returnVal2 = cache.Get(key);
			Assert.IsNotNull(returnVal2);
			Assert.IsTrue(Util.ByteArrayEquals(value, returnVal2));

			Util.Sleep(7000);

			byte[] returnVal3 = cache.Get(key);
			Assert.IsNull(returnVal3);
		}

		[TestMethod]
		public async Task TestAsync()
		{
			var message = "Hello, async!";
			byte[] value = Encoding.UTF8.GetBytes(message);
			string key = "k1";

			DistributedCacheEntryOptions options = new DistributedCacheEntryOptions
			{
				SlidingExpiration = expiration
			};
			await cache.SetAsync(key, value, options);

			byte[] returnVal = await cache.GetAsync(key);

			Assert.IsNotNull(returnVal);
			Assert.IsTrue(Util.ByteArrayEquals(value, returnVal));

			await cache.RemoveAsync(key);

			returnVal = await cache.GetAsync(key);

			Assert.IsNull(returnVal);

			await cache.RefreshAsync(key);
		}

		[TestMethod]
		public async Task TestAsyncNotFound()
		{
			string key = "UNKNOWN";
			byte[] returnVal = await cache.GetAsync(key);
			Assert.IsNull(returnVal);
		}
	}
}
