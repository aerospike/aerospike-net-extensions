/* 
 * Copyright 2018 Aerospike, Inc.
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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Aerospike.Client;

namespace Aerospike.Extensions.Caching
{
	/// <summary>
	/// Distributed cache implementation using Aerospike.
	/// </summary>
	public class AerospikeCache : IDistributedCache, IDisposable
	{
		private AsyncClient client;
		private readonly AerospikeCacheOptions config;
		private readonly SemaphoreSlim clientLock = new SemaphoreSlim(initialCount: 1, maxCount: 1);

		public AerospikeCache(IOptions<AerospikeCacheOptions> options)
		{
			this.config = options.Value;

			if (this.config.Hosts == null || this.config.Hosts.Length == 0)
			{
				throw new ArgumentException($"{nameof(AerospikeCacheOptions.Hosts)} is required.");
			}

			// Create default client policy if not set.
			if (this.config.ClientPolicy == null)
			{
				this.config.ClientPolicy = new AsyncClientPolicy();
			}

			// Set default record expiration from DefaultSlidingExpiration config.
			this.config.ClientPolicy.writePolicyDefault.expiration = (int)config.DefaultSlidingExpiration.TotalSeconds;
		}

		public byte[] Get(string key)
		{
			Connect();

			Record record = null;

			try
			{
				// Read value and reset expiration (via touch) in a single command.
				// Use default record expiration by setting policy to null.
				record = client.Operate(null, new Key(config.Namespace, config.Set, key),
					Operation.Get(config.BinName),
					Operation.Touch()
					);

				return (byte[])record.GetValue(config.BinName);
			}
			catch (AerospikeException ae)
			{
				if (ae.Result == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					return null;
				}
				throw;
			}
		}

		public Task<byte[]> GetAsync(string key, CancellationToken token = default(CancellationToken))
		{
			Connect();

			// Read value and reset expiration (via touch) in a single command.
			// Use default record expiration by setting policy to null.
			RecordHandler listener = new RecordHandler(token, config.BinName);
			client.Operate(null, listener, new Key(config.Namespace, config.Set, key),
				Operation.Get(config.BinName),
				Operation.Touch()
				);
			return listener.Task;
		}

		public void Refresh(string key)
		{
			Connect();

			try
			{
				// Use default record expiration by setting policy to null.
				client.Touch(null, new Key(config.Namespace, config.Set, key));
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.KEY_NOT_FOUND_ERROR)
				{
					throw;
				}
			}
		}

		public Task RefreshAsync(string key, CancellationToken token = default(CancellationToken))
		{
			Connect();

			TouchHandler listener = new TouchHandler(token);
			// Use default record expiration by setting policy to null.
			client.Touch(null, listener, new Key(config.Namespace, config.Set, key));
			return listener.Task;
		}

		public void Remove(string key)
		{
			Connect();
			client.Delete(null, new Key(config.Namespace, config.Set, key));
		}

		public Task RemoveAsync(string key, CancellationToken token = default(CancellationToken))
		{
			Connect();
			DeleteHandler listener = new DeleteHandler(token);
			client.Delete(null, listener, new Key(config.Namespace, config.Set, key));
			return listener.Task;
		}

		public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
		{
			Connect();

			WritePolicy policy = null;
			int expiration = ExpirationSeconds(options);

			if (expiration > 0 && expiration != client.writePolicyDefault.expiration)
			{
				// Use non-default record expiration.
				policy = new WritePolicy(client.writePolicyDefault);
				policy.expiration = expiration;
			}

			client.Put(policy, new Key(config.Namespace, config.Set, key), new Bin(config.BinName, value));
		}

		public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default(CancellationToken))
		{
			Connect();

			WritePolicy policy = null;
			int expiration = ExpirationSeconds(options);

			if (expiration > 0 && expiration != client.writePolicyDefault.expiration)
			{
				// Use non-default record expiration.
				policy = new WritePolicy(client.writePolicyDefault);
				policy.expiration = expiration;
			}

			WriteHandler listener = new WriteHandler(token);
			client.Put(policy, listener, new Key(config.Namespace, config.Set, key), new Bin(config.BinName, value));
			return listener.Task;
		}
    
		private void Connect()
		{
			if (client != null)
			{
				return;
			}

			clientLock.Wait();

			try
			{
				if (client == null)
				{
					client = new AsyncClient(config.ClientPolicy, config.Hosts);
				}
			}
			finally
			{
				clientLock.Release();
			}
		}

		private int ExpirationSeconds(DistributedCacheEntryOptions options)
		{
			// Zero means default expiration.
			int relativeSeconds = 0;

			if (options.AbsoluteExpirationRelativeToNow.HasValue)
			{
				relativeSeconds = (int)options.AbsoluteExpirationRelativeToNow.Value.TotalSeconds;
			}
			else if (options.AbsoluteExpiration.HasValue)
			{
				relativeSeconds = (int)options.AbsoluteExpiration.Value.Subtract(DateTime.UtcNow).TotalSeconds;
			}

			if (options.SlidingExpiration.HasValue)
			{
				int slidingSeconds = (int)options.SlidingExpiration.Value.TotalSeconds;

				if (relativeSeconds == 0)
				{
					return slidingSeconds;
				}

				if (slidingSeconds == 0)
				{
					return relativeSeconds;
				}
				return (slidingSeconds <= relativeSeconds) ? slidingSeconds : relativeSeconds;
			}
			return relativeSeconds;
		}

		public void Dispose()
		{
			if (client != null)
			{
				client.Close();
				client = null;
			}
		}
	}

	internal sealed class RecordHandler : RecordListener
	{
		private readonly TaskCompletionSource<byte[]> tcs = new TaskCompletionSource<byte[]>();
		private readonly CancellationTokenRegistration ctr;
		private readonly string binName;

		public RecordHandler(CancellationToken token, string binName)
		{
			ctr = token.Register(() => tcs.TrySetCanceled(), useSynchronizationContext: false);
			this.binName = binName;
		}

		public void OnSuccess(Key key, Record record)
		{
			byte[] result = (byte[])record.GetValue(binName);
			tcs.TrySetResult(result);
			ctr.Dispose();
		}

		public void OnFailure(AerospikeException ae)
		{
			if (ae.Result == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				tcs.TrySetResult(null);
			}
			else
			{
				tcs.TrySetException(ae);
			}
			ctr.Dispose();
		}

		public System.Threading.Tasks.Task<byte[]> Task { get { return tcs.Task; } }
	}

	internal sealed class WriteHandler : WriteListener
	{
		private readonly TaskCompletionSource<Key> tcs = new TaskCompletionSource<Key>();
		private readonly CancellationTokenRegistration ctr;

		public WriteHandler(CancellationToken token)
		{
			ctr = token.Register(() => tcs.TrySetCanceled(), useSynchronizationContext: false);
		}

		public void OnSuccess(Key key)
		{
			tcs.TrySetResult(key);
			ctr.Dispose();
		}

		public void OnFailure(AerospikeException ae)
		{
			tcs.TrySetException(ae);
			ctr.Dispose();
		}

		public System.Threading.Tasks.Task Task { get { return tcs.Task; } }
	}

	internal sealed class TouchHandler : WriteListener
	{
		private readonly TaskCompletionSource<Key> tcs = new TaskCompletionSource<Key>();
		private readonly CancellationTokenRegistration ctr;

		public TouchHandler(CancellationToken token)
		{
			ctr = token.Register(() => tcs.TrySetCanceled(), useSynchronizationContext: false);
		}

		public void OnSuccess(Key key)
		{
			tcs.TrySetResult(key);
			ctr.Dispose();
		}

		public void OnFailure(AerospikeException ae)
		{
			if (ae.Result == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				tcs.TrySetResult(null);
			}
			else
			{
				tcs.TrySetException(ae);
			}
			ctr.Dispose();
		}

		public System.Threading.Tasks.Task Task { get { return tcs.Task; } }
	}

	internal sealed class DeleteHandler : DeleteListener
	{
		private readonly TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();
		private readonly CancellationTokenRegistration ctr;

		public DeleteHandler(CancellationToken token)
		{
			ctr = token.Register(() => tcs.TrySetCanceled(), useSynchronizationContext: false);
		}

		public void OnSuccess(Key key, bool existed)
		{
			tcs.TrySetResult(existed);
			ctr.Dispose();
		}

		public void OnFailure(AerospikeException ae)
		{
			tcs.TrySetException(ae);
			ctr.Dispose();
		}

		public System.Threading.Tasks.Task Task { get { return tcs.Task; } }
	}
}
