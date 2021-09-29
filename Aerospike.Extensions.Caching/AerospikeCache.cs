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
		private static readonly DateTime AerospikeEpoch = new DateTime(2010, 1, 1, 0, 0, 0, DateTimeKind.Utc);
		private const string ExpirationBinName = "vt";

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
		}

		public byte[] Get(string key)
		{
			Connect();

			object obj = client.Execute(null, new Key(config.Namespace, config.Set, key), 
				"readtouch", "readTouch", Value.Get(config.BinName));

			return (byte[])obj;
		}

		public Task<byte[]> GetAsync(string key, CancellationToken token = default(CancellationToken))
		{
			Connect();

			ExecuteHandler listener = new ExecuteHandler(token);
			client.Execute(null, listener, new Key(config.Namespace, config.Set, key),
				"readtouch", "readTouch", Value.Get(config.BinName)
				);
			return listener.Task;
		}

		public void Refresh(string key)
		{
			Connect();

			client.Execute(null, new Key(config.Namespace, config.Set, key), "readtouch", "touch");
		}

		public Task RefreshAsync(string key, CancellationToken token = default(CancellationToken))
		{
			Connect();

			TouchHandler listener = new TouchHandler(token);
			client.Execute(null, listener, new Key(config.Namespace, config.Set, key), "readtouch", "touch");
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

			SetExpiration(options, out int exp, out int expBin);

			WritePolicy policy = null;

			if (exp > 0)
			{
				// Use non-default record expiration.
				policy = new WritePolicy(client.writePolicyDefault)
				{
					expiration = exp
				};
			}

			if (expBin >= 0)
			{
				client.Put(policy, new Key(config.Namespace, config.Set, key),
								   new Bin(config.BinName, value), new Bin(ExpirationBinName, expBin));
			}
			else
			{
				client.Put(policy, new Key(config.Namespace, config.Set, key),
								   new Bin(config.BinName, value));
			}
		}

		public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default(CancellationToken))
		{
			Connect();

			SetExpiration(options, out int exp, out int expBin);

			WritePolicy policy = null;

			if (exp > 0)
			{
				// Use non-default record expiration.
				policy = new WritePolicy(client.writePolicyDefault)
				{
					expiration = exp
				};
			}

			WriteHandler listener = new WriteHandler(token);

			if (expBin >= 0)
			{
				client.Put(policy, listener, new Key(config.Namespace, config.Set, key),
											 new Bin(config.BinName, value), new Bin(ExpirationBinName, expBin));
			}
			else
			{
				client.Put(policy, listener, new Key(config.Namespace, config.Set, key),
											 new Bin(config.BinName, value));
			}
			return listener.Task;
		}
    
		private void SetExpiration(DistributedCacheEntryOptions options, out int exp, out int expBin)
		{
			if (options.SlidingExpiration.HasValue)
			{
				exp = (int)options.SlidingExpiration.Value.TotalSeconds;

				if (options.AbsoluteExpiration.HasValue)
				{
					int abs = (int)options.AbsoluteExpiration.Value.Subtract(DateTime.UtcNow).TotalSeconds;

					if (abs > exp)
					{
						// Use sliding expiration up until relative absolute expiration.
						expBin = (int)options.AbsoluteExpiration.Value.Subtract(AerospikeEpoch).TotalSeconds;
					}
					else
					{
						// Use absolute relative expiration only and do not set expiration bin.
						exp = abs;
						expBin = -1;
					}
				}
				else if (options.AbsoluteExpirationRelativeToNow.HasValue)
				{
					int abs = (int)options.AbsoluteExpirationRelativeToNow.Value.TotalSeconds;

					if (abs > exp)
					{
						// Use sliding expiration up until relative absolute expiration.
						expBin = (int)DateTime.UtcNow.AddSeconds(abs).Subtract(AerospikeEpoch).TotalSeconds;
					}
					else
					{
						// Use absolute relative expiration only and do not set expiration bin.
						exp = abs;
						expBin = -1;
					}
				}
				else
				{
					// Use sliding expiration only.
					expBin = 0;
				}
				return;
			}

			if (options.AbsoluteExpiration.HasValue)
			{
				// Use absolute expiration only and do not set expiration bin.
				exp = (int)options.AbsoluteExpiration.Value.Subtract(DateTime.UtcNow).TotalSeconds;
				expBin = -1;
				return;
			}

			if (options.AbsoluteExpirationRelativeToNow.HasValue)
			{
				// Use absolute relative expiration only and do not set expiration bin.
				exp = (int)options.AbsoluteExpirationRelativeToNow.Value.TotalSeconds;
				expBin = -1;
				return;
			}

			// Use default expiration and do not set expiration bin.
			exp = 0;
			expBin = -1;
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

					try
					{
						RegisterUDF();
					}
					catch (Exception)
					{
						client.Close();
						client = null;
						throw;
					}
				}
			}
			finally
			{
				clientLock.Release();
			}
		}

		private void RegisterUDF()
		{
			string packageContents = @"
local function setTTL(r)
  local vt = r['vt']
    
  if vt ~= nil then
    -- Get last update time in seconds since Aerospike epoch (2010/1/1 UTC).
    local lut = math.floor(record.last_update_time(r) / 1000)
    -- Get current time in seconds since Aerospike epoch (2010/1/1 UTC).
    local now = os.time() - 1262304000
    -- Get new ttl in seconds.
    local ttl = now - lut + record.ttl(r)

    -- Adjust ttl if greater than absolute void time.    
    if vt > 0 and ttl + now > vt then
      ttl = vt - now
    end

    -- Only set a valid ttl.
	-- Otherwise, let record expire.
    if ttl >= 1 then
      -- Set ttl.
      record.set_ttl(r, ttl)
	  aerospike:update(r)
    end
  end
end

-- Read bin and reset ttl when applicable.
function readTouch(r,name)
  if not aerospike:exists(r) then
    return nil
  end
 
  setTTL(r) 
  return r[name]
end

-- Reset ttl when applicable.
function touch(r)
  if not aerospike:exists(r) then
    return
  end
 
  setTTL(r) 
end
";
			RegisterTask task = client.RegisterUdfString(null, packageContents, "readtouch.lua", Language.LUA);
			task.Wait();
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

	internal sealed class ExecuteHandler : ExecuteListener
	{
		private readonly TaskCompletionSource<byte[]> tcs = new TaskCompletionSource<byte[]>();
		private readonly CancellationTokenRegistration ctr;

		public ExecuteHandler(CancellationToken token)
		{
			ctr = token.Register(() => tcs.TrySetCanceled(), useSynchronizationContext: false);
		}

		public void OnSuccess(Key key, object obj)
		{
			byte[] result = (byte[])obj;
			tcs.TrySetResult(result);
			ctr.Dispose();
		}

		public void OnFailure(AerospikeException ae)
		{
			tcs.TrySetException(ae);
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

	internal sealed class TouchHandler : ExecuteListener
	{
		private readonly TaskCompletionSource<Key> tcs = new TaskCompletionSource<Key>();
		private readonly CancellationTokenRegistration ctr;

		public TouchHandler(CancellationToken token)
		{
			ctr = token.Register(() => tcs.TrySetCanceled(), useSynchronizationContext: false);
		}

		public void OnSuccess(Key key, object obj)
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
