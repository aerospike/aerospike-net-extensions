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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Configuration;
using Aerospike.Client;

namespace Aerospike.Extensions.Caching.Test
{
	[TestClass]
	public class Args
	{
		public static Args Instance = new Args();

		public AerospikeCache cache;
		public TimeSpan expiration;

		public Args()
		{
			var config = new ConfigurationBuilder()
				.AddJsonFile("settings.json")
				.Build();

			string ps = config["Port"];
			int port = int.Parse(config["Port"]);
			string user = config["User"];
			string password = config["Password"];
			Host[] hosts = Host.ParseHosts(config["Host"], null, port);

			AsyncClientPolicy policy = new AsyncClientPolicy();
			policy.asyncMaxCommands = 200;

			if (!user.Equals(""))
			{
				policy.user = user;
				policy.password = password;
			}

			// Set very short expiration, so tests can verify record will expire.
			// Normal expiration should be much longer than this testing case.
			expiration = TimeSpan.FromSeconds(2);

			cache = new AerospikeCache(new AerospikeCacheOptions()
			{
				Hosts = hosts,
				ClientPolicy = policy,
				Namespace = config["Namespace"],
				Set = config["Set"],
				DefaultSlidingExpiration = expiration
			});
		}

		public void Close()
		{
			if (cache != null)
			{
				cache.Dispose();
				cache = null;
			}
		}
	}
}
