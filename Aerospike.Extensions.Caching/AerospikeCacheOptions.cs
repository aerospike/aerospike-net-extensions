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
using Microsoft.Extensions.Options;
using Aerospike.Client;

namespace Aerospike.Extensions.Caching
{
	/// <summary>
	/// Configuration options for <see cref="AerospikeCache"/>.
	/// </summary>
	public class AerospikeCacheOptions : IOptions<AerospikeCacheOptions>
	{
		/// <summary>
		/// One or more hosts used to seed the Aerospike client instance.
		/// This field is required.
		/// </summary>
		public Host[] Hosts { get; set; }

		/// <summary>
		/// Client policy for async and sync commands.
		/// </summary>
		public AsyncClientPolicy ClientPolicy { get; set; }

		/// <summary>
		/// Aerospike cluster namespace used for cache.  Default: "test"
		/// </summary>
		public string Namespace { get; set; } = "test";

		/// <summary>
		/// Aerospike set name used for cache.  Default: "cache"
		/// </summary>
		public string Set { get; set; } = "cache";

		/// <summary>
		/// Aerospike bin name associated with cache value.  Default: "val"
		/// Do not use "vt" because that bin name is reserved to store absolute expiration.
		/// </summary>
		public string BinName { get; set; } = "val";

		/// <summary>
		/// Aerospike configuration instance.
		/// </summary>
		public AerospikeCacheOptions Value => this;
	}
}
