using System;
using System.Threading;
using System.Collections.Generic;

namespace SJP.DiskCache
{
    /// <summary>
    /// A cache entry to be used for assisting with accessing and applying cache policies within a disk cache.
    /// </summary>
    /// <typeparam name="TKey">The type of keys used in the cache.</typeparam>
    public class CacheEntry<TKey> : ICacheEntry<TKey> where TKey : IEquatable<TKey>
    {
        /// <summary>
        /// Initializes a cache entry.
        /// </summary>
        /// <param name="key">The key of the cache entry that is used to retrieve data from a disk cache.</param>
        /// <param name="size">The size (on disk) of the data that the key is associated with.</param>
        public CacheEntry(TKey key, ulong size)
        {
            if (IsNull(key))
                throw new ArgumentNullException(nameof(key));
            if (size == 0)
                throw new ArgumentException("The file size must be non-zero.", nameof(size));

            Key = key;
            Size = size;
        }


        /// <summary>
        /// Persistent Dictionary required constructor
        /// </summary>
        public CacheEntry()
        {

        }

        /// <summary>
        /// The key that the entry represents when looking up in the cache.
        /// </summary>
        public TKey Key { get; set; }

        /// <summary>
        /// The size of the data that the cache entry is associated with.
        /// </summary>
        public ulong Size { get; set; }


        /// <summary>
        /// The last time at which the entry was retrieved from the cache.
        /// </summary>
        public DateTime LastAccessed { get; set; } = DateTime.Now;


        /// <summary>
        /// When the cache entry was created.
        /// </summary>
        public DateTime CreationTime { get; set; } = DateTime.Now;

        /// <summary>
        /// The number of times that the cache entry has been accessed.
        /// </summary>
        public ulong AccessCount
        {
            get { return Convert.ToUInt64(_accessCount); }
            set { _accessCount = Convert.ToInt64(value); }
        }

        /// <summary>
        /// Refreshes the cache entry, primarily to acknowledge an access. Increments access count and restarts access timer.
        /// </summary>
        public void Refresh()
        {
            Interlocked.Increment(ref _accessCount);
            LastAccessed = DateTime.Now;
        }

        private static bool IsNull(TKey key) => !_isValueType && EqualityComparer<TKey>.Default.Equals(key, default(TKey));

        private long _accessCount;

        private readonly static bool _isValueType = typeof(TKey).IsValueType;
    }
}