using System;
using System.Collections.Generic;

namespace SJP.DiskCache
{
    /// <summary>
    /// A cache entry to be used for assisting with accessing and applying cache policies within a disk cache.
    /// </summary>
    /// <typeparam name="TKey">The type of keys used in the cache.</typeparam>
    public class FileEntry<TKey> : IFileEntry<TKey> where TKey : IEquatable<TKey>
    {
        /// <summary>
        /// Initializes a cache entry.
        /// </summary>
        /// <param name="key">The key of the cache entry that is used to retrieve data from a disk cache.</param>
        /// <param name="name">The filename string.</param>
        public FileEntry(TKey key, string name)
        {
            if (IsNull(key))
                throw new ArgumentNullException(nameof(key));
            if (name == null)
                throw new ArgumentException("The file name must be non-zero.");

            Key = key;
            Name = name;
        }


        /// <summary>
        /// Persistent Dictionary required constructor
        /// </summary>
        public FileEntry()
        {

        }

        /// <summary>
        /// The key that the entry represents when looking up in the cache.
        /// </summary>
        public TKey Key { get; set; }

        /// <summary>
        /// The key that the entry represents when looking up in the cache.
        /// </summary>
        public string Name { get; set; }

        private static bool IsNull(TKey key) => !_isValueType && EqualityComparer<TKey>.Default.Equals(key, default(TKey));

        private readonly static bool _isValueType = typeof(TKey).IsValueType;
    }
}