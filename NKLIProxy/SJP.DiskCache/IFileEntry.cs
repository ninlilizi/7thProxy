namespace SJP.DiskCache
{
    /// <summary>
    /// Represents a generic cache entry, including information useful for cache policies.
    /// </summary>
    /// <typeparam name="TKey">The type of keys used in the cache.</typeparam>
    public interface IFileEntry<out TKey>
    {
        /// <summary>
        /// The key that the entry represents when looking up in the cache.
        /// </summary>
        TKey Key { get; }

        /// <summary>
        /// The size of the data that the cache entry is associated with.
        /// </summary>
        string Name { get; }
    }
}