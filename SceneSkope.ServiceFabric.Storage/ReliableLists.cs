using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SceneSkope.ServiceFabric.Storage
{
    public class ReliableLists<TValue>
    {
        internal async static Task<ReliableLists<TValue>> CreateAsync(IReliableStateManager stateManager, IReliableDictionary2<string, ReliableListMetaData> metadataStore, string name)
        {
            var valueStore = await stateManager.GetOrAddAsync<IReliableDictionary2<ReliableListKey, TValue>>(name).ConfigureAwait(false);
            return new ReliableLists<TValue>(stateManager, metadataStore, valueStore, name);
        }

        private readonly IReliableDictionary2<ReliableListKey, TValue> _valueStore;
        private readonly IReliableStateManager _stateManager;
        private readonly IReliableDictionary2<string, ReliableListMetaData> _metadataStore;
        private readonly string _name;

        public ReliableLists(IReliableStateManager stateManager, IReliableDictionary2<string, ReliableListMetaData> metadataStore, IReliableDictionary2<ReliableListKey, TValue> valueStore, string name)
        {
            _stateManager = stateManager;
            _metadataStore = metadataStore;
            _valueStore = valueStore;
            _name = name;
        }

        private string MakeMetadataKey(string key) => _name + "#" + key;

        private Task<ConditionalValue<ReliableListMetaData>> TryGetMetadataAsync(ITransaction tx, string key) =>
            _metadataStore.TryGetValueAsync(tx, MakeMetadataKey(key));

        private Task<ReliableListMetaData> GetMetadataAsync(ITransaction tx, string key) =>
            _metadataStore.GetOrAddAsync(tx, MakeMetadataKey(key), _ => new ReliableListMetaData());

        private Task UpdateMetadataAsync(ITransaction tx, string key, ReliableListMetaData updatedMetadata) =>
            _metadataStore.SetAsync(tx, MakeMetadataKey(key), updatedMetadata);

        public async Task AddAsync(ITransaction tx, string key, TValue value)
        {
            var metadata = await GetMetadataAsync(tx, key).ConfigureAwait(false);
            var index = metadata.From + metadata.Count;
            var listKey = new ReliableListKey { Key = key, Id = index };
            await _valueStore.SetAsync(tx, listKey, value).ConfigureAwait(false);
            var updatedMetadata = new ReliableListMetaData { From = metadata.From, Count = metadata.Count + 1 };
            await UpdateMetadataAsync(tx, key, updatedMetadata).ConfigureAwait(false);
        }

        public async Task<IAsyncEnumerable<string>> CreateKeyEnumerableAsync(ITransaction tx)
        {
            var enumerable = await _metadataStore.CreateKeyEnumerableAsync(tx, EnumerationMode.Unordered).ConfigureAwait(false);
            var prefixLength = _name.Length + 1;
            return new MappingAsyncEnumerable<string, string>(enumerable, key => key.Substring(prefixLength));
        }

        public class ListInformation
        {
            public string Key { get; }
            public int Count { get; }

            public ListInformation(string key, int count)
            {
                Key = key;
                Count = count;
            }
        }

        public async Task<IAsyncEnumerable<ListInformation>> CreateInfoEnumerableAsync(ITransaction tx)
        {
            var enumerable = await _metadataStore.CreateEnumerableAsync(tx, EnumerationMode.Unordered).ConfigureAwait(false);
            var prefixLength = _name.Length + 1;
            return new MappingAsyncEnumerable<KeyValuePair<string, ReliableListMetaData>, ListInformation>(enumerable, kvp =>
                new ListInformation(kvp.Key.Substring(prefixLength), kvp.Value.Count));
        }

        public async Task<ConditionalValue<TValue>> TryGetAsync(ITransaction tx, string key, int index)
        {
            if (index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var metadataValue = await TryGetMetadataAsync(tx, key).ConfigureAwait(false);
            if (metadataValue.HasValue)
            {
                var listKey = new ReliableListKey { Key = key, Id = index };
                return await _valueStore.TryGetValueAsync(tx, listKey).ConfigureAwait(false);
            }
            else
            {
                return new ConditionalValue<TValue>();
            }
        }

        public async Task SetAsync(ITransaction tx, string key, int index, TValue value)
        {
            if (index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            var metadata = await GetMetadataAsync(tx, key).ConfigureAwait(false);
            if (index >= metadata.Count)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            else
            {
                var listKey = new ReliableListKey { Key = key, Id = index };
                await _valueStore.SetAsync(tx, listKey, value).ConfigureAwait(false);
            }
        }

        public async Task AddRangeAsync(ITransaction tx, string key, IEnumerable<TValue> values)
        {
            var metadata = await GetMetadataAsync(tx, key).ConfigureAwait(false);
            var from = metadata.From;
            var count = metadata.Count;
            foreach (var value in values)
            {
                var listKey = new ReliableListKey { Key = key, Id = from + count };
                await _valueStore.SetAsync(tx, listKey, value).ConfigureAwait(false);
                count++;
            }
            var updatedMetadata = new ReliableListMetaData { From = from, Count = count };
            await UpdateMetadataAsync(tx, key, updatedMetadata).ConfigureAwait(false);
        }

        public async Task ClearAsync(ITransaction tx, string key)
        {
            var metadata = await GetMetadataAsync(tx, key).ConfigureAwait(false);
            var from = metadata.From;
            var count = metadata.Count;
            for (var i = 0; i < count; i++)
            {
                var listKey = new ReliableListKey { Key = key, Id = from + i };
                await _valueStore.TryRemoveAsync(tx, listKey).ConfigureAwait(false);
            }
            await UpdateMetadataAsync(tx, key, new ReliableListMetaData()).ConfigureAwait(false);
        }

        public async Task<int> GetCountAsync(ITransaction tx, string key)
        {
            var metadata = await GetMetadataAsync(tx, key).ConfigureAwait(false);
            return metadata.Count;
        }

        public async Task RemoveFromStart(ITransaction tx, string key, int countToRemove = 1)
        {
            var metadata = await GetMetadataAsync(tx, key).ConfigureAwait(false);
            var from = metadata.From;
            var count = metadata.Count;
            if (countToRemove > count)
            {
                throw new InvalidOperationException($"Trying to remove {countToRemove} entries from {_name} with only {count} messages");
            }
            for (var i = 0; i < countToRemove; i++)
            {
                var listKey = new ReliableListKey { Key = key, Id = from };
                await _valueStore.TryRemoveAsync(tx, listKey).ConfigureAwait(false);
                from++;
                count--;
            }
            var updatedMetadata = new ReliableListMetaData { From = from, Count = count };
            await UpdateMetadataAsync(tx, key, updatedMetadata).ConfigureAwait(false);
        }

        private class AsyncEnumerable : IAsyncEnumerable<TValue>
        {
            private class AsyncEnumerator : IAsyncEnumerator<TValue>
            {
                private readonly AsyncEnumerable _enumerable;
                private TValue _current;
                private int _index;

                public TValue Current => _current;

                public AsyncEnumerator(AsyncEnumerable enumerable)
                {
                    _enumerable = enumerable;
                }

                public void Dispose()
                {
                }

                public async Task<bool> MoveNextAsync(CancellationToken cancellationToken)
                {
                    if (_index < _enumerable._metadata.Count)
                    {
                        var result = await _enumerable._list._valueStore.TryGetValueAsync(_enumerable._tx, new ReliableListKey { Key = _enumerable._key, Id = _enumerable._metadata.From + _index }).ConfigureAwait(false);
                        if (result.HasValue)
                        {
                            _current = result.Value;
                            _index++;
                            return true;
                        }
                        else
                        {
                            throw new InvalidOperationException($"List is corrupt - index {_index} does not exist");
                        }
                    }
                    else
                    {
                        return false;
                    }
                }

                public void Reset()
                {
                    _index = 0;
                }
            }

            private readonly ITransaction _tx;
            private readonly string _key;
            private readonly ReliableListMetaData _metadata;
            private readonly ReliableLists<TValue> _list;

            public AsyncEnumerable(ITransaction tx, string key, ReliableListMetaData metadata, ReliableLists<TValue> list)
            {
                _tx = tx;
                _key = key;
                _metadata = metadata;
                _list = list;
            }

            public IAsyncEnumerator<TValue> GetAsyncEnumerator() => new AsyncEnumerator(this);
        }

        public async Task<IAsyncEnumerable<TValue>> CreateEnumerableAsync(ITransaction tx, string key)
        {
            var metadata = await GetMetadataAsync(tx, key).ConfigureAwait(false);
            return new AsyncEnumerable(tx, key, metadata, this);
        }
    }
}
