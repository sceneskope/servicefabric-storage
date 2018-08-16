using System;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Runtime;
using SceneSkope.ServiceFabric.ProtocolBuffers;

namespace SceneSkope.ServiceFabric.Storage
{
    public sealed class ReliableListManager
    {
        private static bool _initialised;

        public static void Initialise(StatefulService serviceBase)
        {
            serviceBase.RegisterProtobufStateSerializer<ReliableListMetaData>()
                .RegisterProtobufStateSerializer<ReliableListKey>();

            _initialised = true;
        }

        public static async Task<ReliableListManager> CreateAsync(IReliableStateManager stateManager)
        {
            if (!_initialised)
            {
                throw new InvalidOperationException("Not initialised");
            }
            var store = await stateManager.GetOrAddAsync<IReliableDictionary2<string, ReliableListMetaData>>("reliableListManager").ConfigureAwait(false);
            return new ReliableListManager(stateManager, store);
        }

        private readonly IReliableStateManager _stateManager;
        private readonly IReliableDictionary2<string, ReliableListMetaData> _metadataStore;

        private ReliableListManager(IReliableStateManager stateManager, IReliableDictionary2<string, ReliableListMetaData> metadataStore)
        {
            _stateManager = stateManager;
            _metadataStore = metadataStore;
        }

        public Task<ReliableLists<TValue>> CreateListsAsync<TValue>(string name) =>
            ReliableLists<TValue>.CreateAsync(_stateManager, _metadataStore, name);
    }
}
