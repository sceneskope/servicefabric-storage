using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SceneSkope.ServiceFabric.Storage
{
    internal sealed class MappingAsyncEnumerable<TSource, TTarget> : IAsyncEnumerable<TTarget>
    {
        public IAsyncEnumerable<TSource> Source { get; }
        public Func<TSource, TTarget> Mapper { get; }

        public MappingAsyncEnumerable(IAsyncEnumerable<TSource> source, Func<TSource, TTarget> mapper)
        {
            Source = source;
            Mapper = mapper;
        }

        private class MappingAsyncEnumerator : IAsyncEnumerator<TTarget>
        {
            public MappingAsyncEnumerable<TSource, TTarget> Parent { get; }
            public IAsyncEnumerator<TSource> Source { get; }

            public MappingAsyncEnumerator(MappingAsyncEnumerable<TSource, TTarget> parent, IAsyncEnumerator<TSource> source)
            {
                Parent = parent;
                Source = source;
            }

            public TTarget Current => Parent.Mapper(Source.Current);

            public void Dispose() => Source.Dispose();

            public Task<bool> MoveNextAsync(CancellationToken cancellationToken) => Source.MoveNextAsync(cancellationToken);

            public void Reset() => Source.Reset();
        }

        public IAsyncEnumerator<TTarget> GetAsyncEnumerator()
        {
            var sourceEnumerator = Source.GetAsyncEnumerator();
            return new MappingAsyncEnumerator(this, sourceEnumerator);
        }
    }
}
