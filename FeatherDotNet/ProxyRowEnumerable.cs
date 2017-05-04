using System.Collections;
using System.Collections.Generic;
using FeatherDotNet.Impl;

namespace FeatherDotNet
{
    /// <summary>
    /// Allocation free enumerable for a proxied row.
    /// </summary>
    public struct ProxyRowEnumerable<TProxyType>: IEnumerable<TProxyType>
    {
        ProxyDataFrame<TProxyType> Parent;

        internal ProxyRowEnumerable(ProxyDataFrame<TProxyType> parent)
        {
            Parent = parent;
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public ProxyRowEnumerator<TProxyType> GetEnumerator() => new ProxyRowEnumerator<TProxyType>(Parent);

        IEnumerator<TProxyType> IEnumerable<TProxyType>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>
    /// Allocation free enumerator for a proxied row.
    /// </summary>
    public struct ProxyRowEnumerator<TProxyType> : IEnumerator<TProxyType>
    {
        ProxyDataFrame<TProxyType> Parent;
        long Index;

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}.Current"/>
        /// </summary>
        public TProxyType Current { get; private set; }

        internal ProxyRowEnumerator(ProxyDataFrame<TProxyType> parent)
        {
            Current = default(TProxyType);
            Parent = parent;
            Index = -1;
        }

        object IEnumerator.Current => Current;

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}"/>
        /// </summary>
        public void Dispose()
        {
            Parent = null;
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}"/>
        /// </summary>
        public bool MoveNext()
        {
            Index++;

            TProxyType nextRow;
            if (!Parent.TryGetRowTranslated(Index, out nextRow)) return false;

            Current = nextRow;
            return true;
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}"/>
        /// </summary>
        public void Reset()
        {
            Index = -1;
        }
    }
}
