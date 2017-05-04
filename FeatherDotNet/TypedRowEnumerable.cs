using System.Collections;
using System.Collections.Generic;
using FeatherDotNet.Impl;

namespace FeatherDotNet
{
    /// <summary>
    /// Allocation free enumerator for a typed row.
    /// </summary>
    public struct TypedRowEnumerator<TRow> :
        IEnumerator<TRow>
        where TRow : struct
    {
        TypedDataFrameBase<TRow> Parent;
        long Index;

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}.Current"/>
        /// </summary>
        public TRow Current { get; private set; }

        object IEnumerator.Current => Current;

        internal TypedRowEnumerator(TypedDataFrameBase<TRow> parent)
        {
            Current = default(TRow);
            Parent = parent;
            Index = -1;
        }

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

            TRow row;
            if (!Parent.TryGetRowTranslated(Index, out row)) return false;

            Current = row;
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

    /// <summary>
    /// Allocation free enumerable for a typed row.
    /// </summary>
    public struct TypedRowEnumerable<TRow>: IEnumerable<TRow>
        where TRow: struct
    {
        TypedDataFrameBase<TRow> Parent;

        internal TypedRowEnumerable(TypedDataFrameBase<TRow> parent)
        {
            Parent = parent;
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public TypedRowEnumerator<TRow> GetEnumerator() => new TypedRowEnumerator<TRow>(Parent);

        IEnumerator<TRow> IEnumerable<TRow>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
