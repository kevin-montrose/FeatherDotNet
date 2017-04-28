using System.Collections;
using System.Collections.Generic;

namespace FeatherDotNet
{
    /// <summary>
    /// Allocation free enumerable for a row.
    /// </summary>
    public struct RowEnumerable : IEnumerable<Row>
    {
        DataFrame Parent;

        internal RowEnumerable(DataFrame parent)
        {
            Parent = parent;
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public RowEnumerator GetEnumerator() => new RowEnumerator(Parent);

        IEnumerator<Row> IEnumerable<Row>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>
    /// Allocation free enumerator for a row.
    /// </summary>
    public struct RowEnumerator : IEnumerator<Row>
    {
        DataFrame Parent;
        long Index;

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}.Current"/>
        /// </summary>
        public Row Current { get; private set; }

        internal RowEnumerator(DataFrame parent)
        {
            Current = default(Row);
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

            Row nextRow;
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
