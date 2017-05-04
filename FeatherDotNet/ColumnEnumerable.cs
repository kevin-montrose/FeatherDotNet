using System.Collections;
using System.Collections.Generic;
using FeatherDotNet.Impl;

namespace FeatherDotNet
{
    /// <summary>
    /// Allocation free enumerable for the columns in a dataframe.
    /// </summary>
    public struct ColumnEnumerable : IEnumerable<Column>
    {
        DataFrame Parent;

        internal ColumnEnumerable(DataFrame parent)
        {
            Parent = parent;
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public ColumnEnumerator GetEnumerator() => new ColumnEnumerator(Parent);

        IEnumerator<Column> IEnumerable<Column>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>
    /// Allocation free enumerator for the columns in a dataframe.
    /// </summary>
    public struct ColumnEnumerator : IEnumerator<Column>
    {
        DataFrame Parent;
        long Index;

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}.Current"/>
        /// </summary>
        public Column Current { get; private set; }

        internal ColumnEnumerator(DataFrame parent)
        {
            Current = default(Column);
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

            Column nextColumn;
            if (!Parent.TryGetColumnTranslated(Index, out nextColumn)) return false;

            Current = nextColumn;
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
