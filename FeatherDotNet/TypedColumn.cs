using System;
using System.Collections;
using System.Collections.Generic;

namespace FeatherDotNet
{
    /// <summary>
    /// Allocation free enumerator for a typed column.
    /// </summary>
    public struct TypedColumnEnumerator<TColumnType>: IEnumerator<TColumnType>
    {
        Column Inner;
        long Index;

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}"/>
        /// </summary>
        public TColumnType Current { get; private set; }

        object IEnumerator.Current => Current;

        internal TypedColumnEnumerator(Column inner)
        {
            Current = default(TColumnType);
            Inner = inner;
            Index = -1;
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}"/>
        /// </summary>
        public void Dispose() { }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}"/>
        /// </summary>
        public bool MoveNext()
        {
            Index++;

            Value value;
            if (!Inner.TryGetValueTranslated(Index, out value)) return false;

            var category = Inner.Parent.Metadata.Columns[Inner.TranslatedColumnIndex].GetCategoryEnumMap<TColumnType>();

            Current = value.UnsafeCast<TColumnType>(category);
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
    /// Represents a column of a TypedDataFrame, or a column of a DataFrame that has been mapped.
    /// 
    /// Typing is validated eagerly, but coercision of particular values is done lazily.
    /// </summary>
    public struct TypedColumn<TColumnType>: 
        IEnumerable<TColumnType>,
        IColumn<TColumnType>
    {
        Column Inner;

        /// <summary>
        /// Returns the Index (in the appropriate basis) of this column in the original dataframe.
        /// </summary>
        public long Index => Inner.Index;
        /// <summary>
        /// Returns the name of this column.
        /// </summary>
        public string Name => Inner.Name;
        /// <summary>
        /// Returns the .NET equivalent type of this column.
        /// </summary>
        public Type Type => Inner.Type;
        /// <summary>
        /// Returns the number of entries in the this column.
        /// This will always match the number of rows in the original dataframe.
        /// </summary>
        public long Length => Inner.Length;

        /// <summary>
        /// <see cref="System.Collections.Generic.IList{T}"/>
        /// 
        /// Throws if <see cref="Length"/> will not fit in an int.
        /// </summary>
        public int Count => checked((int)Length);

        /// <summary>
        /// <see cref="System.Collections.Generic.IList{T}"/>
        /// 
        /// Always return true.
        /// </summary>
        public bool IsReadOnly => true;

        /// <summary>
        /// <see cref="this[long]"/>
        /// <see cref="System.Collections.Generic.IList{T}"/>
        /// </summary>
        public TColumnType this[int index]
        {
            get
            {
                return this[(long)index];
            }
            set
            {
                throw new NotSupportedException("TypedColumn is ReadOnly");
            }
        }

        /// <summary>
        /// Return the value at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, out TColumnType)"/> for non-throwing gets.
        /// </summary>
        /// <param name="rowIndex">The index of the value to get, in the appropriate basis.</param>
        public TColumnType this[long rowIndex]
        {
            get
            {
                var category = Inner.Parent.Metadata.Columns[Inner.TranslatedColumnIndex].GetCategoryEnumMap<TColumnType>();

                return Inner[rowIndex].UnsafeCast<TColumnType>(category);
            }
        }

        internal TypedColumn(Column inner)
        {
            Inner = inner;
        }

        /// <summary>
        /// Converts this column to an array of TColumnType.
        /// 
        /// Throws if the column cannot fit in an array.
        /// </summary>
        public TColumnType[] ToArray() => Inner.ToArray<TColumnType>();

        /// <summary>
        /// Converts a subset of this column to an array of TColumnType.
        /// 
        /// Throws if the subset cannot fit in an array.
        /// </summary>
        public TColumnType[] GetRange(long index, int length) => Inner.GetRange<TColumnType>(index, length);

        /// <summary>
        /// Converts a subset of this column to an array of TColumnType.
        /// 
        /// The array is stored at index 0 in the passed array reference, which is initialized or resized if needed.
        /// </summary>
        public void ToArray(ref TColumnType[] array) => Inner.ToArray(ref array);
        /// <summary>
        /// Converts this column to an array of <see cref="Value"/>.
        /// 
        /// Throws if the column cannot fit in an array.
        /// </summary>
        public void ToArray(ref Value[] array) => Inner.ToArray(ref array);

        /// <summary>
        /// Converts a subset of this column to an array of TColumnType.
        /// 
        /// The column subset starts at the given index (in the dataframe's basis) and is of the given length.
        /// 
        /// The array is stored at index 0 in the passed array reference, which is initialized or resized if needed.
        /// 
        /// Throws if the subset cannot fit in an array.
        /// </summary>
        public void GetRange(long sourceIndex, int length, ref TColumnType[] array) => Inner.GetRange(sourceIndex, length, ref array);
        /// <summary>
        /// Converts a subset of this column to an array of <see cref="Value"/>.
        /// 
        /// The column subset starts at the given index (in the dataframe's basis) and is of the given length.
        /// 
        /// The array is stored at index 0 in the passed array reference, which is initialized or resized if needed.
        /// </summary>
        public void GetRange(long sourceIndex, int length, ref Value[] array) => Inner.GetRange(sourceIndex, length, ref array);

        /// <summary>
        /// Converts a subset of this column to an array of TColumnType.
        /// 
        /// The column subset starts at the given index (in the dataframe's basis) and is of the given length.
        /// 
        /// The array is stored at the given index in the passed array reference, which is initialized or resized if needed.
        /// 
        /// Throws if the subset cannot fit in an array.
        /// </summary>
        public void GetRange(long sourceIndex, int length, ref TColumnType[] array, int destinationIndex) => Inner.GetRange(sourceIndex, length, ref array, destinationIndex);

        /// <summary>
        /// Converts a subset of this column to an array of <see cref="Value"/>.
        /// 
        /// The column subset starts at the given index (in the dataframe's basis) and is of the given length.
        /// 
        /// The array is stored at the given index in the passed array reference, which is initialized or resized if needed.
        /// 
        /// Throws if the column cannot be coerced to the given type, or the subset cannot fit in an array.
        /// </summary>
        public void GetRange(long sourceIndex, int length, ref Value[] array, int destinationIndex) => Inner.GetRange(sourceIndex, length, ref array, destinationIndex);

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public TypedColumnEnumerator<TColumnType> GetEnumerator() => new TypedColumnEnumerator<TColumnType>(Inner);

        IEnumerator<TColumnType> IEnumerable<TColumnType>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Sets value to the value of the row at the passed index (in the dataframe's basis).
        /// 
        /// If the passed index is out of bounds false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue(long rowIndex, out TColumnType value)
        {
            Value innerValue;
            if(!Inner.TryGetValue(rowIndex, out innerValue))
            {
                value = default(TColumnType);
                return false;
            }

            var category = Inner.Parent.Metadata.Columns[Inner.TranslatedColumnIndex].GetCategoryEnumMap<TColumnType>();

            value = innerValue.UnsafeCast<TColumnType>(category);
            return true;
        }

        /// <summary>
        /// Converts this column to an array of the specified type.
        /// 
        /// Throws if the column cannot be coerced to the given type, or cannot fit in an array.
        /// </summary>
        public void ToArray<V>(ref V[] array) => Inner.ToArray(ref array);

        /// <summary>
        /// <see cref="Column.GetRange{T}(long, int)"/>
        /// </summary>
        public void GetRange<V>(long rowSourceIndex, int length, ref V[] array) => Inner.GetRange(rowSourceIndex, length, ref array);

        /// <summary>
        /// <see cref="Column.GetRange(long, int, ref Value[], int)"/>
        /// </summary>
        public void GetRange<V>(long rowSourceIndex, int length, ref V[] array, int destinationIndex) => Inner.GetRange(rowSourceIndex, length, ref array, destinationIndex);

        /// <summary>
        /// <see cref="Column.TryGetValue{T}(long, out T)"/>
        /// </summary>
        public bool TryGetValue<V>(long rowIndex, out V value) => Inner.TryGetValue(rowIndex, out value);

        /// <summary>
        /// <see cref="Column.TryGetValue(long, out Value)"/>
        /// </summary>
        public bool TryGetValue(long rowIndex, out Value value) => Inner.TryGetValue(rowIndex, out value);

        /// <summary>
        /// <see cref="Object.Equals(object)"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            if (!(obj is TypedColumn<TColumnType>)) return false;

            var other = (TypedColumn<TColumnType>)obj;
            return other.Inner.Equals(Inner);
        }

        /// <summary>
        /// <see cref="Object.GetHashCode"/>
        /// </summary>
        public override int GetHashCode()
        {
            return
                Inner.GetHashCode() * 17 +
                typeof(TColumnType).GetHashCode();
        }
        /// <summary>
        /// <see cref="Object.ToString"/>
        /// </summary>
        public override string ToString()
        {
            return $"TypedColumn<{typeof(TColumnType).Name}> \"{Name}\" Index = {Index}";
        }

        /// <summary>
        /// Finds a given value in the column.  Throws if the index will not fit in an int.
        /// </summary>
        public int IndexOf(TColumnType item)
        {
            var ret = LongIndexOf(item);

            if (ret > Int32.MaxValue) throw new InvalidOperationException($"Index {ret:N0} exceeded Int32.MaxValue");

            return (int)ret;
        }

        /// <summary>
        /// Finds a given value in the column.
        /// </summary>
        public long LongIndexOf(TColumnType item)
        {
            var itemIsNull = false;
            if (!typeof(TColumnType).IsValueType)
            {
                itemIsNull = item == null;
            }

            long ix = 0;
            foreach (var val in this)
            {
                bool valIsNull = false;
                if (!typeof(TColumnType).IsValueType)
                {
                    valIsNull = val == null;
                }

                if (itemIsNull)
                {
                    if (valIsNull) return ix;
                }
                else
                {
                    if (item.Equals(val)) return ix;
                }
                ix++;
            }

            return -1;
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public void Insert(int index, TColumnType item)
        {
            throw new NotSupportedException("Column is ReadOnly");
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public void RemoveAt(int index)
        {
            throw new NotSupportedException("Column is ReadOnly");
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public void Add(TColumnType item)
        {
            throw new NotSupportedException("Column is ReadOnly");
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public void Clear()
        {
            throw new NotSupportedException("Column is ReadOnly");
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public bool Remove(TColumnType item)
        {
            throw new NotSupportedException("Column is ReadOnly");
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.ICollection{T}.Contains(T)"/>
        /// </summary>
        public bool Contains(TColumnType item) => LongIndexOf(item) != -1;

        /// <summary>
        /// <see cref="System.Collections.Generic.ICollection{T}.CopyTo(T[], int)"/>
        /// </summary>
        public void CopyTo(TColumnType[] array, int arrayIndex)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (arrayIndex < 0) throw new ArgumentOutOfRangeException(nameof(arrayIndex));
            if (Length > Int32.MaxValue) throw new InvalidOperationException($"Column is too large {Length:N0} to copy into an array");
            if (array.Length < Length) throw new ArgumentException($"Column (size: {Length:N0}) cannot fit in array (size: {array.Length:N0})");

            GetRange(Inner.Parent.UntranslateIndex(0), (int)Length, ref array, arrayIndex);
        }

        /// <summary>
        /// Equivalent to <see cref="ToArray()"/>
        /// </summary>
        public static explicit operator Value[] (TypedColumn<TColumnType> col)
        {
            Value[] ret = null;
            col.ToArray(ref ret);
            return ret;
        }

        /// <summary>
        /// Equivalent to <see cref="ToArray()"/>
        /// </summary>
        public static explicit operator TColumnType[](TypedColumn<TColumnType> col) => col.ToArray();
    }
}
