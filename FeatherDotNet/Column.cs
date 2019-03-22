using System;
using System.Collections;
using System.Collections.Generic;
using FeatherDotNet.Impl;

namespace FeatherDotNet
{
    /// <summary>
    /// Allocation free enumerator for a column.
    /// </summary>
    public struct ColumnValueEnumerator : IEnumerator<Value>
    {
        internal DataFrame Parent;
        internal long TranslatedColumnIndex;
        internal long Index;

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}"/>
        /// </summary>
        public Value Current { get; private set; }

        object IEnumerator.Current => Current;

        internal ColumnValueEnumerator(DataFrame parent, long translatedColumnIndex)
        {
            Current = default(Value);
            Parent = parent;
            TranslatedColumnIndex = translatedColumnIndex;
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

            Value value;
            if (!Parent.TryGetValueTranslated(Index, TranslatedColumnIndex, out value)) return false;

            Current = value;
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
    /// Represents a column of a DataFrame.
    /// 
    /// Is untyped, but returned values can be implicitly coerced to built-in types.
    /// </summary>
    public struct Column :
        IEnumerable<Value>,
        IColumn<Value>
    {
        internal DataFrame Parent;
        internal long TranslatedColumnIndex;

        /// <summary>
        /// Returns the Index (in the appropriate basis) of this column in the original dataframe.
        /// </summary>
        public long Index => Parent.UntranslateIndex(TranslatedColumnIndex);
        /// <summary>
        /// Returns the name of this column.
        /// </summary>
        public string Name => Parent.Metadata.Columns[TranslatedColumnIndex].Name;
        /// <summary>
        /// Returns the .NET equivalent type of this column.
        /// </summary>
        public Type Type => Parent.Metadata.Columns[TranslatedColumnIndex].MappedType;
        /// <summary>
        /// Returns the number of entries in the this column.
        /// This will always match the number of rows in the original dataframe.
        /// </summary>
        public long Length => Parent.RowCount;

        /// <summary>
        /// Return the value at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long rowIndex]
        {
            get
            {
                var translatedRowIndex = Parent.TranslateIndex(rowIndex);

                Value value;
                if (!Parent.TryGetValueTranslated(translatedRowIndex, TranslatedColumnIndex, out value))
                {
                    long minLegal;
                    long maxLegal;
                    switch (Parent.Basis)
                    {
                        case BasisType.One:
                            minLegal = 1;
                            maxLegal = Parent.Metadata.NumRows;
                            break;
                        case BasisType.Zero:
                            minLegal = 0;
                            maxLegal = Parent.Metadata.NumRows - 1;
                            break;
                        default: throw new InvalidOperationException($"Unexpected Basis: {Parent.Basis}");
                    }

                    throw new ArgumentOutOfRangeException($"Row index out of range, valid between [{minLegal}, {maxLegal}] found {rowIndex}");
                }

                return value;
            }
        }

        internal ColumnType OnDiskType => Parent.Metadata.Columns[TranslatedColumnIndex].Type;

        /// <summary>
        /// <see cref="System.Collections.Generic.IList{T}"/>
        /// 
        /// Throws if <see cref="Length"/> will not fit in an int.
        /// </summary>
        public int Count => checked((int) Length);
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
        public Value this[int index]
        {
            get
            {
                return this[(long)index];
            }
            set
            {
                throw new NotSupportedException("Column is ReadOnly");
            }
        }

        internal Column(DataFrame parent, long translatedColumnIndex)
        {
            Parent = parent;
            TranslatedColumnIndex = translatedColumnIndex;
        }

        /// <summary>
        /// Converts this column to an array of the specified type.
        /// 
        /// Throws if the column cannot be coerced to the given type, or cannot fit in an array.
        /// </summary>
        public T[] ToArray<T>()
        {
            if (Length > int.MaxValue) throw new InvalidOperationException($"Length ({Length:N0}) greater that int.MaxValue, can't possibly fit in a single array");

            T[] ret = null;
            GetRange(Parent.UntranslateIndex(0), (int)Length, ref ret);
            return ret;
        }

        /// <summary>
        /// Converts this column to an array of <see cref="Value"/>.
        /// 
        /// Throws if the column cannot fit in an array.
        /// </summary>
        public Value[] ToArray() => ToArray<Value>();

        /// <summary>
        /// Converts a subset of this column to an array of the specified type.
        /// 
        /// Throws if the column cannot be coerced to the given type, or the subset cannot fit in an array.
        /// </summary>
        public T[] GetRange<T>(long rowIndex, int length)
        {
            T[] ret = null;
            GetRange(rowIndex, length, ref ret);
            return ret;
        }

        /// <summary>
        /// Converts a subset of this column to an array of <see cref="Value"/>.
        /// 
        /// Throws if the subset cannot fit in an array.
        /// </summary>
        public Value[] GetRange(long rowIndex, int length) => GetRange<Value>(rowIndex, length);

        /// <summary>
        /// Converts a subset of this column to an array of the specified type.
        /// 
        /// The array is stored at index 0 in the passed array reference, which is initialized or resized if needed.
        /// </summary>
        public void ToArray<T>(ref T[] array)
        {
            if (Length > int.MaxValue) throw new InvalidOperationException($"Length ({Length:N0}) greater that int.MaxValue, can't possibly fit in a single array");

            GetRange(Parent.UntranslateIndex(0), (int)Length, ref array);
        }

        /// <summary>
        /// Converts this column to an array of <see cref="Value"/>.
        /// 
        /// The array is stored at index 0 in the passed array reference, which is initialized or resized if needed.
        /// </summary>
        public void ToArray(ref Value[] array) => ToArray<Value>(ref array);

        /// <summary>
        /// Converts a subset of this column to an array of the specified type.
        /// 
        /// The column subset starts at the given index (in the dataframe's basis) and is of the given length.
        /// 
        /// The array is stored at index 0 in the passed array reference, which is initialized or resized if needed.
        /// 
        /// Throws if the column cannot be coerced to the given type, or the subset cannot fit in an array.
        /// </summary>
        public void GetRange<T>(long rowSourceIndex, int length, ref T[] array)
        {
            GetRange(rowSourceIndex, length, ref array, 0);
        }

        /// <summary>
        /// Converts a subset of this column to an array of <see cref="Value"/>.
        /// 
        /// The column subset starts at the given index (in the dataframe's basis) and is of the given length.
        /// 
        /// The array is stored at index 0 in the passed array reference, which is initialized or resized if needed.
        /// </summary>
        public void GetRange(long rowSourceIndex, int length, ref Value[] array) => GetRange<Value>(rowSourceIndex, length, ref array);

        /// <summary>
        /// Converts a subset of this column to an array of the specified type.
        /// 
        /// The column subset starts at the given index (in the dataframe's basis) and is of the given length.
        /// 
        /// The array is stored at the given index in the passed array reference, which is initialized or resized if needed.
        /// 
        /// Throws if the column cannot be coerced to the given type, or the subset cannot fit in an array.
        /// </summary>
        public void GetRange<T>(long rowSourceIndex, int length, ref T[] array, int destinationIndex)
        {
            if (!OnDiskType.CanMapTo(typeof(T), Parent.Metadata.Columns[TranslatedColumnIndex].CategoryLevels))
            {
                throw new InvalidOperationException($"Cannot convert {Type.Name} to {typeof(T).Name}");
            }

            var translatedIndex = Parent.TranslateIndex(rowSourceIndex);

            if (translatedIndex < 0 || translatedIndex >= Length) throw new ArgumentOutOfRangeException(nameof(rowSourceIndex));

            if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));

            var lastElem = translatedIndex + length;
            if (lastElem > Length) throw new ArgumentOutOfRangeException(nameof(length));

            if (destinationIndex < 0) throw new ArgumentOutOfRangeException(nameof(destinationIndex));

            var arraySize = destinationIndex + length;

            if (array == null)
            {
                array = new T[arraySize];
            }
            else
            {
                if (array.Length < arraySize)
                {
                    Array.Resize(ref array, arraySize);
                }
            }

            if (CanBeBlitted<T>())
            {
                if (typeof(T) == Type) {
                    Parent.UnsafeFastGetRowRange<T>(translatedIndex, TranslatedColumnIndex, array, destinationIndex, length);
                } else {
                    Parent.GetRowRangeWithTypeCast<T>(translatedIndex, TranslatedColumnIndex, array, destinationIndex, length);
                }
                return;
            }

            var category = Parent.Metadata.Columns[TranslatedColumnIndex].GetCategoryEnumMap<T>();

            for (var i = 0; i < length; i++)
            {
                var value = this[rowSourceIndex + i];
                array[destinationIndex + i] = value.UnsafeCast<T>(category);
            }
        }

        /// <summary>
        /// Converts a subset of this column to an array of <see cref="Value"/>.
        /// 
        /// The column subset starts at the given index (in the dataframe's basis) and is of the given length.
        /// 
        /// The array is stored at the given index in the passed array reference, which is initialized or resized if needed.
        /// 
        /// Throws if the column cannot be coerced to the given type, or the subset cannot fit in an array.
        /// </summary>
        public void GetRange(long rowSourceIndex, int length, ref Value[] array, int destinationIndex) => GetRange<Value>(rowSourceIndex, length, ref array, destinationIndex);

        /// <summary>
        /// Sets value to the value of the row at the passed index (in the dataframe's basis), having coerced it to the appropriate type if possible.
        /// 
        /// If the passed index is out of bounds, or the coercing fails, false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue<T>(long rowIndex, out T value)
        {
            Value rawValue;
            if (!TryGetValue(rowIndex, out rawValue))
            {
                value = default(T);
                return false;
            }

            var columnDetails = Parent.Metadata.Columns[TranslatedColumnIndex];
            if (!columnDetails.CanMapTo(typeof(T)))
            {
                value = default(T);
                return false;
            }

            var category = columnDetails.GetCategoryEnumMap<T>();
            value = rawValue.UnsafeCast<T>(category);
            return true;
        }

        /// <summary>
        /// Sets value to the <see cref="Value"/> of the row at the passed index (in the dataframe's basis).
        /// 
        /// If the passed index is out of bounds false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue(long rowIndex, out Value value)
        {
            var translatedRowIndex = Parent.TranslateIndex(rowIndex);

            return TryGetValueTranslated(translatedRowIndex, out value);
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public ColumnValueEnumerator GetEnumerator() => new ColumnValueEnumerator(Parent, TranslatedColumnIndex);

        IEnumerator<Value> IEnumerable<Value>.GetEnumerator() => GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        
        /// <summary>
        /// <see cref="Object.Equals(object)"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            if (!(obj is Column)) return false;

            var other = (Column)obj;
            return
                object.ReferenceEquals(other.Parent, Parent) &&
                other.TranslatedColumnIndex == TranslatedColumnIndex;
        }

        /// <summary>
        /// <see cref="Object.GetHashCode"/>
        /// </summary>
        public override int GetHashCode()
        {
            return
                Parent.GetHashCode() * 17 +
                TranslatedColumnIndex.GetHashCode();
        }
        /// <summary>
        /// <see cref="Object.ToString"/>
        /// </summary>
        public override string ToString()
        {
            return $"Column \"{Name}\" Index = {Index}";
        }

        /// <summary>
        /// Converts the column to a typed column of the given type.
        /// 
        /// Throws if the conversion isn't allowed.
        /// </summary>
        public TypedColumn<T> Cast<T>()
        {
            if (!OnDiskType.CanMapTo(typeof(T), Parent.Metadata.Columns[TranslatedColumnIndex].CategoryLevels))
            {
                throw new InvalidCastException($"Cannot convert {Type.Name} to {typeof(T).Name}");
            }

            var typedColumn = new TypedColumn<T>(this);
            return typedColumn;
        }
        
        /// <summary>
        /// Finds a given value in the column.  Throws if the index will not fit in an int.
        /// </summary>
        public int IndexOf(Value item)
        {
            var ret = LongIndexOf(item);

            if (ret > Int32.MaxValue) throw new InvalidOperationException($"Index {ret:N0} exceeded Int32.MaxValue");

            return (int)ret;
        }

        /// <summary>
        /// Finds a given value in the column.
        /// </summary>
        public long LongIndexOf(Value item)
        {
            long ix = 0;
            foreach(var val in this)
            {
                if (item.Equals(val)) return ix;
                ix++;
            }

            return -1;
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public void Insert(int index, Value item)
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
        public void Add(Value item)
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
        /// <see cref="System.Collections.Generic.ICollection{T}.Contains(T)"/>
        /// </summary>
        public bool Contains(Value item) => LongIndexOf(item) != -1;

        /// <summary>
        /// <see cref="System.Collections.Generic.ICollection{T}.CopyTo(T[], int)"/>
        /// </summary>
        public void CopyTo(Value[] array, int arrayIndex)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (arrayIndex < 0) throw new ArgumentOutOfRangeException(nameof(arrayIndex));
            if (Length > Int32.MaxValue) throw new InvalidOperationException($"Column is too large {Length:N0} to copy into an array");
            if (array.Length < Length) throw new ArgumentException($"Column (size: {Length:N0}) cannot fit in array (size: {array.Length:N0})");

            GetRange(Parent.UntranslateIndex(0), (int)Length, ref array, arrayIndex);
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public bool Remove(Value item)
        {
            throw new NotSupportedException("Column is ReadOnly");
        }

        /// <summary>
        /// Equivalent to <see cref="ToArray()"/>
        /// </summary>
        public static explicit operator Value[](Column col) => col.ToArray();

        internal bool TryGetValueTranslated(long translatedRowIndex, out Value value)
        {
            return Parent.TryGetValueTranslated(translatedRowIndex, TranslatedColumnIndex, out value);
        }
        
        // this is generic because, in theory, the JIT can turn this into a no-op
        static bool CanBeBlitted<T>()
        {
            if (typeof(T) == typeof(byte)) return true;
            if (typeof(T) == typeof(sbyte)) return true;
            if (typeof(T) == typeof(short)) return true;
            if (typeof(T) == typeof(ushort)) return true;
            if (typeof(T) == typeof(int)) return true;
            if (typeof(T) == typeof(uint)) return true;
            if (typeof(T) == typeof(long)) return true;
            if (typeof(T) == typeof(ulong)) return true;
            if (typeof(T) == typeof(float)) return true;
            if (typeof(T) == typeof(double)) return true;

            return false;
        }
    }
}
