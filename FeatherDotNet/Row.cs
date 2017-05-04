using System;
using System.Collections;
using System.Collections.Generic;
using FeatherDotNet.Impl;

namespace FeatherDotNet
{
    /// <summary>
    /// Allocation free enumerator for a row.
    /// </summary>
    public struct RowValueEnumerator : IEnumerator<Value>
    {
        internal DataFrame Parent;
        internal long TranslatedRowIndex;
        internal long Index;

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}"/>
        /// </summary>
        public Value Current { get; private set; }

        object IEnumerator.Current => Current;

        internal RowValueEnumerator(DataFrame parent, long translatedRowIndex)
        {
            Current = default(Value);
            Parent = parent;
            TranslatedRowIndex = translatedRowIndex;
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
            if (!Parent.TryGetValueTranslated(TranslatedRowIndex, Index, out value)) return false;

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
    /// Represents a row of a DataFrame.
    /// 
    /// Is untyped, but returned values can be implicitly coerced to built-in types.
    /// </summary>
    public struct Row: 
        IEnumerable<Value>,
        IRow
    {
        internal DataFrame Parent;
        internal long TranslatedRowIndex;

        /// <summary>
        /// Returns the Index (in the appropriate basis) of this row in the original dataframe.
        /// </summary>
        public long Index => Parent.UntranslateIndex(TranslatedRowIndex);
        /// <summary>
        /// Returns the number of entries in the this column.
        /// This will always match the number of columns in the original dataframe.
        /// </summary>
        public long Length => Parent.ColumnCount;

        /// <summary>
        /// Return the value at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long columnIndex]
        {
            get
            {
                var translatedColumnIndex = Parent.TranslateIndex(columnIndex);

                Value value;
                if(!Parent.TryGetValueTranslated(TranslatedRowIndex, translatedColumnIndex, out value))
                {
                    var rowIndex = Parent.UntranslateIndex(TranslatedRowIndex);

                    long minRowIx, minColIx, maxRowIx, maxColIx;
                    switch (Parent.Basis)
                    {
                        case BasisType.One:
                            minRowIx = 1;
                            maxRowIx = Parent.Metadata.NumRows;
                            minColIx = 1;
                            maxColIx = Parent.Metadata.Columns.Length;
                            break;
                        case BasisType.Zero:
                            minRowIx = 0;
                            maxRowIx = Parent.Metadata.NumRows - 1;
                            minColIx = 0;
                            maxColIx = Parent.Metadata.Columns.Length - 1;
                            break;
                        default: throw new InvalidOperationException($"Unexpected Basis: {Parent.Basis}");
                    }

                    throw new ArgumentOutOfRangeException($"Address out of range, legal range is [{minRowIx}, {minColIx}] - [{maxRowIx}, {maxColIx}], found [{rowIndex}, {columnIndex}]");
                }

                return value;
            }
        }

        /// <summary>
        /// Return the value in the column with the given name.
        /// 
        /// Will throw if no column with that name exists .  Use <see cref="TryGetValue(string, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[string columnName]
        {
            get
            {
                long translatedColumnIndex;
                if(!Parent.TryLookupTranslatedColumnIndex(columnName, out translatedColumnIndex))
                {
                    throw new KeyNotFoundException($"Could not find column with name \"{columnName}\"");
                }

                Value value;
                if (!Parent.TryGetValueTranslated(TranslatedRowIndex, translatedColumnIndex, out value))
                {
                    var rowIndex = Parent.UntranslateIndex(TranslatedRowIndex);
                    var columnIndex = Parent.UntranslateIndex(translatedColumnIndex);

                    long minRowIx, minColIx, maxRowIx, maxColIx;
                    switch (Parent.Basis)
                    {
                        case BasisType.One:
                            minRowIx = 1;
                            maxRowIx = Parent.Metadata.NumRows;
                            minColIx = 1;
                            maxColIx = Parent.Metadata.Columns.Length;
                            break;
                        case BasisType.Zero:
                            minRowIx = 0;
                            maxRowIx = Parent.Metadata.NumRows - 1;
                            minColIx = 0;
                            maxColIx = Parent.Metadata.Columns.Length - 1;
                            break;
                        default: throw new InvalidOperationException($"Unexpected Basis: {Parent.Basis}");
                    }

                    throw new ArgumentOutOfRangeException($"Address out of range, legal range is [{minRowIx}, {minColIx}] - [{maxRowIx}, {maxColIx}], found [{rowIndex}, {columnIndex}]");
                }

                return value;
            }
        }
        
        internal Row(DataFrame parent, long translatedRowIndex)
        {
            Parent = parent;
            TranslatedRowIndex = translatedRowIndex;
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public RowValueEnumerator GetEnumerator() => new RowValueEnumerator(Parent, TranslatedRowIndex);

        IEnumerator<Value> IEnumerable<Value>.GetEnumerator() => GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Sets value to the <see cref="Value"/> of the column at the passed index (in the dataframe's basis).
        /// 
        /// If the passed index is out of bounds false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue(long columnIndex, out Value value)
        {
            var translatedColumnIndex = Parent.TranslateIndex(columnIndex);
            return Parent.TryGetValueTranslated(TranslatedRowIndex, translatedColumnIndex, out value);
        }

        /// <summary>
        /// Sets value to the value of the column at the passed index (in the dataframe's basis), having coerced it to the appropriate type if possible.
        /// 
        /// If the passed index is out of bounds, or the coercing fails, false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue<T>(long columnIndex, out T value)
        {
            Value rawValue;
            if(!TryGetValue(columnIndex, out rawValue))
            {
                value = default(T);
                return false;
            }

            var columnSpec = Parent.Metadata.Columns[rawValue.TranslatedColumnIndex];

            if (!columnSpec.CanMapTo(typeof(T)))
            {
                value = default(T);
                return false;
            }

            value = rawValue.UnsafeCast<T>(columnSpec.GetCategoryEnumMap<T>());
            return true;
        }

        /// <summary>
        /// Sets value to the <see cref="Value"/> of the column with the passed name.
        /// 
        /// If the passed index is out of bounds false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue(string columnName, out Value value)
        {
            long translatedColumnIndex;
            if(!Parent.TryLookupTranslatedColumnIndex(columnName, out translatedColumnIndex))
            {
                value = default(Value);
                return false;
            }

            return Parent.TryGetValueTranslated(TranslatedRowIndex, translatedColumnIndex, out value);
        }

        /// <summary>
        /// Sets value to the value of the column with the given name, having coerced it to the appropriate type if possible.
        /// 
        /// If the passed index is out of bounds, or the coercing fails, false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue<T>(string columnName, out T value)
        {
            Value rawValue;
            if (!TryGetValue(columnName, out rawValue))
            {
                value = default(T);
                return false;
            }

            var columnSpec = Parent.Metadata.Columns[rawValue.TranslatedColumnIndex];

            if (!columnSpec.CanMapTo(typeof(T)))
            {
                value = default(T);
                return false;
            }

            value = rawValue.UnsafeCast<T>(columnSpec.GetCategoryEnumMap<T>());
            return true;
        }

        /// <summary>
        /// Converts this column to an array of <see cref="Value"/>.
        /// 
        /// Throws if the row cannot fit in an array.
        /// </summary>
        public Value[] ToArray()
        {
            Value[] ret = null;
            ToArray(ref ret);
            return ret;
        }

        /// <summary>
        /// Converts this row to an array of <see cref="Value"/>.
        /// 
        /// The array is stored at index 0 in the passed array reference, which is initialized or resized if needed.
        /// </summary>
        public void ToArray(ref Value[] array) => GetRange(Parent.UntranslateIndex(0), (int)Length, ref array);

        /// <summary>
        /// Converts a subset of this row to an array of <see cref="Value"/>.
        /// 
        /// Throws if the subset cannot fit in an array.
        /// </summary>
        public Value[] GetRange(long columnIndex, int length)
        {
            Value[] ret = null;
            GetRange(columnIndex, length, ref ret);
            return ret;
        }

        /// <summary>
        /// Converts a subset of this row to an array of <see cref="Value"/>.
        /// 
        /// The subset starts at the given index (in the dataframe's basis) and is of the given length.
        /// 
        /// The array is stored at index 0 in the passed array reference, which is initialized or resized if needed.
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array) => GetRange(columnSourceIndex, length, ref array, 0);

        /// <summary>
        /// Converts a subset of this row to an array of <see cref="Value"/>.
        /// 
        /// The subset starts at the given index (in the dataframe's basis) and is of the given length.
        /// 
        /// The array is stored at the given index in the passed array reference, which is initialized or resized if needed.
        /// 
        /// Throws if subset cannot fit in an array.
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array, int destinationIndex)
        {
            var translatedColumnIndex = Parent.TranslateIndex(columnSourceIndex);

            if (translatedColumnIndex < 0 || translatedColumnIndex >= Length) throw new ArgumentOutOfRangeException(nameof(columnSourceIndex));

            if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));

            var lastElem = translatedColumnIndex + length;
            if (lastElem > Length) throw new ArgumentOutOfRangeException(nameof(length));

            if (destinationIndex < 0) throw new ArgumentOutOfRangeException(nameof(destinationIndex));

            var arraySize = destinationIndex + length;

            if (array == null)
            {
                array = new Value[arraySize];
            }
            else
            {
                if (array.Length < arraySize)
                {
                    Array.Resize(ref array, arraySize);
                }
            }
            
            for (var i = 0; i < length; i++)
            {
                var value = this[columnSourceIndex + i];
                array[destinationIndex + i] = value;
            }
        }

        /// <summary>
        /// <see cref="Object.Equals(object)"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            if (!(obj is Row)) return false;

            var other = (Row)obj;
            return
                object.ReferenceEquals(other.Parent, Parent) &&
                other.TranslatedRowIndex == TranslatedRowIndex;
        }

        /// <summary>
        /// <see cref="Object.GetHashCode"/>
        /// </summary>
        public override int GetHashCode()
        {
            return
                Parent.GetHashCode() * 17 +
                TranslatedRowIndex.GetHashCode();
        }

        /// <summary>
        /// <see cref="Object.ToString"/>
        /// </summary>
        public override string ToString()
        {
            return $"Row Index = {Parent.UntranslateIndex(TranslatedRowIndex)}";
        }

        /// <summary>
        /// Maps this row to a <see cref="TypedRow{TCol1}"/>.
        /// </summary>
        public TypedRow<TCol1> Map<TCol1>()
        {
            if(Parent.ColumnCount < 1) throw new ArgumentException($"Cannot map row, mapping has 1 column while row has {Parent.ColumnCount:N0} columns");

            var badMap =
                !Parent.Metadata.Columns[0].CanMapTo(typeof(TCol1));

            if (badMap)
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {Parent.Metadata.Columns[0].MappedType.Name}");
            }

            return new TypedRow<TCol1>(this);
        }

        /// <summary>
        /// Maps this row to a <see cref="TypedRow{TCol1, TCol2}"/>.
        /// </summary>
        public TypedRow<TCol1, TCol2> Map<TCol1, TCol2>()
        {
            if (Parent.ColumnCount < 2) throw new ArgumentException($"Cannot map row, mapping has 2 columns while row has {Parent.ColumnCount:N0} columns");

            var badMap =
                !Parent.Metadata.Columns[0].CanMapTo(typeof(TCol1)) ||
                !Parent.Metadata.Columns[1].CanMapTo(typeof(TCol2));

            if (badMap)
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {Parent.Metadata.Columns[0].MappedType.Name}, {typeof(TCol2).Name} = {Parent.Metadata.Columns[1].MappedType.Name}");
            }

            return new TypedRow<TCol1, TCol2>(this);
        }

        /// <summary>
        /// Maps this row to a <see cref="TypedRow{TCol1, TCol2, TCol3}"/>.
        /// </summary>
        public TypedRow<TCol1, TCol2, TCol3> Map<TCol1, TCol2, TCol3>()
        {
            if (Parent.ColumnCount < 3) throw new ArgumentException($"Cannot map row, mapping has 3 columns while row has {Parent.ColumnCount:N0} columns");

            var badMap =
                !Parent.Metadata.Columns[0].CanMapTo(typeof(TCol1)) ||
                !Parent.Metadata.Columns[1].CanMapTo(typeof(TCol2)) ||
                !Parent.Metadata.Columns[2].CanMapTo(typeof(TCol3));

            if (badMap)
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {Parent.Metadata.Columns[0].MappedType.Name}, {typeof(TCol2).Name} = {Parent.Metadata.Columns[1].MappedType.Name}, {typeof(TCol3).Name} = {Parent.Metadata.Columns[2].MappedType.Name}");
            }

            return new TypedRow<TCol1, TCol2, TCol3>(this);
        }

        /// <summary>
        /// Maps this row to a <see cref="TypedRow{TCol1, TCol2, TCol3, TCol4}"/>.
        /// </summary>
        public TypedRow<TCol1, TCol2, TCol3, TCol4> Map<TCol1, TCol2, TCol3, TCol4>()
        {
            if (Parent.ColumnCount < 4) throw new ArgumentException($"Cannot map row, mapping has 4 columns while row has {Parent.ColumnCount:N0} columns");

            var badMap =
                !Parent.Metadata.Columns[0].CanMapTo(typeof(TCol1)) ||
                !Parent.Metadata.Columns[1].CanMapTo(typeof(TCol2)) ||
                !Parent.Metadata.Columns[2].CanMapTo(typeof(TCol3)) ||
                !Parent.Metadata.Columns[3].CanMapTo(typeof(TCol4));

            if (badMap)
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {Parent.Metadata.Columns[0].MappedType.Name}, {typeof(TCol2).Name} = {Parent.Metadata.Columns[1].MappedType.Name}, {typeof(TCol3).Name} = {Parent.Metadata.Columns[2].MappedType.Name}, {typeof(TCol4).Name} = {Parent.Metadata.Columns[3].MappedType.Name}");
            }

            return new TypedRow<TCol1, TCol2, TCol3, TCol4>(this);
        }

        /// <summary>
        /// Maps this row to a <see cref="TypedRow{TCol1, TCol2, TCol3, TCol4, TCol5}"/>.
        /// </summary>
        public TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5> Map<TCol1, TCol2, TCol3, TCol4, TCol5>()
        {
            if (Parent.ColumnCount < 5) throw new ArgumentException($"Cannot map row, mapping has 5 columns while row has {Parent.ColumnCount:N0} columns");

            var badMap =
                !Parent.Metadata.Columns[0].CanMapTo(typeof(TCol1)) ||
                !Parent.Metadata.Columns[1].CanMapTo(typeof(TCol2)) ||
                !Parent.Metadata.Columns[2].CanMapTo(typeof(TCol3)) ||
                !Parent.Metadata.Columns[3].CanMapTo(typeof(TCol4)) ||
                !Parent.Metadata.Columns[4].CanMapTo(typeof(TCol5));

            if (badMap)
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {Parent.Metadata.Columns[0].MappedType.Name}, {typeof(TCol2).Name} = {Parent.Metadata.Columns[1].MappedType.Name}, {typeof(TCol3).Name} = {Parent.Metadata.Columns[2].MappedType.Name}, {typeof(TCol4).Name} = {Parent.Metadata.Columns[3].MappedType.Name}, {typeof(TCol5).Name} = {Parent.Metadata.Columns[4].MappedType.Name}");
            }

            return new TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5>(this);
        }

        /// <summary>
        /// Maps this row to a <see cref="TypedRow{TCol1, TCol2, TCol3, TCol4, TCol5, TCol6}"/>.
        /// </summary>
        public TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6> Map<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6>()
        {
            if (Parent.ColumnCount < 6) throw new ArgumentException($"Cannot map row, mapping has 6 columns while row has {Parent.ColumnCount:N0} columns");

            var badMap =
                !Parent.Metadata.Columns[0].CanMapTo(typeof(TCol1)) ||
                !Parent.Metadata.Columns[1].CanMapTo(typeof(TCol2)) ||
                !Parent.Metadata.Columns[2].CanMapTo(typeof(TCol3)) ||
                !Parent.Metadata.Columns[3].CanMapTo(typeof(TCol4)) ||
                !Parent.Metadata.Columns[4].CanMapTo(typeof(TCol5)) ||
                !Parent.Metadata.Columns[5].CanMapTo(typeof(TCol6));

            if (badMap)
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {Parent.Metadata.Columns[0].MappedType.Name}, {typeof(TCol2).Name} = {Parent.Metadata.Columns[1].MappedType.Name}, {typeof(TCol3).Name} = {Parent.Metadata.Columns[2].MappedType.Name}, {typeof(TCol4).Name} = {Parent.Metadata.Columns[3].MappedType.Name}, {typeof(TCol5).Name} = {Parent.Metadata.Columns[4].MappedType.Name}, {typeof(TCol6).Name} = {Parent.Metadata.Columns[5].MappedType.Name}");
            }

            return new TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6>(this);
        }

        /// <summary>
        /// Maps this row to a <see cref="TypedRow{TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7}"/>.
        /// </summary>
        public TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7> Map<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7>()
        {
            if (Parent.ColumnCount < 7) throw new ArgumentException($"Cannot map row, mapping has 7 columns while row has {Parent.ColumnCount:N0} columns");

            var badMap =
                !Parent.Metadata.Columns[0].CanMapTo(typeof(TCol1)) ||
                !Parent.Metadata.Columns[1].CanMapTo(typeof(TCol2)) ||
                !Parent.Metadata.Columns[2].CanMapTo(typeof(TCol3)) ||
                !Parent.Metadata.Columns[3].CanMapTo(typeof(TCol4)) ||
                !Parent.Metadata.Columns[4].CanMapTo(typeof(TCol5)) ||
                !Parent.Metadata.Columns[5].CanMapTo(typeof(TCol6)) ||
                !Parent.Metadata.Columns[6].CanMapTo(typeof(TCol7));

            if (badMap)
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {Parent.Metadata.Columns[0].MappedType.Name}, {typeof(TCol2).Name} = {Parent.Metadata.Columns[1].MappedType.Name}, {typeof(TCol3).Name} = {Parent.Metadata.Columns[2].MappedType.Name}, {typeof(TCol4).Name} = {Parent.Metadata.Columns[3].MappedType.Name}, {typeof(TCol5).Name} = {Parent.Metadata.Columns[4].MappedType.Name}, {typeof(TCol6).Name} = {Parent.Metadata.Columns[5].MappedType.Name}, {typeof(TCol7).Name} = {Parent.Metadata.Columns[6].MappedType.Name}");
            }

            return new TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7>(this);
        }

        /// <summary>
        /// Maps this row to a <see cref="TypedRow{TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8}"/>.
        /// </summary>
        public TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8> Map<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8>()
        {
            if (Parent.ColumnCount < 8) throw new ArgumentException($"Cannot map row, mapping has 8 columns while row has {Parent.ColumnCount:N0} columns");

            var badMap =
                !Parent.Metadata.Columns[0].CanMapTo(typeof(TCol1)) ||
                !Parent.Metadata.Columns[1].CanMapTo(typeof(TCol2)) ||
                !Parent.Metadata.Columns[2].CanMapTo(typeof(TCol3)) ||
                !Parent.Metadata.Columns[3].CanMapTo(typeof(TCol4)) ||
                !Parent.Metadata.Columns[4].CanMapTo(typeof(TCol5)) ||
                !Parent.Metadata.Columns[5].CanMapTo(typeof(TCol6)) ||
                !Parent.Metadata.Columns[6].CanMapTo(typeof(TCol7)) ||
                !Parent.Metadata.Columns[7].CanMapTo(typeof(TCol8));

            if (badMap)
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {Parent.Metadata.Columns[0].MappedType.Name}, {typeof(TCol2).Name} = {Parent.Metadata.Columns[1].MappedType.Name}, {typeof(TCol3).Name} = {Parent.Metadata.Columns[2].MappedType.Name}, {typeof(TCol4).Name} = {Parent.Metadata.Columns[3].MappedType.Name}, {typeof(TCol5).Name} = {Parent.Metadata.Columns[4].MappedType.Name}, {typeof(TCol6).Name} = {Parent.Metadata.Columns[5].MappedType.Name}, {typeof(TCol7).Name} = {Parent.Metadata.Columns[6].MappedType.Name}, {typeof(TCol8).Name} = {Parent.Metadata.Columns[7].MappedType.Name}");
            }

            return new TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8>(this);
        }

        /// <summary>
        /// Equivalent to <see cref="ToArray()"/>
        /// </summary>
        public static explicit operator Value[](Row row) => row.ToArray();

        internal T UnsafeGetTranslated<T>(long translatedColumnIndex)
        {
            Value value;
            if(!Parent.TryGetValueTranslated(TranslatedRowIndex, translatedColumnIndex, out value)) throw new InvalidOperationException($"Unexpectedly couldn't find value at {translatedColumnIndex}");

            var category = Parent.Metadata.Columns[translatedColumnIndex].GetCategoryEnumMap<T>();

            return value.UnsafeCast<T>(category);
        }
    }
}
