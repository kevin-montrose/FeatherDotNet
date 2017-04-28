using System;
using System.Collections;
using System.Collections.Generic;

namespace FeatherDotNet
{
    /// <summary>
    /// Allocation free enumerator for a typed row.
    /// </summary>
    public struct TypedRowValueEnumerator: IEnumerator<Value>
    {
        RowValueEnumerator Inner;
        long Length;
        long Index;

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}"/>
        /// </summary>
        public Value Current => Inner.Current;

        object IEnumerator.Current => Current;

        internal TypedRowValueEnumerator(RowValueEnumerator inner, long length)
        {
            Inner = inner;
            Length = length;
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
            if (Index >= Length) return false;

            if (!Inner.MoveNext()) return false;

            return true;
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerator{T}"/>
        /// </summary>
        public void Reset()
        {
            Inner.Reset();
            Index = -1;
        }
    }

    /// <summary>
    /// Represents a typed row with 1 column.
    /// </summary>
    public struct TypedRow<TCol1>: 
        IEnumerable<Value>,
        IRow
    {
        Row Inner;

        /// <summary>
        /// Returns the Index (in the appropriate basis) of this row in the original dataframe.
        /// </summary>
        public long Index => Inner.Index;
        /// <summary>
        /// Returns the number of entries in the this column.
        /// This will always be 1.
        /// </summary>
        public long Length => 1;

        /// <summary>
        /// Return the value at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long columnIndex] => Inner[columnIndex];

        /// <summary>
        /// Return the value in the column with the given name.
        /// 
        /// Will throw if no column with that name exists .  Use <see cref="TryGetValue(string, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[string columnName] => Inner[columnName];

        /// <summary>
        /// Returns the value of the 1st column in the row.
        /// </summary>
        public TCol1 Column1 => Inner.UnsafeGetTranslated<TCol1>(0);

        internal TypedRow(Row inner)
        {
            Inner = inner;
        }

        /// <summary>
        /// <see cref="Object.Equals(object)"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            if (!(obj is TypedRow<TCol1>)) return false;

            var other = (TypedRow<TCol1>)obj;
            return other.Inner.Equals(Inner);
        }

        /// <summary>
        /// <see cref="Object.GetHashCode"/>
        /// </summary>
        public override int GetHashCode()
        {
            return
                Inner.GetHashCode() * 17 +
                typeof(TCol1).GetHashCode();
        }

        /// <summary>
        /// <see cref="Object.ToString"/>
        /// </summary>
        public override string ToString()
        {
            return $"TypedRow<{typeof(TCol1).Name}> Index = {Index}";
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public TypedRowValueEnumerator GetEnumerator() => new TypedRowValueEnumerator(Inner.GetEnumerator(), Length);

        IEnumerator<Value> IEnumerable<Value>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// <see cref="Row.TryGetValue(long, out Value)"/>
        /// </summary>
        public bool TryGetValue(long columnIndex, out Value value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(long, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(long columnIndex, out T value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue(string, out Value)"/>
        /// </summary>
        public bool TryGetValue(string columnName, out Value value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(string, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(string columnName, out T value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.ToArray()"/>
        /// </summary>
        public Value[] ToArray() => Inner.ToArray();

        /// <summary>
        /// <see cref="Row.GetRange(long, int)"/>
        /// </summary>
        public Value[] GetRange(long columnIndex, int length) => Inner.GetRange(columnIndex, length);

        /// <summary>
        /// <see cref="Row.ToArray(ref Value[])"/>
        /// </summary>
        public void ToArray(ref Value[] array) => ToArray(ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[])"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array) => GetRange(columnSourceIndex, length, ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[], int)"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array, int destinationIndex) => GetRange(columnSourceIndex, length, ref array, destinationIndex);

        /// <summary>
        /// Equivalent to <see cref="Row.ToArray()"/>
        /// </summary>
        public static explicit operator Value[] (TypedRow<TCol1> row) => row.ToArray();
    }

    /// <summary>
    /// Represents a typed row with 2 columns.
    /// </summary>
    public struct TypedRow<TCol1, TCol2>:
        IEnumerable<Value>,
        IRow
    {
        Row Inner;

        /// <summary>
        /// Returns the Index (in the appropriate basis) of this row in the original dataframe.
        /// </summary>
        public long Index => Inner.Index;
        /// <summary>
        /// Returns the number of entries in the this column.
        /// This will always be 2.
        /// </summary>
        public long Length => 2;
        /// <summary>
        /// Return the value at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long columnIndex] => Inner[columnIndex];

        /// <summary>
        /// Return the value in the column with the given name.
        /// 
        /// Will throw if no column with that name exists .  Use <see cref="TryGetValue(string, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[string columnName] => Inner[columnName];

        /// <summary>
        /// Returns the value of the 1st column in the row.
        /// </summary>
        public TCol1 Column1 => Inner.UnsafeGetTranslated<TCol1>(0);
        /// <summary>
        /// Returns the value of the 2nd column in the row.
        /// </summary>
        public TCol2 Column2 => Inner.UnsafeGetTranslated<TCol2>(1);

        internal TypedRow(Row inner)
        {
            Inner = inner;
        }

        /// <summary>
        /// <see cref="Object.Equals(object)"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            if (!(obj is TypedRow<TCol1, TCol2>)) return false;

            var other = (TypedRow<TCol1, TCol2>)obj;
            return other.Inner.Equals(Inner);
        }

        /// <summary>
        /// <see cref="Object.GetHashCode"/>
        /// </summary>
        public override int GetHashCode()
        {
            return
                (Inner.GetHashCode() * 17 +
                typeof(TCol1).GetHashCode()) * 17 +
                typeof(TCol2).GetHashCode();
        }

        /// <summary>
        /// <see cref="Object.ToString"/>
        /// </summary>
        public override string ToString()
        {
            return $"TypedRow<{typeof(TCol1).Name}, {typeof(TCol2).Name}> Index = {Index}";
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public TypedRowValueEnumerator GetEnumerator() => new TypedRowValueEnumerator(Inner.GetEnumerator(), Length);

        IEnumerator<Value> IEnumerable<Value>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// <see cref="Row.TryGetValue(long, out Value)"/>
        /// </summary>
        public bool TryGetValue(long columnIndex, out Value value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(long, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(long columnIndex, out T value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue(string, out Value)"/>
        /// </summary>
        public bool TryGetValue(string columnName, out Value value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(string, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(string columnName, out T value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.ToArray()"/>
        /// </summary>
        public Value[] ToArray() => Inner.ToArray();

        /// <summary>
        /// <see cref="Row.GetRange(long, int)"/>
        /// </summary>
        public Value[] GetRange(long columnIndex, int length) => Inner.GetRange(columnIndex, length);

        /// <summary>
        /// <see cref="Row.ToArray(ref Value[])"/>
        /// </summary>
        public void ToArray(ref Value[] array) => ToArray(ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[])"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array) => GetRange(columnSourceIndex, length, ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[], int)"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array, int destinationIndex) => GetRange(columnSourceIndex, length, ref array, destinationIndex);

        /// <summary>
        /// Equivalent to <see cref="Row.ToArray()"/>
        /// </summary>
        public static explicit operator Value[] (TypedRow<TCol1, TCol2> row) => row.ToArray();
    }

    /// <summary>
    /// Represents a typed row with 3 columns.
    /// </summary>
    public struct TypedRow<TCol1, TCol2, TCol3>: 
        IEnumerable<Value>,
        IRow
    {
        Row Inner;

        /// <summary>
        /// Returns the Index (in the appropriate basis) of this row in the original dataframe.
        /// </summary>
        public long Index => Inner.Index;
        /// <summary>
        /// Returns the number of entries in the this column.
        /// This will always be 3.
        /// </summary>
        public long Length => 3;
        /// <summary>
        /// Return the value at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long columnIndex] => Inner[columnIndex];

        /// <summary>
        /// Return the value in the column with the given name.
        /// 
        /// Will throw if no column with that name exists .  Use <see cref="TryGetValue(string, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[string columnName] => Inner[columnName];

        /// <summary>
        /// Returns the value of the 1st column in the row.
        /// </summary>
        public TCol1 Column1 => Inner.UnsafeGetTranslated<TCol1>(0);
        /// <summary>
        /// Returns the value of the 2nd column in the row.
        /// </summary>
        public TCol2 Column2 => Inner.UnsafeGetTranslated<TCol2>(1);
        /// <summary>
        /// Returns the value of the 3rd column in the row.
        /// </summary>
        public TCol3 Column3 => Inner.UnsafeGetTranslated<TCol3>(2);

        internal TypedRow(Row inner)
        {
            Inner = inner;
        }

        /// <summary>
        /// <see cref="Object.Equals(object)"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            if (!(obj is TypedRow<TCol1, TCol2, TCol3>)) return false;

            var other = (TypedRow<TCol1, TCol2, TCol3>)obj;
            return other.Inner.Equals(Inner);
        }

        /// <summary>
        /// <see cref="Object.GetHashCode"/>
        /// </summary>
        public override int GetHashCode()
        {
            return
                ((Inner.GetHashCode() * 17 +
                typeof(TCol1).GetHashCode()) * 17 +
                typeof(TCol2).GetHashCode()) * 17 +
                typeof(TCol3).GetHashCode();
        }

        /// <summary>
        /// <see cref="Object.ToString"/>
        /// </summary>
        public override string ToString()
        {
            return $"TypedRow<{typeof(TCol1).Name}, {typeof(TCol2).Name}, {typeof(TCol3).Name}> Index = {Index}";
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public TypedRowValueEnumerator GetEnumerator() => new TypedRowValueEnumerator(Inner.GetEnumerator(), Length);

        IEnumerator<Value> IEnumerable<Value>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// <see cref="Row.TryGetValue(long, out Value)"/>
        /// </summary>
        public bool TryGetValue(long columnIndex, out Value value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(long, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(long columnIndex, out T value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue(string, out Value)"/>
        /// </summary>
        public bool TryGetValue(string columnName, out Value value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(string, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(string columnName, out T value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.ToArray()"/>
        /// </summary>
        public Value[] ToArray() => Inner.ToArray();

        /// <summary>
        /// <see cref="Row.GetRange(long, int)"/>
        /// </summary>
        public Value[] GetRange(long columnIndex, int length) => Inner.GetRange(columnIndex, length);

        /// <summary>
        /// <see cref="Row.ToArray(ref Value[])"/>
        /// </summary>
        public void ToArray(ref Value[] array) => ToArray(ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[])"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array) => GetRange(columnSourceIndex, length, ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[], int)"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array, int destinationIndex) => GetRange(columnSourceIndex, length, ref array, destinationIndex);

        /// <summary>
        /// Equivalent to <see cref="Row.ToArray()"/>
        /// </summary>
        public static explicit operator Value[] (TypedRow<TCol1, TCol2, TCol3> row) => row.ToArray();
    }

    /// <summary>
    /// Represents a typed row with 4 columns.
    /// </summary>
    public struct TypedRow<TCol1, TCol2, TCol3, TCol4>:
        IEnumerable<Value>,
        IRow
    {
        Row Inner;

        /// <summary>
        /// Returns the Index (in the appropriate basis) of this row in the original dataframe.
        /// </summary>
        public long Index => Inner.Index;
        /// <summary>
        /// Returns the number of entries in the this column.
        /// This will always be 4.
        /// </summary>
        public long Length => 4;
        /// <summary>
        /// Return the value at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long columnIndex] => Inner[columnIndex];

        /// <summary>
        /// Return the value in the column with the given name.
        /// 
        /// Will throw if no column with that name exists .  Use <see cref="TryGetValue(string, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[string columnName] => Inner[columnName];

        /// <summary>
        /// Returns the value of the 1st column in the row.
        /// </summary>
        public TCol1 Column1 => Inner.UnsafeGetTranslated<TCol1>(0);
        /// <summary>
        /// Returns the value of the 2nd column in the row.
        /// </summary>
        public TCol2 Column2 => Inner.UnsafeGetTranslated<TCol2>(1);
        /// <summary>
        /// Returns the value of the 3rd column in the row.
        /// </summary>
        public TCol3 Column3 => Inner.UnsafeGetTranslated<TCol3>(2);
        /// <summary>
        /// Returns the value of the 4th column in the row.
        /// </summary>
        public TCol4 Column4 => Inner.UnsafeGetTranslated<TCol4>(3);

        internal TypedRow(Row inner)
        {
            Inner = inner;
        }

        /// <summary>
        /// <see cref="Object.Equals(object)"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            if (!(obj is TypedRow<TCol1, TCol2, TCol3, TCol4>)) return false;

            var other = (TypedRow<TCol1, TCol2, TCol3, TCol4>)obj;
            return other.Inner.Equals(Inner);
        }

        /// <summary>
        /// <see cref="Object.GetHashCode"/>
        /// </summary>
        public override int GetHashCode()
        {
            return
                (((Inner.GetHashCode() * 17 +
                typeof(TCol1).GetHashCode()) * 17 +
                typeof(TCol2).GetHashCode()) * 17 +
                typeof(TCol3).GetHashCode()) * 17 +
                typeof(TCol4).GetHashCode();
        }

        /// <summary>
        /// <see cref="Object.ToString"/>
        /// </summary>
        public override string ToString()
        {
            return $"TypedRow<{typeof(TCol1).Name}, {typeof(TCol2).Name}, {typeof(TCol3).Name}, {typeof(TCol4).Name}> Index = {Index}";
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public TypedRowValueEnumerator GetEnumerator() => new TypedRowValueEnumerator(Inner.GetEnumerator(), Length);

        IEnumerator<Value> IEnumerable<Value>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// <see cref="Row.TryGetValue(long, out Value)"/>
        /// </summary>
        public bool TryGetValue(long columnIndex, out Value value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(long, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(long columnIndex, out T value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue(string, out Value)"/>
        /// </summary>
        public bool TryGetValue(string columnName, out Value value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(string, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(string columnName, out T value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.ToArray()"/>
        /// </summary>
        public Value[] ToArray() => Inner.ToArray();

        /// <summary>
        /// <see cref="Row.GetRange(long, int)"/>
        /// </summary>
        public Value[] GetRange(long columnIndex, int length) => Inner.GetRange(columnIndex, length);

        /// <summary>
        /// <see cref="Row.ToArray(ref Value[])"/>
        /// </summary>
        public void ToArray(ref Value[] array) => ToArray(ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[])"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array) => GetRange(columnSourceIndex, length, ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[], int)"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array, int destinationIndex) => GetRange(columnSourceIndex, length, ref array, destinationIndex);

        /// <summary>
        /// Equivalent to <see cref="Row.ToArray()"/>
        /// </summary>
        public static explicit operator Value[] (TypedRow<TCol1, TCol2, TCol3, TCol4> row) => row.ToArray();
    }

    /// <summary>
    /// Represents a typed row with 5 columns.
    /// </summary>
    public struct TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5>:
        IEnumerable<Value>,
        IRow
    {
        Row Inner;

        /// <summary>
        /// Returns the Index (in the appropriate basis) of this row in the original dataframe.
        /// </summary>
        public long Index => Inner.Index;
        /// <summary>
        /// Returns the number of entries in the this column.
        /// This will always be 5.
        /// </summary>
        public long Length => 5;
        /// <summary>
        /// Return the value at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long columnIndex] => Inner[columnIndex];

        /// <summary>
        /// Return the value in the column with the given name.
        /// 
        /// Will throw if no column with that name exists .  Use <see cref="TryGetValue(string, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[string columnName] => Inner[columnName];

        /// <summary>
        /// Returns the value of the 1st column in the row.
        /// </summary>
        public TCol1 Column1 => Inner.UnsafeGetTranslated<TCol1>(0);
        /// <summary>
        /// Returns the value of the 2nd column in the row.
        /// </summary>
        public TCol2 Column2 => Inner.UnsafeGetTranslated<TCol2>(1);
        /// <summary>
        /// Returns the value of the 3rd column in the row.
        /// </summary>
        public TCol3 Column3 => Inner.UnsafeGetTranslated<TCol3>(2);
        /// <summary>
        /// Returns the value of the 4th column in the row.
        /// </summary>
        public TCol4 Column4 => Inner.UnsafeGetTranslated<TCol4>(3);
        /// <summary>
        /// Returns the value of the 5th column in the row.
        /// </summary>
        public TCol5 Column5 => Inner.UnsafeGetTranslated<TCol5>(4);

        internal TypedRow(Row inner)
        {
            Inner = inner;
        }

        /// <summary>
        /// <see cref="Object.Equals(object)"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            if (!(obj is TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5>)) return false;

            var other = (TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5>)obj;
            return other.Inner.Equals(Inner);
        }

        /// <summary>
        /// <see cref="Object.GetHashCode"/>
        /// </summary>
        public override int GetHashCode()
        {
            return
                ((((Inner.GetHashCode() * 17 +
                typeof(TCol1).GetHashCode()) * 17 +
                typeof(TCol2).GetHashCode()) * 17 +
                typeof(TCol3).GetHashCode()) * 17 +
                typeof(TCol4).GetHashCode()) * 17 +
                typeof(TCol5).GetHashCode();
        }

        /// <summary>
        /// <see cref="Object.ToString"/>
        /// </summary>
        public override string ToString()
        {
            return $"TypedRow<{typeof(TCol1).Name}, {typeof(TCol2).Name}, {typeof(TCol3).Name}, {typeof(TCol4).Name}, {typeof(TCol5).Name}> Index = {Index}";
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public TypedRowValueEnumerator GetEnumerator() => new TypedRowValueEnumerator(Inner.GetEnumerator(), Length);

        IEnumerator<Value> IEnumerable<Value>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// <see cref="Row.TryGetValue(long, out Value)"/>
        /// </summary>
        public bool TryGetValue(long columnIndex, out Value value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(long, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(long columnIndex, out T value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue(string, out Value)"/>
        /// </summary>
        public bool TryGetValue(string columnName, out Value value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(string, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(string columnName, out T value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.ToArray()"/>
        /// </summary>
        public Value[] ToArray() => Inner.ToArray();

        /// <summary>
        /// <see cref="Row.GetRange(long, int)"/>
        /// </summary>
        public Value[] GetRange(long columnIndex, int length) => Inner.GetRange(columnIndex, length);

        /// <summary>
        /// <see cref="Row.ToArray(ref Value[])"/>
        /// </summary>
        public void ToArray(ref Value[] array) => ToArray(ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[])"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array) => GetRange(columnSourceIndex, length, ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[], int)"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array, int destinationIndex) => GetRange(columnSourceIndex, length, ref array, destinationIndex);

        /// <summary>
        /// Equivalent to <see cref="Row.ToArray()"/>
        /// </summary>
        public static explicit operator Value[] (TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5> row) => row.ToArray();
    }

    /// <summary>
    /// Represents a typed row with 6 columns.
    /// </summary>
    public struct TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6>: 
        IEnumerable<Value>,
        IRow
    {
        Row Inner;

        /// <summary>
        /// Returns the Index (in the appropriate basis) of this row in the original dataframe.
        /// </summary>
        public long Index => Inner.Index;
        /// <summary>
        /// Returns the number of entries in the this column.
        /// This will always be 6.
        /// </summary>
        public long Length => 6;
        /// <summary>
        /// Return the value at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long columnIndex] => Inner[columnIndex];

        /// <summary>
        /// Return the value in the column with the given name.
        /// 
        /// Will throw if no column with that name exists .  Use <see cref="TryGetValue(string, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[string columnName] => Inner[columnName];

        /// <summary>
        /// Returns the value of the 1st column in the row.
        /// </summary>
        public TCol1 Column1 => Inner.UnsafeGetTranslated<TCol1>(0);
        /// <summary>
        /// Returns the value of the 2nd column in the row.
        /// </summary>
        public TCol2 Column2 => Inner.UnsafeGetTranslated<TCol2>(1);
        /// <summary>
        /// Returns the value of the 3rd column in the row.
        /// </summary>
        public TCol3 Column3 => Inner.UnsafeGetTranslated<TCol3>(2);
        /// <summary>
        /// Returns the value of the 4th column in the row.
        /// </summary>
        public TCol4 Column4 => Inner.UnsafeGetTranslated<TCol4>(3);
        /// <summary>
        /// Returns the value of the 5th column in the row.
        /// </summary>
        public TCol5 Column5 => Inner.UnsafeGetTranslated<TCol5>(4);
        /// <summary>
        /// Returns the value of the 6th column in the row.
        /// </summary>
        public TCol6 Column6 => Inner.UnsafeGetTranslated<TCol6>(5);

        internal TypedRow(Row inner)
        {
            Inner = inner;
        }

        /// <summary>
        /// <see cref="Object.Equals(object)"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            if (!(obj is TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6>)) return false;

            var other = (TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6>)obj;
            return other.Inner.Equals(Inner);
        }

        /// <summary>
        /// <see cref="Object.GetHashCode"/>
        /// </summary>
        public override int GetHashCode()
        {
            return
                (((((Inner.GetHashCode() * 17 +
                typeof(TCol1).GetHashCode()) * 17 +
                typeof(TCol2).GetHashCode()) * 17 +
                typeof(TCol3).GetHashCode()) * 17 +
                typeof(TCol4).GetHashCode()) * 17 +
                typeof(TCol5).GetHashCode()) * 17 +
                typeof(TCol6).GetHashCode();
        }

        /// <summary>
        /// <see cref="Object.ToString"/>
        /// </summary>
        public override string ToString()
        {
            return $"TypedRow<{typeof(TCol1).Name}, {typeof(TCol2).Name}, {typeof(TCol3).Name}, {typeof(TCol4).Name}, {typeof(TCol5).Name}, {typeof(TCol6).Name}> Index = {Index}";
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public TypedRowValueEnumerator GetEnumerator() => new TypedRowValueEnumerator(Inner.GetEnumerator(), Length);

        IEnumerator<Value> IEnumerable<Value>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// <see cref="Row.TryGetValue(long, out Value)"/>
        /// </summary>
        public bool TryGetValue(long columnIndex, out Value value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(long, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(long columnIndex, out T value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue(string, out Value)"/>
        /// </summary>
        public bool TryGetValue(string columnName, out Value value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(string, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(string columnName, out T value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.ToArray()"/>
        /// </summary>
        public Value[] ToArray() => Inner.ToArray();

        /// <summary>
        /// <see cref="Row.GetRange(long, int)"/>
        /// </summary>
        public Value[] GetRange(long columnIndex, int length) => Inner.GetRange(columnIndex, length);

        /// <summary>
        /// <see cref="Row.ToArray(ref Value[])"/>
        /// </summary>
        public void ToArray(ref Value[] array) => ToArray(ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[])"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array) => GetRange(columnSourceIndex, length, ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[], int)"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array, int destinationIndex) => GetRange(columnSourceIndex, length, ref array, destinationIndex);

        /// <summary>
        /// Equivalent to <see cref="Row.ToArray()"/>
        /// </summary>
        public static explicit operator Value[] (TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6> row) => row.ToArray();
    }

    /// <summary>
    /// Represents a typed row with 7 columns.
    /// </summary>
    public struct TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7>: 
        IEnumerable<Value>,
        IRow
    {
        Row Inner;

        /// <summary>
        /// Returns the Index (in the appropriate basis) of this row in the original dataframe.
        /// </summary>
        public long Index => Inner.Index;
        /// <summary>
        /// Returns the number of entries in the this column.
        /// This will always be 7.
        /// </summary>
        public long Length => 7;
        /// <summary>
        /// Return the value at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long columnIndex] => Inner[columnIndex];

        /// <summary>
        /// Return the value in the column with the given name.
        /// 
        /// Will throw if no column with that name exists .  Use <see cref="TryGetValue(string, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[string columnName] => Inner[columnName];

        /// <summary>
        /// Returns the value of the 1st column in the row.
        /// </summary>
        public TCol1 Column1 => Inner.UnsafeGetTranslated<TCol1>(0);
        /// <summary>
        /// Returns the value of the 2nd column in the row.
        /// </summary>
        public TCol2 Column2 => Inner.UnsafeGetTranslated<TCol2>(1);
        /// <summary>
        /// Returns the value of the 3rd column in the row.
        /// </summary>
        public TCol3 Column3 => Inner.UnsafeGetTranslated<TCol3>(2);
        /// <summary>
        /// Returns the value of the 4th column in the row.
        /// </summary>
        public TCol4 Column4 => Inner.UnsafeGetTranslated<TCol4>(3);
        /// <summary>
        /// Returns the value of the 5th column in the row.
        /// </summary>
        public TCol5 Column5 => Inner.UnsafeGetTranslated<TCol5>(4);
        /// <summary>
        /// Returns the value of the 6th column in the row.
        /// </summary>
        public TCol6 Column6 => Inner.UnsafeGetTranslated<TCol6>(5);
        /// <summary>
        /// Returns the value of the 7th column in the row.
        /// </summary>
        public TCol7 Column7 => Inner.UnsafeGetTranslated<TCol7>(6);

        internal TypedRow(Row inner)
        {
            Inner = inner;
        }

        /// <summary>
        /// <see cref="Object.Equals(object)"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            if (!(obj is TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7>)) return false;

            var other = (TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7>)obj;
            return other.Inner.Equals(Inner);
        }

        /// <summary>
        /// <see cref="Object.GetHashCode"/>
        /// </summary>
        public override int GetHashCode()
        {
            return
                ((((((Inner.GetHashCode() * 17 +
                typeof(TCol1).GetHashCode()) * 17 +
                typeof(TCol2).GetHashCode()) * 17 +
                typeof(TCol3).GetHashCode()) * 17 +
                typeof(TCol4).GetHashCode()) * 17 +
                typeof(TCol5).GetHashCode()) * 17 +
                typeof(TCol6).GetHashCode()) * 17 +
                typeof(TCol7).GetHashCode();
        }

        /// <summary>
        /// <see cref="Object.ToString"/>
        /// </summary>
        public override string ToString()
        {
            return $"TypedRow<{typeof(TCol1).Name}, {typeof(TCol2).Name}, {typeof(TCol3).Name}, {typeof(TCol4).Name}, {typeof(TCol5).Name}, {typeof(TCol6).Name}, {typeof(TCol7).Name}> Index = {Index}";
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public TypedRowValueEnumerator GetEnumerator() => new TypedRowValueEnumerator(Inner.GetEnumerator(), Length);

        IEnumerator<Value> IEnumerable<Value>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// <see cref="Row.TryGetValue(long, out Value)"/>
        /// </summary>
        public bool TryGetValue(long columnIndex, out Value value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(long, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(long columnIndex, out T value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue(string, out Value)"/>
        /// </summary>
        public bool TryGetValue(string columnName, out Value value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(string, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(string columnName, out T value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.ToArray()"/>
        /// </summary>
        public Value[] ToArray() => Inner.ToArray();

        /// <summary>
        /// <see cref="Row.GetRange(long, int)"/>
        /// </summary>
        public Value[] GetRange(long columnIndex, int length) => Inner.GetRange(columnIndex, length);

        /// <summary>
        /// <see cref="Row.ToArray(ref Value[])"/>
        /// </summary>
        public void ToArray(ref Value[] array) => ToArray(ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[])"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array) => GetRange(columnSourceIndex, length, ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[], int)"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array, int destinationIndex) => GetRange(columnSourceIndex, length, ref array, destinationIndex);

        /// <summary>
        /// Equivalent to <see cref="Row.ToArray()"/>
        /// </summary>
        public static explicit operator Value[] (TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7> row) => row.ToArray();
    }

    /// <summary>
    /// Represents a typed row with 8 columns.
    /// </summary>
    public struct TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8>:
        IEnumerable<Value>,
        IRow
    {
        Row Inner;

        /// <summary>
        /// Returns the Index (in the appropriate basis) of this row in the original dataframe.
        /// </summary>
        public long Index => Inner.Index;
        /// <summary>
        /// Returns the number of entries in the this column.
        /// This will always be 8.
        /// </summary>
        public long Length => 8;
        /// <summary>
        /// Return the value at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long columnIndex] => Inner[columnIndex];

        /// <summary>
        /// Return the value in the column with the given name.
        /// 
        /// Will throw if no column with that name exists .  Use <see cref="TryGetValue(string, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[string columnName] => Inner[columnName];

        /// <summary>
        /// Returns the value of the 1st column in the row.
        /// </summary>
        public TCol1 Column1 => Inner.UnsafeGetTranslated<TCol1>(0);
        /// <summary>
        /// Returns the value of the 2nd column in the row.
        /// </summary>
        public TCol2 Column2 => Inner.UnsafeGetTranslated<TCol2>(1);
        /// <summary>
        /// Returns the value of the 3rd column in the row.
        /// </summary>
        public TCol3 Column3 => Inner.UnsafeGetTranslated<TCol3>(2);
        /// <summary>
        /// Returns the value of the 4th column in the row.
        /// </summary>
        public TCol4 Column4 => Inner.UnsafeGetTranslated<TCol4>(3);
        /// <summary>
        /// Returns the value of the 5th column in the row.
        /// </summary>
        public TCol5 Column5 => Inner.UnsafeGetTranslated<TCol5>(4);
        /// <summary>
        /// Returns the value of the 6th column in the row.
        /// </summary>
        public TCol6 Column6 => Inner.UnsafeGetTranslated<TCol6>(5);
        /// <summary>
        /// Returns the value of the 7th column in the row.
        /// </summary>
        public TCol7 Column7 => Inner.UnsafeGetTranslated<TCol7>(6);
        /// <summary>
        /// Returns the value of the 8th column in the row.
        /// </summary>
        public TCol8 Column8 => Inner.UnsafeGetTranslated<TCol8>(7);

        internal TypedRow(Row inner)
        {
            Inner = inner;
        }

        /// <summary>
        /// <see cref="Object.Equals(object)"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            if (!(obj is TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8>)) return false;

            var other = (TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8>)obj;
            return other.Inner.Equals(Inner);
        }

        /// <summary>
        /// <see cref="Object.GetHashCode"/>
        /// </summary>
        public override int GetHashCode()
        {
            return
                (((((((Inner.GetHashCode() * 17 +
                typeof(TCol1).GetHashCode()) * 17 +
                typeof(TCol2).GetHashCode()) * 17 +
                typeof(TCol3).GetHashCode()) * 17 +
                typeof(TCol4).GetHashCode()) * 17 +
                typeof(TCol5).GetHashCode()) * 17 +
                typeof(TCol6).GetHashCode()) * 17 +
                typeof(TCol7).GetHashCode()) * 17 +
                typeof(TCol8).GetHashCode();
        }

        /// <summary>
        /// <see cref="Object.ToString"/>
        /// </summary>
        public override string ToString()
        {
            return $"TypedRow<{typeof(TCol1).Name}, {typeof(TCol2).Name}, {typeof(TCol3).Name}, {typeof(TCol4).Name}, {typeof(TCol5).Name}, {typeof(TCol6).Name}, {typeof(TCol7).Name}, {typeof(TCol8).Name}> Index = {Index}";
        }

        /// <summary>
        /// <see cref="System.Collections.Generic.IEnumerable{T}.GetEnumerator"/>
        /// </summary>
        public TypedRowValueEnumerator GetEnumerator() => new TypedRowValueEnumerator(Inner.GetEnumerator(), Length);

        IEnumerator<Value> IEnumerable<Value>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// <see cref="Row.TryGetValue(long, out Value)"/>
        /// </summary>
        public bool TryGetValue(long columnIndex, out Value value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(long, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(long columnIndex, out T value) => Inner.TryGetValue(columnIndex, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue(string, out Value)"/>
        /// </summary>
        public bool TryGetValue(string columnName, out Value value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.TryGetValue{T}(string, out T)"/>
        /// </summary>
        public bool TryGetValue<T>(string columnName, out T value) => Inner.TryGetValue(columnName, out value);

        /// <summary>
        /// <see cref="Row.ToArray()"/>
        /// </summary>
        public Value[] ToArray() => Inner.ToArray();

        /// <summary>
        /// <see cref="Row.GetRange(long, int)"/>
        /// </summary>
        public Value[] GetRange(long columnIndex, int length) => Inner.GetRange(columnIndex, length);

        /// <summary>
        /// <see cref="Row.ToArray(ref Value[])"/>
        /// </summary>
        public void ToArray(ref Value[] array) => ToArray(ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[])"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array) => GetRange(columnSourceIndex, length, ref array);

        /// <summary>
        /// <see cref="Row.GetRange(long, int, ref Value[], int)"/>
        /// </summary>
        public void GetRange(long columnSourceIndex, int length, ref Value[] array, int destinationIndex) => GetRange(columnSourceIndex, length, ref array, destinationIndex);

        /// <summary>
        /// Equivalent to <see cref="Row.ToArray()"/>
        /// </summary>
        public static explicit operator Value[] (TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8> row) => row.ToArray();
    }
}
