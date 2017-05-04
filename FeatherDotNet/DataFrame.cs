using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Threading;
using FeatherDotNet.Impl;

namespace FeatherDotNet
{
    /// <summary>
    /// Represents a dataframe.
    /// 
    /// Is untyped, but can be mapped to columns of known types or proxied for easy interop with .NET classes or structs.
    /// 
    /// Is backed by a MemoryMappedFile, remember to Dispose when done using the dataframe.
    /// 
    /// Any <see cref="ProxyDataFrame{TProxyType}"/>, <see cref="TypedDataFrameBase{TRowType}"/>, or <see cref="Value"/> instances
    /// (and their enumerables) obtained via a DataFrame become invalid after that DataFrame is disposed.  Be sure to have converted
    /// to built-in types prior to Disposing.
    /// </summary>
    public partial class DataFrame: IDisposable, IDataFrame
    {
        readonly object InternalSyncLock = new object();

        MemoryMappedFile File;
        MemoryMappedViewAccessor View;
        readonly internal Metadata Metadata;
        volatile Dictionary<string, long> ColumnNameLookup;

        /// <summary>
        /// Number of rows in the DataFrame.
        /// </summary>
        public long RowCount => Metadata.NumRows;
        /// <summary>
        /// Number of columns in the DataFrame.
        /// </summary>
        public long ColumnCount => Metadata.Columns.Length;

        /// <summary>
        /// Whether this DataFrame is addressable with base-0 or base-1 indexes.
        /// </summary>
        public BasisType Basis { get; private set; }

        /// <summary>
        /// An enumerable of all the columns in this DataFrame.
        /// </summary>
        public ColumnEnumerable AllColumns { get; private set; }
        /// <summary>
        /// An enumerable of all the rows in this DataFrame.
        /// </summary>
        public RowEnumerable AllRows { get; private set; }

        /// <summary>
        /// A utility accessor for columns in this DataFrame.
        /// </summary>
        public ColumnMap Columns { get; private set; }
        /// <summary>
        /// A utility accessor for rows in this DataFrame.
        /// </summary>
        public RowMap Rows { get; private set; }

        /// <summary>
        /// Return the row at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetRow(long, out Row)"/> for non-throwing gets.
        /// </summary>
        public Row this[long rowIndex]
        {
            get
            {
                return Rows[rowIndex];
            }
        }

        /// <summary>
        /// Return the column with the given name.
        /// 
        /// Will throw if the name is not found.  Use <see cref="TryGetColumn(string, out Column)"/> for non-throwing gets.
        /// </summary>
        public Column this[string columnName]
        {
            get
            {
                return Columns[columnName];
            }
        }

        /// <summary>
        /// Return the value at the given row and column indexes.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long rowIndex, long columnIndex]
        {
            get
            {
                Value value;
                if (!TryGetValue(rowIndex, columnIndex, out value))
                {
                    long minRowIx, minColIx, maxRowIx, maxColIx;
                    switch (Basis)
                    {
                        case BasisType.One:
                            minRowIx = 1;
                            maxRowIx = Metadata.NumRows;
                            minColIx = 1;
                            maxColIx = Metadata.Columns.Length;
                            break;
                        case BasisType.Zero:
                            minRowIx = 0;
                            maxRowIx = Metadata.NumRows - 1;
                            minColIx = 0;
                            maxColIx = Metadata.Columns.Length - 1;
                            break;
                        default: throw new InvalidOperationException($"Unexpected Basis: {Basis}");
                    }

                    throw new ArgumentOutOfRangeException($"Address out of range, legal range is [{minRowIx}, {minColIx}] - [{maxRowIx}, {maxColIx}], found [{rowIndex}, {columnIndex}]");
                }

                return value;
            }
        }

        /// <summary>
        /// Return the value at the given row index in the column with the given name.
        /// 
        /// Will throw if the index is out of bounds or the column is not found.  Use <see cref="TryGetValue(long, string, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long rowIndex, string columnName]
        {
            get
            {
                Value value;
                if (!TryGetValue(rowIndex, columnName, out value))
                {
                    long translatedColumIx;
                    if (!TryLookupTranslatedColumnIndex(columnName, out translatedColumIx))
                    {
                        throw new ArgumentOutOfRangeException($"Could not find column for name \"{columnName}\"");
                    }

                    var columnIndex = UntranslateIndex(translatedColumIx);

                    long minRowIx, minColIx, maxRowIx, maxColIx;
                    switch (Basis)
                    {
                        case BasisType.One:
                            minRowIx = 1;
                            maxRowIx = Metadata.NumRows;
                            minColIx = 1;
                            maxColIx = Metadata.Columns.Length;
                            break;
                        case BasisType.Zero:
                            minRowIx = 0;
                            maxRowIx = Metadata.NumRows - 1;
                            minColIx = 0;
                            maxColIx = Metadata.Columns.Length - 1;
                            break;
                        default: throw new InvalidOperationException($"Unexpected Basis: {Basis}");
                    }

                    throw new ArgumentOutOfRangeException($"Address out of range, legal range is [{minRowIx}, {minColIx}] - [{maxRowIx}, {maxColIx}], found [{rowIndex}, {columnIndex}]");
                }

                return value;
            }
        }

        internal DataFrame(MemoryMappedFile file, Metadata metadata, BasisType basis)
        {
            Basis = basis;
            File = file;
            Metadata = metadata;
            View = File.CreateViewAccessor();

            AllColumns = new ColumnEnumerable(this);
            AllRows = new RowEnumerable(this);

            Columns = new ColumnMap(this);
            Rows = new RowMap(this);
        }

        /// <summary>
        /// Sets column to the column at the given index.
        /// 
        /// Returns true if a column exists at that index, and false otherwise.
        /// </summary>
        public bool TryGetColumn(long index, out Column column)
        {
            var translatedIndex = TranslateIndex(index);

            return TryGetColumnTranslated(translatedIndex, out column);
        }

        /// <summary>
        /// Sets column to the column with the given name.
        /// 
        /// Returns true if a column exists with that name, and false otherwise.
        /// </summary>
        public bool TryGetColumn(string columnName, out Column column)
        {
            long translatedIndex;
            if (!TryLookupTranslatedColumnIndex(columnName, out translatedIndex))
            {
                column = default(Column);
                return false;
            }

            column =
                new Column
                {
                    Parent = this,
                    TranslatedColumnIndex = translatedIndex
                };
            return true;
        }

        /// <summary>
        /// Gets row to the row at the given index.
        /// 
        /// Returns true if a row exists at that index, and false otherwise.
        /// </summary>
        public bool TryGetRow(long index, out Row row)
        {
            long translatedIndex = TranslateIndex(index);

            return TryGetRowTranslated(translatedIndex, out row);
        }

        /// <summary>
        /// Sets value to the value at the row and column indexes passed in.
        /// 
        /// If the passed indexes are out of bounds false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue(long rowIndex, long columnIndex, out Value value)
        {
            var translatedRowIndex = TranslateIndex(rowIndex);
            var translatedColumnIndex = TranslateIndex(columnIndex);

            return TryGetValueTranslated(translatedRowIndex, translatedColumnIndex, out value);
        }

        /// <summary>
        /// Sets value to the value, coerced to the appropriate type, at the row and column indexes passed in.
        /// 
        /// If the passed indexes are out of bounds, or the value cannot be coerced, false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue<T>(long rowIndex, long columnIndex, out T value)
        {
            Column column;
            if(!TryGetColumn(columnIndex, out column))
            {
                value = default(T);
                return false;
            }

            return column.TryGetValue<T>(rowIndex, out value);
        }

        /// <summary>
        /// Sets value to the value at the row given row index in the column with the given name.
        /// 
        /// If the passed index is out of bounds or no column with the given name exists, false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue(long rowIndex, string columnName, out Value value)
        {
            var translatedRowIndex = TranslateIndex(rowIndex);
            long translatedColumnIndex;
            if (!TryLookupTranslatedColumnIndex(columnName, out translatedColumnIndex))
            {
                value = default(Value);
                return false;
            }

            return TryGetValueTranslated(translatedRowIndex, translatedColumnIndex, out value);
        }

        /// <summary>
        /// Sets value to the value, coerced to the appropriate type, at the given row index in the column with the given name.
        /// 
        /// If the passed index is out of bounds, no column with the given name exists, or the value cannot be coerced then false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue<T>(long rowIndex, string columnName, out T value)
        {
            Column column;
            if (!TryGetColumn(columnName, out column))
            {
                value = default(T);
                return false;
            }

            return column.TryGetValue<T>(rowIndex, out value);
        }

        /// <summary>
        /// <see cref="IDisposable.Dispose"/>
        /// </summary>
        public void Dispose()
        {
            // burn down View
            {
                var copy = View;
                if (copy != null)
                {
                    copy.Dispose();
                    View = null;
                }
            }

            // burn down File
            {
                var copy = File;
                if (copy != null)
                {
                    copy.Dispose();
                    File = null;
                }
            }
        }

        internal ColumnType TypeForTranslated(long translatedColumnIndex)
        {
            var column = Metadata.Columns[translatedColumnIndex];
            return column.Type;
        }

        internal bool TryGetRowTranslated(long translatedRowIndex, out Row row)
        {
            if (translatedRowIndex < 0 || translatedRowIndex >= Metadata.NumRows)
            {
                row = default(Row);
                return false;
            }

            row = new Row(this, translatedRowIndex);
            return true;
        }

        internal bool TryGetValueTranslated(long translatedRowIndex, long translatedColumnIndex, out Value value)
        {
            if (translatedRowIndex < 0 || translatedColumnIndex < 0)
            {
                value = default(Value);
                return false;
            }

            if (translatedRowIndex >= Metadata.NumRows || translatedColumnIndex >= Metadata.Columns.Length)
            {
                value = default(Value);
                return false;
            }

            value = new Value(translatedRowIndex, translatedColumnIndex, this);
            return true;
        }

        internal bool TryGetColumnTranslated(long translatedIndex, out Column column)
        {
            if (translatedIndex < 0 || translatedIndex >= Metadata.Columns.Length)
            {
                column = default(Column);
                return false;
            }

            column =
                new Column
                {
                    TranslatedColumnIndex = translatedIndex,
                    Parent = this
                };
            return true;
        }

        internal long TranslateIndex(long passedIndex)
        {
            switch (Basis)
            {
                case BasisType.One: return passedIndex - 1;
                case BasisType.Zero: return passedIndex;
                default: throw new InvalidOperationException($"Unexpected Basis: {Basis}");
            }
        }

        internal long UntranslateIndex(long translatedIndex)
        {
            switch (Basis)
            {
                case BasisType.One: return translatedIndex + 1;
                case BasisType.Zero: return translatedIndex;
                default: throw new InvalidOperationException($"Unexpected Basis: {Basis}");
            }
        }

        internal bool TryLookupTranslatedColumnIndex(string columnName, out long translatedIndex)
        {
            // only bother to spin this up if we need it
            if (ColumnNameLookup == null)
            {
                lock (InternalSyncLock)
                {
                    if (ColumnNameLookup == null)
                    {
                        var lookup = new Dictionary<string, long>(Metadata.Columns.Length);
                        for (var i = 0; i < Metadata.Columns.Length; i++)
                        {
                            lookup.Add(Metadata.Columns[i].Name, i);
                        }

                        ColumnNameLookup = lookup;
                    }
                }
            }

            return ColumnNameLookup.TryGetValue(columnName, out translatedIndex);
        }

        // Map from the memory mapped file
        internal void UnsafeFastGetRowRange<T>(long translatedRowIndex, long translatedColumnIndex, T[] array, int destinationIndex, int length)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var entrySize = columnMetadata.Type.GetAlignment();
            var dataStart = columnMetadata.DataOffset;
            var byteOffset = translatedRowIndex * entrySize;

            var byteIndex = dataStart + byteOffset;
            UnsafeArrayReader<T>.ReadArray(View, byteIndex, array, 0, length);
        }

        internal bool IsNullTranslated(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var nullBitmaskOffset = columnMetadata.NullBitmaskOffset;

            var nullBitmaskByteOffset = nullBitmaskOffset + (translatedRowIndex / 8);
            var nullBitmaskBitmask = (byte)(1 << (byte)(translatedRowIndex % 8));

            var nullBitmaskByte = View.ReadByte(nullBitmaskByteOffset);

            var isNull = (nullBitmaskByte & nullBitmaskBitmask) == 0;
            return isNull;
        }

        internal double ReadDouble(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var dataOffset = columnMetadata.DataOffset;

            var valueOffset = dataOffset + translatedRowIndex * sizeof(double);

            return View.ReadDouble(valueOffset);
        }

        internal float ReadFloat(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var dataOffset = columnMetadata.DataOffset;

            var valueOffset = dataOffset + translatedRowIndex * sizeof(float);

            return View.ReadSingle(valueOffset);
        }

        internal sbyte ReadInt8(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var dataOffset = columnMetadata.DataOffset;

            var valueOffset = dataOffset + translatedRowIndex * sizeof(sbyte);

            return View.ReadSByte(valueOffset);
        }

        internal byte ReadUInt8(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var dataOffset = columnMetadata.DataOffset;

            var valueOffset = dataOffset + translatedRowIndex * sizeof(byte);

            return View.ReadByte(valueOffset);
        }

        internal short ReadInt16(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var dataOffset = columnMetadata.DataOffset;

            var valueOffset = dataOffset + translatedRowIndex * sizeof(short);

            return View.ReadInt16(valueOffset);
        }

        internal ushort ReadUInt16(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var dataOffset = columnMetadata.DataOffset;

            var valueOffset = dataOffset + translatedRowIndex * sizeof(ushort);

            return View.ReadUInt16(valueOffset);
        }

        internal int ReadInt32(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var dataOffset = columnMetadata.DataOffset;

            var valueOffset = dataOffset + translatedRowIndex * sizeof(int);

            return View.ReadInt32(valueOffset);
        }

        internal uint ReadUInt32(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var dataOffset = columnMetadata.DataOffset;

            var valueOffset = dataOffset + translatedRowIndex * sizeof(uint);

            return View.ReadUInt32(valueOffset);
        }

        internal long ReadInt64(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var dataOffset = columnMetadata.DataOffset;

            var valueOffset = dataOffset + translatedRowIndex * sizeof(long);

            return View.ReadInt64(valueOffset);
        }

        internal ulong ReadUInt64(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var dataOffset = columnMetadata.DataOffset;

            var valueOffset = dataOffset + translatedRowIndex * sizeof(ulong);

            return View.ReadUInt64(valueOffset);
        }

        const int MIN_BYTE_BUFFER_SIZE = sizeof(long);
        ThreadLocal<byte[]> ByteBuffer = new ThreadLocal<byte[]>();
        internal string ReadString(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            return ReadString(View, RowCount, ByteBuffer, ref columnMetadata, translatedRowIndex);
        }

        internal static string ReadString(MemoryMappedViewAccessor view, long rowCount, ThreadLocal<byte[]> byteBuffer, ref ColumnSpec columnMetadata, long translatedRowIndex)
        {
            var dataOffset = columnMetadata.DataOffset;

            var stringOffsetOffset = dataOffset + translatedRowIndex * sizeof(int);
            var stringOffset = view.ReadInt32(stringOffsetOffset);

            var nextStringOffsetOffset = dataOffset + (translatedRowIndex + 1) * sizeof(int);
            var nextStringOffset = view.ReadInt32(nextStringOffsetOffset);

            var stringDataStart = dataOffset + (rowCount + 1) * sizeof(int);

            var stringDataPadding = 0;
            if (stringDataStart % FeatherMagic.ARROW_ALIGNMENT != 0)
            {
                stringDataPadding = FeatherMagic.ARROW_ALIGNMENT - (int)(stringDataStart % FeatherMagic.ARROW_ALIGNMENT);
            }
            stringDataStart += stringDataPadding;

            var stringDataStartIx = stringDataStart + stringOffset;
            var stringDataEndIx = stringDataStart + nextStringOffset;

            var stringLengthLong = stringDataEndIx - stringDataStartIx;
            if (stringLengthLong == 0) return "";

            if (stringLengthLong < 0 || stringLengthLong > Int32.MaxValue)
            {
                throw new InvalidOperationException($"Tried to create a string with an absurd length {stringLengthLong:N0}");
            }

            var stringLength = (int)stringLengthLong;
            
            var buffer = byteBuffer?.Value;
            if (buffer == null || buffer.Length < stringLength)
            {
                int newSize;
                if (stringLength < 4096)
                {
                    newSize = stringLength;

                    // get it to the nearest power of two
                    //   only calculating for the first 32-bits
                    //   since the size check on stringLength
                    //   should exclude anything larger
                    newSize--;
                    newSize |= newSize >> 1;
                    newSize |= newSize >> 2;
                    newSize |= newSize >> 4;
                    newSize |= newSize >> 8;
                    newSize |= newSize >> 16;
                    newSize++;
                }
                else
                {
                    // round to nearest whole page size
                    newSize = (stringLength / 4096) * 4096 + (stringLength % 4096);
                }

                if (buffer == null)
                {
                    newSize = Math.Max(newSize, MIN_BYTE_BUFFER_SIZE);
                    buffer = new byte[newSize];
                }
                else
                {
                    Array.Resize(ref buffer, newSize);
                }

                if (byteBuffer != null)
                {
                    byteBuffer.Value = buffer;
                }
            }

            view.ReadArray(stringDataStartIx, buffer, 0, stringLength);

            var ret = Encoding.UTF8.GetString(buffer, 0, stringLength);
            return ret;
        }

        internal bool ReadBool(long translatedRowIndex, long translatedColumnIndex)
        {
            var columnMetadata = Metadata.Columns[translatedColumnIndex];
            var dataOffset = columnMetadata.DataOffset;

            var byteOffset = translatedRowIndex / 8;
            var bitOffset = (byte)(translatedRowIndex % 8);
            var bitMask = (byte)(1 << bitOffset);

            var boolByte = View.ReadByte(dataOffset + byteOffset);

            return (boolByte & bitMask) != 0;
        }
    }
}