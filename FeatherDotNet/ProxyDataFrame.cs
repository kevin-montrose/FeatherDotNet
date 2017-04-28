using System;

namespace FeatherDotNet
{
    /// <summary>
    /// Utility class for addressing a <see cref="ProxyDataFrame{TProxyType}"/>'s rows.
    /// </summary>
    public struct ProxyRowMap<TProxyType>
    {
        ProxyDataFrame<TProxyType> Parent;

        /// <summary>
        /// Number of rows in the dataframe
        /// </summary>
        public long Count => Parent.RowCount;

        /// <summary>
        /// Returns the row at the given index (in the dataframe's basis).
        /// 
        /// Throws if the index is out of range.
        /// </summary>
        public TProxyType this[long index]
        {
            get
            {
                var raw = Parent.Inner.Rows[index];
                return Parent.ProxyRow(raw);
            }
        }

        internal ProxyRowMap(ProxyDataFrame<TProxyType> parent)
        {
            Parent = parent;
        }
    }

    /// <summary>
    /// Represents a dataframe, where each row has been mapped to an instance of a type.
    /// 
    /// Is backed by a <see cref="DataFrame"/>, and will become invalid when that dataframe is disposed.
    /// </summary>
    public sealed class ProxyDataFrame<TProxyType> : IDataFrame
    {
        readonly Func<Row, TProxyType, TProxyType> Mapper;
        readonly Func<TProxyType> Factory;

        /// <summary>
        /// The backing <see cref="DataFrame"/>
        /// </summary>
        public DataFrame Inner { get; private set; }

        /// <summary>
        /// Number of rows in the DataFrame.
        /// </summary>
        public long RowCount => Inner.RowCount;
        /// <summary>
        /// Number of columns in the DataFrame.
        /// </summary>
        public long ColumnCount => Inner.ColumnCount;

        /// <summary>
        /// Whether this DataFrame is addressable with base-0 or base-1 indexes.
        /// </summary>
        public BasisType Basis => Inner.Basis;
        
        /// <summary>
        /// An enumerable of all the columns in this DataFrame.
        /// </summary>
        public ColumnEnumerable AllColumns => Inner.AllColumns;
        /// <summary>
        /// An enumerable of all the rows in this DataFrame.
        /// </summary>
        public ProxyRowEnumerable<TProxyType> AllRows { get; private set; }

        /// <summary>
        /// A utility accessor for columns in this DataFrame.
        /// </summary>
        public ColumnMap Columns => Inner.Columns;
        /// <summary>
        /// A utility accessor for rows in this DataFrame.
        /// </summary>
        public ProxyRowMap<TProxyType> Rows { get; private set; }

        /// <summary>
        /// Return the row at the given index.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetRow(long, out TProxyType)"/> for non-throwing gets.
        /// </summary>
        public TProxyType this[long rowIndex] => Rows[rowIndex];

        /// <summary>
        /// Return the column with the given name.
        /// 
        /// Will throw if the name is not found.  Use <see cref="TryGetColumn(string, out Column)"/> for non-throwing gets.
        /// </summary>
        public Column this[string columnName] => Inner[columnName];

        /// <summary>
        /// Return the value at the given row and column indexes.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long rowIndex, long columnIndex] => Inner[rowIndex, columnIndex];

        /// <summary>
        /// Return the value at the given row index in the column with the given name.
        /// 
        /// Will throw if the index is out of bounds or the column is not found.  Use <see cref="TryGetValue(long, string, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long rowIndex, string columnName] => Inner[rowIndex, columnName];

        internal ProxyDataFrame(DataFrame inner, Func<Row, TProxyType, TProxyType> mapper, Func<TProxyType> factory)
        {
            Inner = inner;
            Mapper = mapper;
            Factory = factory;

            AllRows = new ProxyRowEnumerable<TProxyType>(this);
            Rows = new ProxyRowMap<TProxyType>(this);
        }

        /// <summary>
        /// Sets column to the column at the given index.
        /// 
        /// Returns true if a column exists at that index, and false otherwise.
        /// </summary>
        public bool TryGetColumn(long index, out Column column) => Inner.TryGetColumn(index, out column);

        /// <summary>
        /// Sets column to the column with the given name.
        /// 
        /// Returns true if a column exists with that name, and false otherwise.
        /// </summary>
        public bool TryGetColumn(string columnName, out Column column) => Inner.TryGetColumn(columnName, out column);

        /// <summary>
        /// Sets row to the row at the given index.
        /// 
        /// Returns true if a row exists at that index, and false otherwise.
        /// </summary>
        public bool TryGetRow(long index, out TProxyType row)
        {
            var translatedRowIndex = Inner.TranslateIndex(index);

            return TryGetRowTranslated(translatedRowIndex, out row);
        }

        /// <summary>
        /// Sets value to the value at the row and column indexes passed in.
        /// 
        /// If the passed indexes are out of bounds false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue(long rowIndex, long columnIndex, out Value value) => Inner.TryGetValue(rowIndex, columnIndex, out value);

        /// <summary>
        /// Sets value to the value, coerced to the appropriate type, at the row and column indexes passed in.
        /// 
        /// If the passed indexes are out of bounds, or the value cannot be coerced, false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue<T>(long rowIndex, long columnIndex, out T value) => Inner.TryGetValue(rowIndex, columnIndex, out value);
        
        /// <summary>
        /// Sets value to the value at the row given row index in the column with the given name.
        /// 
        /// If the passed index is out of bounds or no column with the given name exists, false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue(long rowIndex, string columnName, out Value value) => Inner.TryGetValue(rowIndex, columnName, out value);

        /// <summary>
        /// Sets value to the value, coerced to the appropriate type, at the given row index in the column with the given name.
        /// 
        /// If the passed index is out of bounds, no column with the given name exists, or the value cannot be coerced then false is returned.  Otherwise, true is returned;
        /// </summary>
        public bool TryGetValue<T>(long rowIndex, string columnName, out T value) => Inner.TryGetValue(rowIndex, columnName, out value);

        internal bool TryGetRowTranslated(long translatedRowIndex, out TProxyType row)
        {
            Row rawRow;
            if (!Inner.TryGetRowTranslated(translatedRowIndex, out rawRow))
            {
                row = default(TProxyType);
                return false;
            }

            row = ProxyRow(rawRow);
            return true;
        }

        internal TProxyType ProxyRow(Row row)
        {
            var toRet = Factory();
            return Mapper(row, toRet);
        }
    }
}
