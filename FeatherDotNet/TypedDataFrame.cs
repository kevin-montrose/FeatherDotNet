using FeatherDotNet.Impl;

namespace FeatherDotNet
{
    /// <summary>
    /// Utility class for addressing a <see cref="TypedDataFrameBase{TRowType}"/> rows.
    /// </summary>
    public struct TypedRowMap<TRow>
        where TRow : struct
    {
        TypedDataFrameBase<TRow> Parent;

        /// <summary>
        /// Number of rows in the dataframe
        /// </summary>
        public long Count => Parent.RowCount;

        /// <summary>
        /// Returns the row at the given index (in the dataframe's basis).
        /// 
        /// Throws if the index is out of range.
        /// </summary>
        public TRow this[long index]
        {
            get
            {
                var dynRow = Parent.Inner.Rows[index];

                return Parent.MapRow(dynRow);
            }
        }

        internal TypedRowMap(TypedDataFrameBase<TRow> parent)
        {
            Parent = parent;
        }
    }

    /// <summary>
    /// Represents a dataframe, where each column has been typed.
    /// 
    /// Is backed by a <see cref="DataFrame"/>, and will become invalid when that dataframe is disposed.
    /// </summary>
    public abstract class TypedDataFrameBase<TRowType>: IDataFrame
        where TRowType : struct
    {
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
        /// An enumerable of all the rows in this DataFrame.
        /// </summary>
        public TypedRowEnumerable<TRowType> AllRows { get; private set; }
        /// <summary>
        /// An enumerable of all the columns in this DataFrame.
        /// </summary>
        public ColumnEnumerable AllColumns => Inner.AllColumns;

        /// <summary>
        /// A utility accessor for columns in this DataFrame.
        /// </summary>
        public ColumnMap Columns => Inner.Columns;
        /// <summary>
        /// A utility accessor for rows in this DataFrame.
        /// </summary>
        public TypedRowMap<TRowType> Rows { get; private set; }

        /// <summary>
        /// Return the value at the given row and column indexes.
        /// 
        /// Will throw if the index is out of bounds.  Use <see cref="TryGetValue(long, long, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long rowIndex, long columnIndex]
        {
            get { return Inner[rowIndex, columnIndex]; }
        }

        /// <summary>
        /// Return the value at the given row index in the column with the given name.
        /// 
        /// Will throw if the index is out of bounds or the column is not found.  Use <see cref="TryGetValue(long, string, out Value)"/> for non-throwing gets.
        /// </summary>
        public Value this[long rowIndex, string columnName]
        {
            get { return Inner[rowIndex, columnName]; }
        }

        /// <summary>
        /// Creates a new TypedDateFrameBase
        /// </summary>
        protected TypedDataFrameBase(DataFrame inner)
        {
            Inner = inner;

            AllRows = new TypedRowEnumerable<TRowType>(this);
            Rows = new TypedRowMap<TRowType>(this);
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

        /// <summary>
        /// Sets row to the row at the given index.
        /// 
        /// Returns true if a row exists at that index, and false otherwise.
        /// </summary>
        public bool TryGetRow(long rowIndex, out TRowType row)
        {
            var translated = Inner.TranslateIndex(rowIndex);
            return TryGetRowTranslated(translated, out row);
        }

        internal bool TryGetRowTranslated(long translatedRowIndex, out TRowType row)
        {
            Row dynRow;
            if (!Inner.TryGetRowTranslated(translatedRowIndex, out dynRow))
            {
                row = default(TRowType);
                return false;
            }

            row = MapRow(dynRow);
            return true;
        }

        /// <summary>
        /// Maps an untyped row to TRowType.
        /// </summary>
        protected internal abstract TRowType MapRow(Row row);
    }

    /// <summary>
    /// Represents a dataframe with one typed column.
    /// </summary>
    public sealed class TypedDataFrame<TCol1> : TypedDataFrameBase<TypedRow<TCol1>>
    {
        /// <summary>
        /// The first column in the dataframe.
        /// </summary>
        public TypedColumn<TCol1> Column1 { get; private set; }

        internal TypedDataFrame(DataFrame inner) : base(inner)
        {
            Column inner1;
            inner.TryGetColumnTranslated(0, out inner1);
            Column1 = new TypedColumn<TCol1>(inner1);
        }

        /// <summary>
        /// <see cref="TypedDataFrameBase{TRowType}.MapRow(Row)"/>
        /// </summary>
        protected internal override TypedRow<TCol1> MapRow(Row row) => new TypedRow<TCol1>(row);
    }

    /// <summary>
    /// Represents a dataframe with two typed columns.
    /// </summary>
    public sealed class TypedDataFrame<TCol1, TCol2> : TypedDataFrameBase<TypedRow<TCol1, TCol2>>
    {
        /// <summary>
        /// The first column in the dataframe.
        /// </summary>
        public TypedColumn<TCol1> Column1 { get; private set; }
        /// <summary>
        /// The second column in the dataframe.
        /// </summary>
        public TypedColumn<TCol2> Column2 { get; private set; }

        internal TypedDataFrame(DataFrame inner) : base(inner)
        {
            Column inner1;
            inner.TryGetColumnTranslated(0, out inner1);
            Column1 = new TypedColumn<TCol1>(inner1);

            Column inner2;
            inner.TryGetColumnTranslated(1, out inner2);
            Column2 = new TypedColumn<TCol2>(inner2);
        }

        /// <summary>
        /// <see cref="TypedDataFrameBase{TRowType}.MapRow(Row)"/>
        /// </summary>
        protected internal override TypedRow<TCol1, TCol2> MapRow(Row row) => new TypedRow<TCol1, TCol2>(row);
    }

    /// <summary>
    /// Represents a dataframe with three typed columns.
    /// </summary>
    public sealed class TypedDataFrame<TCol1, TCol2, TCol3> : TypedDataFrameBase<TypedRow<TCol1, TCol2, TCol3>>
    {
        /// <summary>
        /// The first column in the dataframe.
        /// </summary>
        public TypedColumn<TCol1> Column1 { get; private set; }
        /// <summary>
        /// The second column in the dataframe.
        /// </summary>
        public TypedColumn<TCol2> Column2 { get; private set; }
        /// <summary>
        /// The third column in the dataframe.
        /// </summary>
        public TypedColumn<TCol3> Column3 { get; private set; }

        internal TypedDataFrame(DataFrame inner) : base(inner)
        {
            Column inner1;
            inner.TryGetColumnTranslated(0, out inner1);
            Column1 = new TypedColumn<TCol1>(inner1);

            Column inner2;
            inner.TryGetColumnTranslated(1, out inner2);
            Column2 = new TypedColumn<TCol2>(inner2);

            Column inner3;
            inner.TryGetColumnTranslated(2, out inner3);
            Column3 = new TypedColumn<TCol3>(inner3);
        }

        /// <summary>
        /// <see cref="TypedDataFrameBase{TRowType}.MapRow(Row)"/>
        /// </summary>
        protected internal override TypedRow<TCol1, TCol2, TCol3> MapRow(Row row) => new TypedRow<TCol1, TCol2, TCol3>(row);
    }

    /// <summary>
    /// Represents a dataframe with four typed columns.
    /// </summary>
    public sealed class TypedDataFrame<TCol1, TCol2, TCol3, TCol4> : TypedDataFrameBase<TypedRow<TCol1, TCol2, TCol3, TCol4>>
    {
        /// <summary>
        /// The first column in the dataframe.
        /// </summary>
        public TypedColumn<TCol1> Column1 { get; private set; }
        /// <summary>
        /// The second column in the dataframe.
        /// </summary>
        public TypedColumn<TCol2> Column2 { get; private set; }
        /// <summary>
        /// The third column in the dataframe.
        /// </summary>
        public TypedColumn<TCol3> Column3 { get; private set; }
        /// <summary>
        /// The fourth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol4> Column4 { get; private set; }

        internal TypedDataFrame(DataFrame inner) : base(inner)
        {
            Column inner1;
            inner.TryGetColumnTranslated(0, out inner1);
            Column1 = new TypedColumn<TCol1>(inner1);

            Column inner2;
            inner.TryGetColumnTranslated(1, out inner2);
            Column2 = new TypedColumn<TCol2>(inner2);

            Column inner3;
            inner.TryGetColumnTranslated(2, out inner3);
            Column3 = new TypedColumn<TCol3>(inner3);

            Column inner4;
            inner.TryGetColumnTranslated(3, out inner4);
            Column4 = new TypedColumn<TCol4>(inner4);
        }

        /// <summary>
        /// <see cref="TypedDataFrameBase{TRowType}.MapRow(Row)"/>
        /// </summary>
        protected internal override TypedRow<TCol1, TCol2, TCol3, TCol4> MapRow(Row row) => new TypedRow<TCol1, TCol2, TCol3, TCol4>(row);
    }

    /// <summary>
    /// Represents a dataframe with five typed columns.
    /// </summary>
    public sealed class TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5> : TypedDataFrameBase<TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5>>
    {
        /// <summary>
        /// The first column in the dataframe.
        /// </summary>
        public TypedColumn<TCol1> Column1 { get; private set; }
        /// <summary>
        /// The second column in the dataframe.
        /// </summary>
        public TypedColumn<TCol2> Column2 { get; private set; }
        /// <summary>
        /// The third column in the dataframe.
        /// </summary>
        public TypedColumn<TCol3> Column3 { get; private set; }
        /// <summary>
        /// The fourth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol4> Column4 { get; private set; }
        /// <summary>
        /// The fifth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol5> Column5 { get; private set; }

        internal TypedDataFrame(DataFrame inner) : base(inner)
        {
            Column inner1;
            inner.TryGetColumnTranslated(0, out inner1);
            Column1 = new TypedColumn<TCol1>(inner1);

            Column inner2;
            inner.TryGetColumnTranslated(1, out inner2);
            Column2 = new TypedColumn<TCol2>(inner2);

            Column inner3;
            inner.TryGetColumnTranslated(2, out inner3);
            Column3 = new TypedColumn<TCol3>(inner3);

            Column inner4;
            inner.TryGetColumnTranslated(3, out inner4);
            Column4 = new TypedColumn<TCol4>(inner4);

            Column inner5;
            inner.TryGetColumnTranslated(4, out inner5);
            Column5 = new TypedColumn<TCol5>(inner5);
        }

        /// <summary>
        /// <see cref="TypedDataFrameBase{TRowType}.MapRow(Row)"/>
        /// </summary>
        protected internal override TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5> MapRow(Row row) => new TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5>(row);
    }

    /// <summary>
    /// Represents a dataframe with six typed columns.
    /// </summary>
    public sealed class TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6> : TypedDataFrameBase<TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6>>
    {
        /// <summary>
        /// The first column in the dataframe.
        /// </summary>
        public TypedColumn<TCol1> Column1 { get; private set; }
        /// <summary>
        /// The second column in the dataframe.
        /// </summary>
        public TypedColumn<TCol2> Column2 { get; private set; }
        /// <summary>
        /// The third column in the dataframe.
        /// </summary>
        public TypedColumn<TCol3> Column3 { get; private set; }
        /// <summary>
        /// The fourth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol4> Column4 { get; private set; }
        /// <summary>
        /// The fifth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol5> Column5 { get; private set; }
        /// <summary>
        /// The sixth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol6> Column6 { get; private set; }

        internal TypedDataFrame(DataFrame inner) : base(inner)
        {
            Column inner1;
            inner.TryGetColumnTranslated(0, out inner1);
            Column1 = new TypedColumn<TCol1>(inner1);

            Column inner2;
            inner.TryGetColumnTranslated(1, out inner2);
            Column2 = new TypedColumn<TCol2>(inner2);

            Column inner3;
            inner.TryGetColumnTranslated(2, out inner3);
            Column3 = new TypedColumn<TCol3>(inner3);

            Column inner4;
            inner.TryGetColumnTranslated(3, out inner4);
            Column4 = new TypedColumn<TCol4>(inner4);

            Column inner5;
            inner.TryGetColumnTranslated(4, out inner5);
            Column5 = new TypedColumn<TCol5>(inner5);

            Column inner6;
            inner.TryGetColumnTranslated(5, out inner6);
            Column6 = new TypedColumn<TCol6>(inner6);
        }

        /// <summary>
        /// <see cref="TypedDataFrameBase{TRowType}.MapRow(Row)"/>
        /// </summary>
        protected internal override TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6> MapRow(Row row) => new TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6>(row);
    }

    /// <summary>
    /// Represents a dataframe with seven typed columns.
    /// </summary>
    public sealed class TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7> : TypedDataFrameBase<TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7>>
    {
        /// <summary>
        /// The first column in the dataframe.
        /// </summary>
        public TypedColumn<TCol1> Column1 { get; private set; }
        /// <summary>
        /// The second column in the dataframe.
        /// </summary>
        public TypedColumn<TCol2> Column2 { get; private set; }
        /// <summary>
        /// The third column in the dataframe.
        /// </summary>
        public TypedColumn<TCol3> Column3 { get; private set; }
        /// <summary>
        /// The fourth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol4> Column4 { get; private set; }
        /// <summary>
        /// The fifth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol5> Column5 { get; private set; }
        /// <summary>
        /// The sixth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol6> Column6 { get; private set; }
        /// <summary>
        /// The seventh column in the dataframe.
        /// </summary>
        public TypedColumn<TCol7> Column7 { get; private set; }

        internal TypedDataFrame(DataFrame inner) : base(inner)
        {
            Column inner1;
            inner.TryGetColumnTranslated(0, out inner1);
            Column1 = new TypedColumn<TCol1>(inner1);

            Column inner2;
            inner.TryGetColumnTranslated(1, out inner2);
            Column2 = new TypedColumn<TCol2>(inner2);

            Column inner3;
            inner.TryGetColumnTranslated(2, out inner3);
            Column3 = new TypedColumn<TCol3>(inner3);

            Column inner4;
            inner.TryGetColumnTranslated(3, out inner4);
            Column4 = new TypedColumn<TCol4>(inner4);

            Column inner5;
            inner.TryGetColumnTranslated(4, out inner5);
            Column5 = new TypedColumn<TCol5>(inner5);

            Column inner6;
            inner.TryGetColumnTranslated(5, out inner6);
            Column6 = new TypedColumn<TCol6>(inner6);

            Column inner7;
            inner.TryGetColumnTranslated(6, out inner7);
            Column7 = new TypedColumn<TCol7>(inner7);
        }

        /// <summary>
        /// <see cref="TypedDataFrameBase{TRowType}.MapRow(Row)"/>
        /// </summary>
        protected internal override TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7> MapRow(Row row) => new TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7>(row);
    }

    /// <summary>
    /// Represents a dataframe with eight typed columns.
    /// </summary>
    public sealed class TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8> : TypedDataFrameBase<TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8>>
    {
        /// <summary>
        /// The first column in the dataframe.
        /// </summary>
        public TypedColumn<TCol1> Column1 { get; private set; }
        /// <summary>
        /// The second column in the dataframe.
        /// </summary>
        public TypedColumn<TCol2> Column2 { get; private set; }
        /// <summary>
        /// The third column in the dataframe.
        /// </summary>
        public TypedColumn<TCol3> Column3 { get; private set; }
        /// <summary>
        /// The fourth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol4> Column4 { get; private set; }
        /// <summary>
        /// The fifth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol5> Column5 { get; private set; }
        /// <summary>
        /// The sixth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol6> Column6 { get; private set; }
        /// <summary>
        /// The seventh column in the dataframe.
        /// </summary>
        public TypedColumn<TCol7> Column7 { get; private set; }
        /// <summary>
        /// The eigth column in the dataframe.
        /// </summary>
        public TypedColumn<TCol8> Column8 { get; private set; }

        internal TypedDataFrame(DataFrame inner) : base(inner)
        {
            Column inner1;
            inner.TryGetColumnTranslated(0, out inner1);
            Column1 = new TypedColumn<TCol1>(inner1);

            Column inner2;
            inner.TryGetColumnTranslated(1, out inner2);
            Column2 = new TypedColumn<TCol2>(inner2);

            Column inner3;
            inner.TryGetColumnTranslated(2, out inner3);
            Column3 = new TypedColumn<TCol3>(inner3);

            Column inner4;
            inner.TryGetColumnTranslated(3, out inner4);
            Column4 = new TypedColumn<TCol4>(inner4);

            Column inner5;
            inner.TryGetColumnTranslated(4, out inner5);
            Column5 = new TypedColumn<TCol5>(inner5);

            Column inner6;
            inner.TryGetColumnTranslated(5, out inner6);
            Column6 = new TypedColumn<TCol6>(inner6);

            Column inner7;
            inner.TryGetColumnTranslated(6, out inner7);
            Column7 = new TypedColumn<TCol7>(inner7);

            Column inner8;
            inner.TryGetColumnTranslated(7, out inner8);
            Column8 = new TypedColumn<TCol8>(inner8);
        }

        /// <summary>
        /// <see cref="TypedDataFrameBase{TRowType}.MapRow(Row)"/>
        /// </summary>
        protected internal override TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8> MapRow(Row row) => new TypedRow<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8>(row);
    }
}