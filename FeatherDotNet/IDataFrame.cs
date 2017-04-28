namespace FeatherDotNet
{
    interface IDataFrame
    {
        long RowCount { get; }
        long ColumnCount { get; }

        BasisType Basis { get; }

        Value this[long rowIndex, long columnIndex] { get; }
        Value this[long rowIndex, string columnName] { get; }

        bool TryGetValue(long rowIndex, long columnIndex, out Value value);
        bool TryGetValue<T>(long rowIndex, long columnIndex, out T value);

        bool TryGetValue(long rowIndex, string columnName, out Value value);
        bool TryGetValue<T>(long rowIndex, string columnName, out T value);
    }
}
