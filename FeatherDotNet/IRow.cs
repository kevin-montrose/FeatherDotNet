namespace FeatherDotNet
{
    interface IRow
    {
        long Index { get; }
        long Length { get; }

        bool TryGetValue(long columnIndex, out Value value);
        bool TryGetValue<T>(long columnIndex, out T value);

        bool TryGetValue(string columnName, out Value value);
        bool TryGetValue<T>(string columnName, out T value);

        Value[] ToArray();
        Value[] GetRange(long columnIndex, int length);

        void ToArray(ref Value[] array);
        void GetRange(long columnSourceIndex, int length, ref Value[] array);
        void GetRange(long columnSourceIndex, int length, ref Value[] array, int destinationIndex);
    }
}
