using System;
using System.Collections.Generic;

namespace FeatherDotNet
{
    interface IColumn<T>:
        IList<T> // implement so Linq's ElementAt is as fast as you'd expect
    {
        long Index { get; }
        string Name { get; }
        Type Type { get; }
        long Length { get; }

        T[] GetRange(long rowIndex, int length);

        T[] ToArray();

        void ToArray(ref T[] array);
        void ToArray<V>(ref V[] array);
        void ToArray(ref Value[] array);

        void GetRange(long rowSourceIndex, int length, ref T[] array);
        void GetRange<V>(long rowSourceIndex, int length, ref V[] array);
        void GetRange(long rowSourceIndex, int length, ref Value[] array);

        void GetRange(long rowSourceIndex, int length, ref T[] array, int destinationIndex);
        void GetRange<V>(long rowSourceIndex, int length, ref V[] array, int destinationIndex);
        void GetRange(long rowSourceIndex, int length, ref Value[] array, int destinationIndex);

        bool TryGetValue(long rowIndex, out T value);
        bool TryGetValue<V>(long rowIndex, out V value);
        bool TryGetValue(long rowIndex, out Value value);
    }
}