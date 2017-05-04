using System;
using System.Linq;
using FeatherDotNet.Impl;

namespace FeatherDotNet
{
    /// <summary>
    /// Represents a value in a dataframe cell.
    /// 
    /// Depending on the underlying type, can be coerced to:
    ///   - (s)byte(?)
    ///   - (u)short(?)
    ///   - (u)int(?)
    ///   - (u)long(?)
    ///   - float(?)
    ///   - double(?)
    ///   - bool(?)
    ///   - string
    ///   - DateTime(?)
    ///   - DateTimeOffset(?)
    ///   - TimeSpan(?)
    ///   
    /// Conversions can be performed with casts (which throw if the conversion is invalid) or the TryConvert methods which
    /// return false if the conversion is invalid.
    /// 
    /// Conversions are done lazily.  Values just represent offsets into a dataframe.
    /// 
    /// Enumerations are a special case.  If the underyling type is a category, enums can be used if:
    ///   - the names of each enum field match a category name
    ///   - the valyues of each enum field match a category index
    ///   
    /// However, no cast operators can be provided for enumerations.  Conversion must go through either an (int) cast or
    /// a TryConvert call.
    /// </summary>
    public struct Value:
        IEquatable<Value>,
        IEquatable<byte>,
        IEquatable<byte?>,
        IEquatable<sbyte>,
        IEquatable<sbyte?>,
        IEquatable<short>,
        IEquatable<short?>,
        IEquatable<ushort>,
        IEquatable<ushort?>,
        IEquatable<int>,
        IEquatable<int?>,
        IEquatable<uint>,
        IEquatable<uint?>,
        IEquatable<long>,
        IEquatable<long?>,
        IEquatable<ulong>,
        IEquatable<ulong?>,
        IEquatable<float>,
        IEquatable<float?>,
        IEquatable<double>,
        IEquatable<double?>,
        IEquatable<bool>,
        IEquatable<bool?>,
        IEquatable<string>,
        IEquatable<DateTime>,
        IEquatable<DateTime?>,
        IEquatable<DateTimeOffset>,
        IEquatable<DateTimeOffset?>,
        IEquatable<TimeSpan>,
        IEquatable<TimeSpan?>
    {
        DataFrame Parent;

        long TranslatedRowIndex;
        internal long TranslatedColumnIndex;

        /// <summary>
        /// The row index, in the containing dataframe's basis, of this Value.
        /// </summary>
        public long RowIndex => Parent.UntranslateIndex(TranslatedRowIndex);
        /// <summary>
        /// The column index, in the containing dataframe's basis, of this Value.
        /// </summary>
        public long ColumnIndex => Parent.UntranslateIndex(TranslatedColumnIndex);
        /// <summary>
        /// The .NET type that best matches the underlying value of this Value.
        /// 
        /// Categories (which best mapped to enums) have a Type of System.Enum.
        /// </summary>
        public Type Type => OnDiskType.GetMapType();

        /// <summary>
        /// The row that contains this value.
        /// </summary>
        public Row Row
        {
            get
            {
                Row row;
                if (!Parent.TryGetRowTranslated(TranslatedRowIndex, out row))
                {
                    throw new ArgumentOutOfRangeException($"Row index unexpectedly out of bounds {TranslatedRowIndex}");
                }

                return row;
            }
        }

        /// <summary>
        /// The column that contains this value.
        /// </summary>
        public Column Column
        {
            get
            {
                Column col;
                if (!Parent.TryGetColumnTranslated(TranslatedColumnIndex, out col))
                {
                    throw new ArgumentOutOfRangeException($"Column index unexpectedly out of bounds {TranslatedColumnIndex}");
                }

                return col;
            }
        }

        internal ColumnType OnDiskType => Parent.Metadata.Columns[TranslatedColumnIndex].Type;
        internal bool IsCategory => (OnDiskType == ColumnType.Int32 || OnDiskType == ColumnType.NullableInt32) && Categories != null;
        internal string[] Categories => Parent.Metadata.Columns[TranslatedColumnIndex].CategoryLevels;

        internal Value(long translatedRowIndex, long translatedColumnIndex, DataFrame parent)
        {
            Parent = parent;
            TranslatedRowIndex = translatedRowIndex;
            TranslatedColumnIndex = translatedColumnIndex;
        }

#pragma warning disable 1591
        public bool Equals(Value other)
        {
            switch (other.OnDiskType)
            {
                // TODO (Binary)
                case ColumnType.NullableBinary:
                case ColumnType.Binary:
                // END TODO

                case ColumnType.Category:
                    return CategoryEquals(this, other);
                case ColumnType.NullableCategory:
                    return NullableCategoryEquals(this, other);

                case ColumnType.Date:
                case ColumnType.Timestamp_Microsecond:
                case ColumnType.Timestamp_Millisecond:
                case ColumnType.Timestamp_Nanosecond:
                case ColumnType.Timestamp_Second:
                    return this.Equals((DateTime)other);

                case ColumnType.Time_Microsecond:
                case ColumnType.Time_Millisecond:
                case ColumnType.Time_Nanosecond:
                case ColumnType.Time_Second:
                    return this.Equals((TimeSpan)other);

                case ColumnType.NullableDate:
                case ColumnType.NullableTimestamp_Microsecond:
                case ColumnType.NullableTimestamp_Millisecond:
                case ColumnType.NullableTimestamp_Nanosecond:
                case ColumnType.NullableTimestamp_Second:
                    return this.Equals((DateTime?)other);

                case ColumnType.NullableTime_Microsecond:
                case ColumnType.NullableTime_Millisecond:
                case ColumnType.NullableTime_Nanosecond:
                case ColumnType.NullableTime_Second:
                    return this.Equals((TimeSpan?)other);

                case ColumnType.Bool: return this.Equals((bool)other);
                case ColumnType.NullableBool: return this.Equals((bool?)other);
                case ColumnType.Double: return this.Equals((double)other);
                case ColumnType.NullableDouble: return this.Equals((double?)other);
                case ColumnType.Float: return this.Equals((float)other);
                case ColumnType.NullableFloat: return this.Equals((float?)other);
                case ColumnType.Int16: return this.Equals((short)other);
                case ColumnType.NullableInt16: return this.Equals((short?)other);
                case ColumnType.Int32: return this.Equals((int)other);
                case ColumnType.NullableInt32: return this.Equals((int?)other);
                case ColumnType.Int64: return this.Equals((long)other);
                case ColumnType.NullableInt64: return this.Equals((long?)other);
                case ColumnType.Int8: return this.Equals((sbyte)other);
                case ColumnType.NullableInt8: return this.Equals((sbyte?)other);

                case ColumnType.NullableString:
                case ColumnType.String:
                    return this.Equals((string)other);

                case ColumnType.Uint16: return this.Equals((ushort)other);
                case ColumnType.NullableUint16: return this.Equals((ushort?)other);
                case ColumnType.Uint32: return this.Equals((uint)other);
                case ColumnType.NullableUint32: return this.Equals((uint?)other);
                case ColumnType.Uint64: return this.Equals((long)other);
                case ColumnType.NullableUint64: return this.Equals((ulong?)other);
                case ColumnType.Uint8: return this.Equals((byte)other);
                case ColumnType.NullableUint8: return this.Equals((byte?)other);

                default: throw new Exception($"Unexpected ColumnType {other.OnDiskType}");
            }
        }

        public bool Equals(bool b)
        {
            bool other;
            if (!TryConvert(out other)) return false;

            return b == other;
        }

        public bool Equals(bool? b)
        {
            bool? other;
            if (!TryConvert(out other)) return false;

            return b == other;
        }

        public bool Equals(byte b)
        {
            byte other;
            if (!TryConvert(out other)) return false;

            return b == other;
        }

        public bool Equals(byte? b)
        {
            byte? other;
            if (!TryConvert(out other)) return false;

            return b == other;
        }

        public bool Equals(sbyte b)
        {
            sbyte other;
            if (!TryConvert(out other)) return false;

            return b == other;
        }

        public bool Equals(sbyte? b)
        {
            sbyte? other;
            if (!TryConvert(out other)) return false;

            return b == other;
        }

        public bool Equals(short s)
        {
            short other;
            if (!TryConvert(out other)) return false;

            return s == other;
        }

        public bool Equals(short? s)
        {
            short? other;
            if (!TryConvert(out other)) return false;

            return s == other;
        }

        public bool Equals(ushort s)
        {
            ushort other;
            if (!TryConvert(out other)) return false;

            return s == other;
        }

        public bool Equals(ushort? s)
        {
            ushort? other;
            if (!TryConvert(out other)) return false;

            return s == other;
        }

        public bool Equals(int i)
        {
            int other;
            if (!TryConvert(out other)) return false;

            return i == other;
        }

        public bool Equals(int? i)
        {
            int? other;
            if (!TryConvert(out other)) return false;

            return i == other;
        }

        public bool Equals(uint i)
        {
            uint other;
            if (!TryConvert(out other)) return false;

            return i == other;
        }

        public bool Equals(uint? i)
        {
            uint? other;
            if (!TryConvert(out other)) return false;

            return i == other;
        }

        public bool Equals(long l)
        {
            long other;
            if (!TryConvert(out other)) return false;

            return l == other;
        }

        public bool Equals(long? l)
        {
            long? other;
            if (!TryConvert(out other)) return false;

            return l == other;
        }

        public bool Equals(ulong l)
        {
            ulong other;
            if (!TryConvert(out other)) return false;

            return l == other;
        }

        public bool Equals(ulong? l)
        {
            ulong? other;
            if (!TryConvert(out other)) return false;

            return l == other;
        }

        public bool Equals(double d)
        {
            double other;
            if (!TryConvert(out other)) return false;

            return d == other;
        }

        public bool Equals(double? d)
        {
            double? other;
            if (!TryConvert(out other)) return false;

            return d == other;
        }

        public bool Equals(float f)
        {
            float other;
            if (!TryConvert(out other)) return false;

            return f == other;
        }

        public bool Equals(float? f)
        {
            float? other;
            if (!TryConvert(out other)) return false;

            return f == other;
        }

        public bool Equals(string str)
        {
            string other;
            if (!TryConvert(out other)) return false;

            return str == other;
        }

        public bool Equals(DateTime date)
        {
            DateTime other;
            if (!TryConvert(out other)) return false;

            return date == other;
        }

        public bool Equals(DateTime? date)
        {
            DateTime? other;
            if (!TryConvert(out other)) return false;

            return date == other;
        }

        public bool Equals(DateTimeOffset date)
        {
            DateTimeOffset other;
            if (!TryConvert(out other)) return false;

            return date == other;
        }

        public bool Equals(DateTimeOffset? date)
        {
            DateTimeOffset? other;
            if (!TryConvert(out other)) return false;

            return date == other;
        }

        public bool Equals(TimeSpan time)
        {
            TimeSpan other;
            if (!TryConvert(out other)) return false;

            return time == other;
        }

        public bool Equals(TimeSpan? time)
        {
            TimeSpan? other;
            if (!TryConvert(out other)) return false;

            return time == other;
        }

        public bool TryConvert(out double value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Double ||
                type == ColumnType.Float  ||
                type == ColumnType.Int16  ||
                type == ColumnType.Int32  ||
                type == ColumnType.Int64  ||
                type == ColumnType.Uint8  ||
                type == ColumnType.Uint16 ||
                type == ColumnType.Uint32 ||
                type == ColumnType.Uint64 ||
                type == ColumnType.Uint8;

            var canConvertNullable =
                type == ColumnType.NullableDouble ||
                type == ColumnType.NullableFloat  ||
                type == ColumnType.NullableInt16  ||
                type == ColumnType.NullableInt32  ||
                type == ColumnType.NullableInt64  ||
                type == ColumnType.NullableInt8   ||
                type == ColumnType.NullableUint16 ||
                type == ColumnType.NullableUint32 ||
                type == ColumnType.NullableUint64 ||
                type == ColumnType.NullableUint8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = double.NaN;
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = double.NaN;
                    return false;
                }
            }

            switch (type)
            {
                case ColumnType.Double:
                case ColumnType.NullableDouble:
                    value = Parent.ReadDouble(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Float:
                case ColumnType.NullableFloat:
                    value = Parent.ReadFloat(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int16:
                case ColumnType.NullableInt16:
                    value = Parent.ReadInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint16:
                case ColumnType.NullableUint16:
                    value = (ushort)Parent.ReadUInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int32:
                case ColumnType.NullableInt32:
                    value = Parent.ReadInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint32:
                case ColumnType.NullableUint32:
                    value = (uint)Parent.ReadUInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int64:
                case ColumnType.NullableInt64:
                    value = Parent.ReadInt64(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint64:
                case ColumnType.NullableUint64:
                    value = (ulong)Parent.ReadUInt64(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int8:
                case ColumnType.NullableInt8:
                    value = (sbyte)Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint8:
                case ColumnType.NullableUint8:
                    value = (byte)Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public bool TryConvert(out double? value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Double ||
                type == ColumnType.Float  ||
                type == ColumnType.Int16  ||
                type == ColumnType.Int32  ||
                type == ColumnType.Int64  ||
                type == ColumnType.Int8   ||
                type == ColumnType.Uint16 ||
                type == ColumnType.Uint32 ||
                type == ColumnType.Uint64 ||
                type == ColumnType.Uint8;

            var canConvertNullable =
                type == ColumnType.NullableDouble ||
                type == ColumnType.NullableFloat  ||
                type == ColumnType.NullableInt16  ||
                type == ColumnType.NullableInt32  ||
                type == ColumnType.NullableInt64  ||
                type == ColumnType.NullableInt8   ||
                type == ColumnType.NullableUint16 ||
                type == ColumnType.NullableUint32 ||
                type == ColumnType.NullableUint64 ||
                type == ColumnType.NullableUint8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            switch (type)
            {
                case ColumnType.Double:
                case ColumnType.NullableDouble:
                    value = Parent.ReadDouble(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Float:
                case ColumnType.NullableFloat:
                    value = Parent.ReadFloat(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int16:
                case ColumnType.NullableInt16:
                    value = Parent.ReadInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint16:
                case ColumnType.NullableUint16:
                    value = (ushort)Parent.ReadUInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int32:
                case ColumnType.NullableInt32:
                    value = Parent.ReadInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint32:
                case ColumnType.NullableUint32:
                    value = (uint)Parent.ReadUInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int64:
                case ColumnType.NullableInt64:
                    value = Parent.ReadInt64(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint64:
                case ColumnType.NullableUint64:
                    value = (ulong)Parent.ReadUInt64(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int8:
                case ColumnType.NullableInt8:
                    value = (sbyte)Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint8:
                case ColumnType.NullableUint8:
                    value = (byte)Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public static implicit operator double(Value value)
        {
            double ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to double, underlying type is {value.OnDiskType}");
        }

        public static implicit operator double? (Value value)
        {
            double? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to double?, underlying type is {value.OnDiskType}");
        }

        public bool TryConvert(out float value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Float  ||
                type == ColumnType.Int16  ||
                type == ColumnType.Int32  ||
                type == ColumnType.Int64  ||
                type == ColumnType.Int8   ||
                type == ColumnType.Uint16 ||
                type == ColumnType.Uint32 ||
                type == ColumnType.Uint64 ||
                type == ColumnType.Uint8;

            var canConvertNullable =
                type == ColumnType.NullableFloat  ||
                type == ColumnType.NullableInt16  ||
                type == ColumnType.NullableInt32  ||
                type == ColumnType.NullableInt64  ||
                type == ColumnType.NullableInt8   ||
                type == ColumnType.NullableUint16 ||
                type == ColumnType.NullableUint32 ||
                type == ColumnType.NullableUint64 ||
                type == ColumnType.NullableUint8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = float.NaN;
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = float.NaN;
                    return false;
                }
            }

            switch (type)
            {
                case ColumnType.Float:
                case ColumnType.NullableFloat:
                    value = Parent.ReadFloat(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int16:
                case ColumnType.NullableInt16:
                    value = Parent.ReadInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint16:
                case ColumnType.NullableUint16:
                    value = (ushort)Parent.ReadUInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int32:
                case ColumnType.NullableInt32:
                    value = Parent.ReadInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint32:
                case ColumnType.NullableUint32:
                    value = (uint)Parent.ReadUInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int64:
                case ColumnType.NullableInt64:
                    value = Parent.ReadInt64(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint64:
                case ColumnType.NullableUint64:
                    value = (ulong)Parent.ReadUInt64(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int8:
                case ColumnType.NullableInt8:
                    value = (sbyte)Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint8:
                case ColumnType.NullableUint8:
                    value = (byte)Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public bool TryConvert(out float? value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Float  ||
                type == ColumnType.Int16  ||
                type == ColumnType.Int32  ||
                type == ColumnType.Int64  ||
                type == ColumnType.Int8   ||
                type == ColumnType.Uint16 ||
                type == ColumnType.Uint32 ||
                type == ColumnType.Uint64 ||
                type == ColumnType.Uint8;

            var canConvertNullable =
                type == ColumnType.NullableFloat  ||
                type == ColumnType.NullableInt16  ||
                type == ColumnType.NullableInt32  ||
                type == ColumnType.NullableInt64  ||
                type == ColumnType.NullableInt8   ||
                type == ColumnType.NullableUint16 ||
                type == ColumnType.NullableUint32 ||
                type == ColumnType.NullableUint64 ||
                type == ColumnType.NullableUint8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            switch (type)
            {
                case ColumnType.Float:
                case ColumnType.NullableFloat:
                    value = Parent.ReadFloat(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int16:
                case ColumnType.NullableInt16:
                    value = Parent.ReadInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint16:
                case ColumnType.NullableUint16:
                    value = (ushort)Parent.ReadUInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int32:
                case ColumnType.NullableInt32:
                    value = Parent.ReadInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint32:
                case ColumnType.NullableUint32:
                    value = (uint)Parent.ReadUInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int64:
                case ColumnType.NullableInt64:
                    value = Parent.ReadInt64(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint64:
                case ColumnType.NullableUint64:
                    value = (ulong)Parent.ReadUInt64(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int8:
                case ColumnType.NullableInt8:
                    value = (sbyte)Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint8:
                case ColumnType.NullableUint8:
                    value = (byte)Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public static implicit operator float(Value value)
        {
            float ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to float, underlying type is {value.OnDiskType}");
        }

        public static implicit operator float? (Value value)
        {
            float? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to float?, underlying type is {value.OnDiskType}");
        }

        public bool TryConvert(out byte value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Uint8;

            var canConvertNullable =
                type == ColumnType.NullableUint8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = 0;
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = 0;
                    return false;
                }
            }

            switch (type)
            {
                case ColumnType.Uint8:
                case ColumnType.NullableUint8:
                    value = Parent.ReadUInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public bool TryConvert(out byte? value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Uint8;

            var canConvertNullable =
                type == ColumnType.NullableUint8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            switch (type)
            {
                case ColumnType.Uint8:
                case ColumnType.NullableUint8:
                    value = Parent.ReadUInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public static implicit operator byte(Value value)
        {
            byte ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to byte, underlying type is {value.OnDiskType}");
        }

        public static implicit operator byte? (Value value)
        {
            byte? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to byte?, underlying type is {value.OnDiskType}");
        }

        public bool TryConvert(out sbyte value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Int8;

            var canConvertNullable =
                type == ColumnType.NullableInt8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = 0;
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = 0;
                    return false;
                }
            }

            switch (type)
            {
                case ColumnType.Int8:
                case ColumnType.NullableInt8:
                    value = Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public bool TryConvert(out sbyte? value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Int8;

            var canConvertNullable =
                type == ColumnType.NullableInt8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            switch (type)
            {
                case ColumnType.Int8:
                case ColumnType.NullableInt8:
                    value = Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public static implicit operator sbyte(Value value)
        {
            sbyte ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to sbyte, underlying type is {value.OnDiskType}");
        }

        public static implicit operator sbyte? (Value value)
        {
            sbyte? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to sbyte?, underlying type is {value.OnDiskType}");
        }

        public bool TryConvert(out short value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Int16 ||
                type == ColumnType.Int8;

            var canConvertNullable =
                type == ColumnType.NullableInt16 ||
                type == ColumnType.NullableInt8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = 0;
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = 0;
                    return false;
                }
            }

            switch (type)
            {
                case ColumnType.Int16:
                case ColumnType.NullableInt16:
                    value = Parent.ReadInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int8:
                case ColumnType.NullableInt8:
                    value = Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public bool TryConvert(out short? value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Int16 ||
                type == ColumnType.Int8;

            var canConvertNullable =
                type == ColumnType.NullableInt16 ||
                type == ColumnType.NullableInt8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            switch (type)
            {
                case ColumnType.Int16:
                case ColumnType.NullableInt16:
                    value = Parent.ReadInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int8:
                case ColumnType.NullableInt8:
                    value = Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public static implicit operator short(Value value)
        {
            short ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to short, underlying type is {value.OnDiskType}");
        }

        public static implicit operator short? (Value value)
        {
            short? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to short?, underlying type is {value.OnDiskType}");
        }

        public bool TryConvert(out ushort value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Uint16 ||
                type == ColumnType.Uint8;

            var canConvertNullable =
                type == ColumnType.NullableUint16 ||
                type == ColumnType.NullableUint8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = 0;
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = 0;
                    return false;
                }
            }

            switch (type)
            {
                case ColumnType.Uint16:
                case ColumnType.NullableUint16:
                    value = (ushort)Parent.ReadUInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint8:
                case ColumnType.NullableUint8:
                    value = Parent.ReadUInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public bool TryConvert(out ushort? value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Uint16 ||
                type == ColumnType.Uint8;

            var canConvertNullable =
                type == ColumnType.NullableUint16 ||
                type == ColumnType.NullableUint8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            switch (type)
            {
                case ColumnType.Uint16:
                case ColumnType.NullableUint16:
                    value = (ushort)Parent.ReadUInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint8:
                case ColumnType.NullableUint8:
                    value = Parent.ReadUInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public static implicit operator ushort(Value value)
        {
            ushort ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to ushort, underlying type is {value.OnDiskType}");
        }

        public static implicit operator ushort? (Value value)
        {
            ushort? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to ushort?, underlying type is {value.OnDiskType}");
        }

        public bool TryConvert(out int value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Int16 ||
                type == ColumnType.Int32 ||
                type == ColumnType.Int8;

            var canConvertNullable =
                type == ColumnType.NullableInt16 ||
                type == ColumnType.NullableInt32 ||
                type == ColumnType.NullableInt8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = 0;
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = 0;
                    return false;
                }
            }

            switch (type)
            {
                case ColumnType.Int16:
                case ColumnType.NullableInt16:
                    value = Parent.ReadInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int32:
                case ColumnType.NullableInt32:
                    value = Parent.ReadInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int8:
                case ColumnType.NullableInt8:
                    value = Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public bool TryConvert(out int? value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Int16 ||
                type == ColumnType.Int32 ||
                type == ColumnType.Int8;

            var canConvertNullable =
                type == ColumnType.NullableInt16 ||
                type == ColumnType.NullableInt32 ||
                type == ColumnType.NullableInt8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            switch (type)
            {
                case ColumnType.Int16:
                case ColumnType.NullableInt16:
                    value = Parent.ReadInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int32:
                case ColumnType.NullableInt32:
                    value = Parent.ReadInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int8:
                case ColumnType.NullableInt8:
                    value = Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public static implicit operator int(Value value)
        {
            int ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to int, underlying type is {value.OnDiskType}");
        }

        public static implicit operator int? (Value value)
        {
            int? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to int?, underlying type is {value.OnDiskType}");
        }

        public bool TryConvert(out uint value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Uint16 ||
                type == ColumnType.Uint32 ||
                type == ColumnType.Uint8;

            var canConvertNullable =
                type == ColumnType.NullableUint16 ||
                type == ColumnType.NullableUint32 ||
                type == ColumnType.NullableUint8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = 0;
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = 0;
                    return false;
                }
            }

            switch (type)
            {
                case ColumnType.Uint16:
                case ColumnType.NullableUint16:
                    value = (uint)Parent.ReadUInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint32:
                case ColumnType.NullableUint32:
                    value = (uint)Parent.ReadUInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint8:
                case ColumnType.NullableUint8:
                    value = Parent.ReadUInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public bool TryConvert(out uint? value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Uint16 ||
                type == ColumnType.Uint32 ||
                type == ColumnType.Uint8;

            var canConvertNullable =
                type == ColumnType.NullableUint16 ||
                type == ColumnType.NullableUint32 ||
                type == ColumnType.NullableUint8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            switch (type)
            {
                case ColumnType.Uint16:
                case ColumnType.NullableUint16:
                    value = (ushort)Parent.ReadUInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint32:
                case ColumnType.NullableUint32:
                    value = (uint)Parent.ReadUInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint8:
                case ColumnType.NullableUint8:
                    value = Parent.ReadUInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public static implicit operator uint(Value value)
        {
            uint ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to uint, underlying type is {value.OnDiskType}");
        }

        public static implicit operator uint? (Value value)
        {
            uint? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to uint?, underlying type is {value.OnDiskType}");
        }

        public bool TryConvert(out long value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Int16 ||
                type == ColumnType.Int32 ||
                type == ColumnType.Int64 ||
                type == ColumnType.Int8;

            var canConvertNullable =
                type == ColumnType.NullableInt16 ||
                type == ColumnType.NullableInt32 ||
                type == ColumnType.NullableInt64 ||
                type == ColumnType.NullableInt8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = 0;
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = 0;
                    return false;
                }
            }

            switch (type)
            {
                case ColumnType.Int16:
                case ColumnType.NullableInt16:
                    value = Parent.ReadInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int32:
                case ColumnType.NullableInt32:
                    value = Parent.ReadInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int64:
                case ColumnType.NullableInt64:
                    value = Parent.ReadInt64(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int8:
                case ColumnType.NullableInt8:
                    value = Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public bool TryConvert(out long? value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Int16 ||
                type == ColumnType.Int32 ||
                type == ColumnType.Int64 ||
                type == ColumnType.Int8;

            var canConvertNullable =
                type == ColumnType.NullableInt16 ||
                type == ColumnType.NullableInt32 ||
                type == ColumnType.NullableInt64 ||
                type == ColumnType.NullableInt8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            switch (type)
            {
                case ColumnType.Int16:
                case ColumnType.NullableInt16:
                    value = Parent.ReadInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int32:
                case ColumnType.NullableInt32:
                    value = Parent.ReadInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int64:
                case ColumnType.NullableInt64:
                    value = Parent.ReadInt64(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Int8:
                case ColumnType.NullableInt8:
                    value = Parent.ReadInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public static implicit operator long(Value value)
        {
            long ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to long, underlying type is {value.OnDiskType}");
        }

        public static implicit operator long? (Value value)
        {
            long? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to long?, underlying type is {value.OnDiskType}");
        }

        public bool TryConvert(out ulong value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Uint16 ||
                type == ColumnType.Uint32 ||
                type == ColumnType.Uint64 ||
                type == ColumnType.Uint8;

            var canConvertNullable =
                type == ColumnType.NullableUint16 ||
                type == ColumnType.NullableUint32 ||
                type == ColumnType.NullableUint64 ||
                type == ColumnType.NullableUint8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = 0;
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = 0;
                    return false;
                }
            }

            switch (type)
            {
                case ColumnType.Uint16:
                case ColumnType.NullableUint16:
                    value = (ushort)Parent.ReadUInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint32:
                case ColumnType.NullableUint32:
                    value = (uint)Parent.ReadUInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint64:
                case ColumnType.NullableUint64:
                    value = (ulong)Parent.ReadUInt64(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint8:
                case ColumnType.NullableUint8:
                    value = Parent.ReadUInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public bool TryConvert(out ulong? value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Uint16 ||
                type == ColumnType.Uint32 ||
                type == ColumnType.Uint64 ||
                type == ColumnType.Uint8;

            var canConvertNullable =
                type == ColumnType.NullableUint16 ||
                type == ColumnType.NullableUint32 ||
                type == ColumnType.NullableUint64 ||
                type == ColumnType.NullableUint8;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            switch (type)
            {
                case ColumnType.Uint16:
                case ColumnType.NullableUint16:
                    value = (ushort)Parent.ReadUInt16(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint32:
                case ColumnType.NullableUint32:
                    value = (uint)Parent.ReadUInt32(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint64:
                case ColumnType.NullableUint64:
                    value = (ulong)Parent.ReadUInt64(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;
                case ColumnType.Uint8:
                case ColumnType.NullableUint8:
                    value = Parent.ReadUInt8(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public static implicit operator ulong(Value value)
        {
            ulong ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to ulong, underlying type is {value.OnDiskType}");
        }

        public static implicit operator ulong? (Value value)
        {
            ulong? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to ulong?, underlying type is {value.OnDiskType}");
        }

        public bool TryConvert(out string value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.String;

            var canConvertNullable =
                type == ColumnType.NullableString;

            var canConvertCategory = IsCategory;

            var canConvert = canConvertNonNullable || canConvertNullable || canConvertCategory;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertCategory)
            {
                var isNullable = OnDiskType == ColumnType.NullableInt32;
                var categories = Categories;

                if (isNullable)
                {
                    var asIndex = (int?)this;
                    if (asIndex == null)
                    {
                        value = null;
                        return true;
                    }

                    if(asIndex.Value < 0 || asIndex.Value >= categories.Length)
                    {
                        value = null;
                        return false;
                    }

                    value = categories[asIndex.Value];
                }
                else
                {
                    var asIndex = (int)this;
                    if (asIndex < 0 || asIndex >= categories.Length)
                    {
                        value = null;
                        return false;
                    }

                    value = categories[asIndex];
                }
                
                return true;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            switch (type)
            {
                case ColumnType.String:
                    value = Parent.ReadString(TranslatedRowIndex, TranslatedColumnIndex) ?? "";    // non-nullable string should never return null
                    return true;
                case ColumnType.NullableString:
                    value = Parent.ReadString(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public static implicit operator string(Value value)
        {
            string ret;
            if(value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to string, underlying type is {value.OnDiskType}");
        }

        public static implicit operator bool(Value value)
        {
            bool ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to bool, underlying type is {value.OnDiskType}");
        }

        public static implicit operator bool? (Value value)
        {
            bool? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to bool?, underlying type is {value.OnDiskType}");
        }

        public bool TryConvert(out bool value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Bool;

            var canConvertNullable =
                type == ColumnType.NullableBool;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = false;
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = false;
                    return false;
                }
            }

            switch (type)
            {
                case ColumnType.Bool:
                case ColumnType.NullableBool:
                    value = Parent.ReadBool(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public bool TryConvert(out bool? value)
        {
            var type = OnDiskType;
            
            var canConvertNonNullable =
                type == ColumnType.Bool;

            var canConvertNullable =
                type == ColumnType.NullableBool;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            switch (type)
            {
                case ColumnType.Bool:
                case ColumnType.NullableBool:
                    value = Parent.ReadBool(TranslatedRowIndex, TranslatedColumnIndex);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        
        const long NANOSECONDS_PER_TICK = 100;
        const double TICKS_PER_NANOSECOND = 1.0 / NANOSECONDS_PER_TICK;
        const long TICKS_PER_MICROSECOND = (long)(TICKS_PER_NANOSECOND * 1000);
        const long TICKS_PER_MILLISECOND = (TICKS_PER_MICROSECOND * 1000);
        const long TICKS_PER_SECOND = (TICKS_PER_MILLISECOND * 1000);

        public static implicit operator DateTime(Value value)
        {
            DateTime ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to DateTime, underlying type is {value.OnDiskType}");
        }

        public static implicit operator DateTime? (Value value)
        {
            DateTime? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to DateTime?, underlying type is {value.OnDiskType}");
        }

        public static implicit operator DateTimeOffset(Value value) => (DateTime)value;
        public static implicit operator DateTimeOffset?(Value value) => (DateTime?)value;

        public bool TryConvert(out DateTime value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Timestamp_Microsecond ||
                type == ColumnType.Timestamp_Millisecond ||
                type == ColumnType.Timestamp_Nanosecond ||
                type == ColumnType.Timestamp_Second ||
                type == ColumnType.Date;

            var canConvertNullable =
                type == ColumnType.NullableTimestamp_Microsecond ||
                type == ColumnType.NullableTimestamp_Millisecond ||
                type == ColumnType.NullableTimestamp_Nanosecond ||
                type == ColumnType.NullableTimestamp_Second ||
                type == ColumnType.NullableDate;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = default(DateTime);
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = default(DateTime);
                    return false;
                }
            }

            var isDate = type == ColumnType.Date || type == ColumnType.NullableDate;

            if (isDate)
            {
                var val = Parent.ReadInt32(TranslatedRowIndex, TranslatedColumnIndex);

                value = FeatherMagic.DATETIME_EPOCH + TimeSpan.FromDays(val);
                return true;
            }
            else
            {
                var val = Parent.ReadInt64(TranslatedRowIndex, TranslatedColumnIndex);

                switch (type)
                {
                    case ColumnType.Timestamp_Microsecond:
                    case ColumnType.NullableTimestamp_Microsecond:
                        value = FeatherMagic.DATETIME_EPOCH + TimeSpan.FromTicks(val * TICKS_PER_MICROSECOND);
                        return true;

                    case ColumnType.Timestamp_Millisecond:
                    case ColumnType.NullableTimestamp_Millisecond:
                        value = FeatherMagic.DATETIME_EPOCH + TimeSpan.FromTicks(val * TICKS_PER_MILLISECOND);
                        return true;

                    case ColumnType.Timestamp_Nanosecond:
                    case ColumnType.NullableTimestamp_Nanosecond:
                        value = FeatherMagic.DATETIME_EPOCH + TimeSpan.FromTicks((long)Math.Round((val * TICKS_PER_NANOSECOND)));
                        return true;

                    case ColumnType.Timestamp_Second:
                    case ColumnType.NullableTimestamp_Second:
                        value = FeatherMagic.DATETIME_EPOCH + TimeSpan.FromTicks(val * TICKS_PER_SECOND);
                        return true;

                    default: throw new InvalidOperationException($"Unexpected column type {type}");
                }
            }
        }

        public bool TryConvert(out DateTime? value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Timestamp_Microsecond ||
                type == ColumnType.Timestamp_Millisecond ||
                type == ColumnType.Timestamp_Nanosecond ||
                type == ColumnType.Timestamp_Second ||
                type == ColumnType.Date;

            var canConvertNullable =
                type == ColumnType.NullableTimestamp_Microsecond ||
                type == ColumnType.NullableTimestamp_Millisecond ||
                type == ColumnType.NullableTimestamp_Nanosecond ||
                type == ColumnType.NullableTimestamp_Second ||
                type == ColumnType.NullableDate;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            var isDate = type == ColumnType.Date || type == ColumnType.NullableDate;

            if (isDate)
            {
                var val = Parent.ReadInt32(TranslatedRowIndex, TranslatedColumnIndex);

                value = FeatherMagic.DATETIME_EPOCH + TimeSpan.FromDays(val);
                return true;
            }
            else
            {
                var val = Parent.ReadInt64(TranslatedRowIndex, TranslatedColumnIndex);

                switch (type)
                {
                    case ColumnType.Timestamp_Microsecond:
                    case ColumnType.NullableTimestamp_Microsecond:
                        value = FeatherMagic.DATETIME_EPOCH + TimeSpan.FromTicks(val * TICKS_PER_MICROSECOND);
                        return true;

                    case ColumnType.Timestamp_Millisecond:
                    case ColumnType.NullableTimestamp_Millisecond:
                        value = FeatherMagic.DATETIME_EPOCH + TimeSpan.FromTicks(val * TICKS_PER_MILLISECOND);
                        return true;

                    case ColumnType.Timestamp_Nanosecond:
                    case ColumnType.NullableTimestamp_Nanosecond:
                        value = FeatherMagic.DATETIME_EPOCH + TimeSpan.FromTicks((long)Math.Round((val * TICKS_PER_NANOSECOND)));
                        return true;

                    case ColumnType.Timestamp_Second:
                    case ColumnType.NullableTimestamp_Second:
                        value = FeatherMagic.DATETIME_EPOCH + TimeSpan.FromTicks(val * TICKS_PER_SECOND);
                        return true;

                    default: throw new InvalidOperationException($"Unexpected column type {type}");
                }
            }
        }

        public bool TryConvert(out DateTimeOffset value)
        {
            DateTime dt;
            if(!TryConvert(out dt))
            {
                value = default(DateTimeOffset);
                return false;
            }

            value = dt;
            return true;
        }

        public bool TryConvert(out DateTimeOffset? value)
        {
            DateTime? dt;
            if (!TryConvert(out dt))
            {
                value = null;
                return false;
            }

            value = dt;
            return true;
        }

        public static implicit operator TimeSpan(Value value)
        {
            TimeSpan ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to TimeSpan, underlying type is {value.OnDiskType}");
        }

        public static implicit operator TimeSpan? (Value value)
        {
            TimeSpan? ret;
            if (value.TryConvert(out ret))
            {
                return ret;
            }

            throw new InvalidCastException($"Could not convert value to TimeSpan?, underlying type is {value.OnDiskType}");
        }

        public bool TryConvert(out TimeSpan value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Time_Microsecond ||
                type == ColumnType.Time_Millisecond ||
                type == ColumnType.Time_Nanosecond ||
                type == ColumnType.Time_Second;

            var canConvertNullable =
                type == ColumnType.NullableTime_Microsecond ||
                type == ColumnType.NullableTime_Millisecond ||
                type == ColumnType.NullableTime_Nanosecond ||
                type == ColumnType.NullableTime_Second;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = default(TimeSpan);
                return false;
            }

            if (canConvertNullable)
            {
                // null isn't castable, freak out
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = default(TimeSpan);
                    return false;
                }
            }

            var val = Parent.ReadInt64(TranslatedRowIndex, TranslatedColumnIndex);

            switch (type)
            {
                case ColumnType.NullableTime_Microsecond:
                case ColumnType.Time_Microsecond:
                    value = TimeSpan.FromTicks(val * TICKS_PER_MICROSECOND);
                    return true;

                case ColumnType.NullableTime_Millisecond:
                case ColumnType.Time_Millisecond:
                    value = TimeSpan.FromTicks(val * TICKS_PER_MILLISECOND);
                    return true;

                case ColumnType.NullableTime_Nanosecond:
                case ColumnType.Time_Nanosecond:
                    value = TimeSpan.FromTicks((long)Math.Round(val * TICKS_PER_NANOSECOND));
                    return true;

                case ColumnType.NullableTime_Second:
                case ColumnType.Time_Second:
                    value = TimeSpan.FromTicks(val * TICKS_PER_SECOND);
                    return true;
                    
                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public bool TryConvert(out TimeSpan? value)
        {
            var type = OnDiskType;

            var canConvertNonNullable =
                type == ColumnType.Time_Microsecond ||
                type == ColumnType.Time_Millisecond ||
                type == ColumnType.Time_Nanosecond ||
                type == ColumnType.Time_Second;

            var canConvertNullable =
                type == ColumnType.NullableTime_Microsecond ||
                type == ColumnType.NullableTime_Millisecond ||
                type == ColumnType.NullableTime_Nanosecond ||
                type == ColumnType.NullableTime_Second;

            var canConvert = canConvertNonNullable || canConvertNullable;

            if (!canConvert)
            {
                value = null;
                return false;
            }

            if (canConvertNullable)
            {
                if (Parent.IsNullTranslated(TranslatedRowIndex, TranslatedColumnIndex))
                {
                    value = null;
                    return true;
                }
            }

            var val = Parent.ReadInt64(TranslatedRowIndex, TranslatedColumnIndex);

            switch (type)
            {
                case ColumnType.NullableTime_Microsecond:
                case ColumnType.Time_Microsecond:
                    value = TimeSpan.FromTicks(val * TICKS_PER_MICROSECOND);
                    return true;

                case ColumnType.NullableTime_Millisecond:
                case ColumnType.Time_Millisecond:
                    value = TimeSpan.FromTicks(val * TICKS_PER_MILLISECOND);
                    return true;

                case ColumnType.NullableTime_Nanosecond:
                case ColumnType.Time_Nanosecond:
                    value = TimeSpan.FromTicks((long)Math.Round(val * TICKS_PER_NANOSECOND));
                    return true;

                case ColumnType.NullableTime_Second:
                case ColumnType.Time_Second:
                    value = TimeSpan.FromTicks(val * TICKS_PER_SECOND);
                    return true;

                default: throw new InvalidOperationException($"Unexpected column type {type}");
            }
        }

        public override string ToString()
        {
            string valStr;
            switch (OnDiskType)
            {
                // TODO (Binary)
                case ColumnType.Binary:
                case ColumnType.NullableBinary:
                    throw new NotImplementedException();
                // END TODO

                case ColumnType.Category:
                case ColumnType.NullableCategory:
                case ColumnType.String: valStr = "\"" + this + "\""; break;
                case ColumnType.NullableString:
                    var asStr = ((string)this);
                    valStr = asStr != null ?  "\"" + asStr + "\"" : "null";
                    break;

                case ColumnType.Bool: valStr = ((bool)this).ToString(); break;
                case ColumnType.NullableBool:
                    var asBool = ((bool?)this);
                    valStr = asBool != null ? asBool.ToString() : "null";
                    break;

                case ColumnType.Date:
                case ColumnType.Timestamp_Microsecond:
                case ColumnType.Timestamp_Millisecond:
                case ColumnType.Timestamp_Nanosecond:
                case ColumnType.Timestamp_Second:
                    valStr = ((DateTime)this).ToString();
                    break;

                case ColumnType.NullableDate:
                case ColumnType.NullableTimestamp_Microsecond:
                case ColumnType.NullableTimestamp_Millisecond:
                case ColumnType.NullableTimestamp_Nanosecond:
                case ColumnType.NullableTimestamp_Second:
                    var asDatetime = ((DateTime?)this);
                    valStr = asDatetime != null ? asDatetime.ToString() : "null";
                    break;

                case ColumnType.Time_Microsecond:
                case ColumnType.Time_Millisecond:
                    valStr = ((TimeSpan)this).ToString();
                    break;

                case ColumnType.Time_Nanosecond:
                case ColumnType.Time_Second:
                    var asTimeSpan = ((TimeSpan?)this);
                    valStr = asTimeSpan != null ? asTimeSpan.ToString() : "null";
                    break;

                case ColumnType.Double: valStr = ((double)this).ToString(); break;
                case ColumnType.NullableDouble:
                    var asNullableDouble = (double?)this;
                    valStr = asNullableDouble.HasValue ? asNullableDouble.ToString() : "null";
                    break;

                case ColumnType.Float: valStr = ((float)this).ToString(); break;
                case ColumnType.NullableFloat:
                    var asNullableFloat = (float?)this;
                    valStr = asNullableFloat.HasValue ? asNullableFloat.ToString() : "null";
                    break;

                case ColumnType.Int16: valStr = ((short)this).ToString(); break;
                case ColumnType.NullableInt16:
                    var asNullableShort = (short?)this;
                    valStr = asNullableShort.HasValue ? asNullableShort.ToString() : "null";
                    break;

                case ColumnType.Uint16: valStr = ((ushort)this).ToString(); break;
                case ColumnType.NullableUint16:
                    var asNullableUShort = (ushort?)this;
                    valStr = asNullableUShort.HasValue ? asNullableUShort.ToString() : "null";
                    break;

                case ColumnType.Int32: valStr = ((int)this).ToString(); break;
                case ColumnType.NullableInt32:
                    var asNullableInt = (int?)this;
                    valStr = asNullableInt.HasValue ? asNullableInt.ToString() : "null";
                    break;

                case ColumnType.Uint32: valStr = ((uint)this).ToString(); break;
                case ColumnType.NullableUint32:
                    var asNullableUint = (uint?)this;
                    valStr = asNullableUint.HasValue ? asNullableUint.ToString() : "null";
                    break;

                case ColumnType.Int64: valStr = ((long)this).ToString(); break;
                case ColumnType.NullableInt64:
                    var asNullableLong = (long?)this;
                    valStr = asNullableLong.HasValue ? asNullableLong.ToString() : "null";
                    break;

                case ColumnType.Uint64: valStr = ((ulong)this).ToString(); break;
                case ColumnType.NullableUint64:
                    var asNullableUlong = (ulong?)this;
                    valStr = asNullableUlong.HasValue ? asNullableUlong.ToString() : "null";
                    break;

                case ColumnType.Int8: valStr = ((sbyte)this).ToString(); break;
                case ColumnType.NullableInt8:
                    var asNullableSbyte = (sbyte?)this;
                    valStr = asNullableSbyte.HasValue ? asNullableSbyte.ToString() : "null";
                    break;

                case ColumnType.Uint8: valStr = ((byte)this).ToString(); break;
                case ColumnType.NullableUint8:
                    var asNullableByte = (byte?)this;
                    valStr = asNullableByte.HasValue ? asNullableByte.ToString() : "null";
                    break;

                default: throw new InvalidOperationException($"Unexpected ColumnType {Type}");
            }

            return $"Value Type={OnDiskType}, Row={Parent.UntranslateIndex(TranslatedRowIndex)}, Column={Parent.UntranslateIndex(TranslatedColumnIndex)}, Value={valStr}";
        }
#pragma warning restore 1591

        // unsafe 'cause it'll explode messily if the conversion isn't valid
        internal T UnsafeCast<T>(CategoryEnumMapType categories)
        {
            return ValueCaster<T>.Cast(this, categories);
        }

        internal static T ConvertEnum<T>(Value value, CategoryEnumMapType categories)
            where T: struct
        {
            switch (categories)
            {
                case CategoryEnumMapType.ByName: return (T)Enum.Parse(typeof(T), value, ignoreCase: true);
                case CategoryEnumMapType.ByOrdinal:
                    var asInt = (int)value;
                    return EnumMapper<T>.Map(asInt);

                default: throw new Exception($"Unexpected {nameof(CategoryEnumMapType)} {categories}");
            }
        }

        internal static T? ConvertNullableEnum<T>(Value value, CategoryEnumMapType categories)
            where T : struct
        {
            var isNullable =
                value.OnDiskType == ColumnType.NullableInt32 ||
                value.OnDiskType == ColumnType.NullableString;

            if (isNullable)
            {
                if (value.Parent.IsNullTranslated(value.TranslatedRowIndex, value.TranslatedColumnIndex))
                {
                    return null;
                }
            }

            switch (categories)
            {
                case CategoryEnumMapType.ByName: return (T)Enum.Parse(typeof(T), value, ignoreCase: true);
                case CategoryEnumMapType.ByOrdinal:
                    var asInt = (int)value;
                    return EnumMapper<T>.Map(asInt);

                default: throw new Exception($"Unexpected {nameof(CategoryEnumMapType)} {categories}");
            }
        }

        static bool CategoryEquals(Value a, Value b)
        {
            if (!a.IsCategory && !b.IsCategory) throw new Exception($"Shouldn't be possible, tried to compare two non-category types");
            if (a.IsCategory && !b.IsCategory) return false;
            if (b.IsCategory && !a.IsCategory) return false;

            // these are often zero-allocation, so might as well
            var aAsStr = (string)a;
            var bAsStr = (string)b;

            return aAsStr == bAsStr;
        }

        static bool NullableCategoryEquals(Value a, Value b) => CategoryEquals(a, b);
    }
}
