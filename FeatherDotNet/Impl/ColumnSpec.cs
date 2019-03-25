using System;
using System.Collections.Generic;

namespace FeatherDotNet.Impl
{
    enum DateTimePrecisionType
    {
        NONE = 0,

        Nanosecond,
        Microsecond,
        Millisecond,
        Second,
    }

    enum CategoryEnumMapType : byte
    {
        NONE = 0,

        ByName,
        ByOrdinal
    }

    enum ColumnType : byte
    {
        NONE = 0,

        Bool,

        Int8,
        Int16,
        Int32,
        Int64,

        Uint8,
        Uint16,
        Uint32,
        Uint64,

        Float,
        Double,

        Binary,

        Category,

        Timestamp_Second,
        Timestamp_Millisecond,
        Timestamp_Microsecond,
        Timestamp_Nanosecond,

        Time_Second,
        Time_Millisecond,
        Time_Microsecond,
        Time_Nanosecond,

        Date,

        String,

        // Nullable!
        NullableBool,

        NullableInt8,
        NullableInt16,
        NullableInt32,
        NullableInt64,

        NullableUint8,
        NullableUint16,
        NullableUint32,
        NullableUint64,

        NullableFloat,
        NullableDouble,

        NullableBinary,

        NullableCategory,

        NullableTimestamp_Second,
        NullableTimestamp_Millisecond,
        NullableTimestamp_Microsecond,
        NullableTimestamp_Nanosecond,

        NullableTime_Second,
        NullableTime_Millisecond,
        NullableTime_Microsecond,
        NullableTime_Nanosecond,

        NullableDate,

        NullableString
    }

    static class ColumnTypeExtensionMethods
    {
        public static feather.fbs.Type MapToFeatherEnum(this ColumnType onDiskType)
        {
            switch (onDiskType)
            {
                case ColumnType.Binary:
                case ColumnType.NullableBinary: return feather.fbs.Type.BINARY;

                case ColumnType.Bool:
                case ColumnType.NullableBool: return feather.fbs.Type.BOOL;

                case ColumnType.Category:
                case ColumnType.NullableCategory:
                    //return feather.fbs.Type.CATEGORY;
                    // note: even though there is a CATEGORY type, R produces Int32 backed
                    //   columns.  Possibly a bug?
                    return feather.fbs.Type.INT32;

                case ColumnType.Date:
                case ColumnType.NullableDate: return feather.fbs.Type.DATE;

                case ColumnType.Double:
                case ColumnType.NullableDouble: return feather.fbs.Type.DOUBLE;

                case ColumnType.Float:
                case ColumnType.NullableFloat: return feather.fbs.Type.FLOAT;

                case ColumnType.Int16:
                case ColumnType.NullableInt16: return feather.fbs.Type.INT16;

                case ColumnType.Int32:
                case ColumnType.NullableInt32: return feather.fbs.Type.INT32;

                case ColumnType.Int64:
                case ColumnType.NullableInt64: return feather.fbs.Type.INT64;

                case ColumnType.Int8:
                case ColumnType.NullableInt8: return feather.fbs.Type.INT8;
                    
                case ColumnType.Timestamp_Millisecond:
                case ColumnType.Timestamp_Microsecond:
                case ColumnType.Timestamp_Second:
                case ColumnType.Timestamp_Nanosecond:
                case ColumnType.NullableTimestamp_Millisecond:
                case ColumnType.NullableTimestamp_Microsecond:
                case ColumnType.NullableTimestamp_Second:
                case ColumnType.NullableTimestamp_Nanosecond:
                    //return feather.fbs.Type.TIMESTAMP;
                    // note: even though there is a TIMESTAMP type, the actual primitive's the R library produces
                    //  is an int64
                    return feather.fbs.Type.INT64;

                case ColumnType.Time_Microsecond:
                case ColumnType.Time_Millisecond:
                case ColumnType.Time_Nanosecond:
                case ColumnType.Time_Second:
                case ColumnType.NullableTime_Microsecond:
                case ColumnType.NullableTime_Millisecond:
                case ColumnType.NullableTime_Nanosecond:
                case ColumnType.NullableTime_Second:
                    //return feather.fbs.Type.TIME;
                    // note: even though there is a TIME type, the actual primitive's the R library produces
                    //  is an int64
                    return feather.fbs.Type.INT64;

                case ColumnType.Uint16:
                case ColumnType.NullableUint16: return feather.fbs.Type.UINT16;

                case ColumnType.Uint32:
                case ColumnType.NullableUint32: return feather.fbs.Type.UINT32;

                case ColumnType.Uint64:
                case ColumnType.NullableUint64: return feather.fbs.Type.UINT64;

                case ColumnType.Uint8:
                case ColumnType.NullableUint8: return feather.fbs.Type.UINT8;

                case ColumnType.String:
                case ColumnType.NullableString: return feather.fbs.Type.UTF8;

                default: throw new Exception($"Unexpected ColumnType {onDiskType}");
            }
        }

        public static byte GetAlignment(this ColumnType onDiskType)
        {
            switch (onDiskType)
            {
                case ColumnType.Bool:
                case ColumnType.NullableBool: return sizeof(byte);

                case ColumnType.Category:
                    return 4;

                // TODO (Binary)
                case ColumnType.NullableBinary:
                case ColumnType.Binary:
                    throw new NotImplementedException();
                // END TODO

                case ColumnType.Date:
                    return 4;

                case ColumnType.NullableDouble:
                case ColumnType.Double: return sizeof(double);

                case ColumnType.NullableFloat:
                case ColumnType.Float: return sizeof(float);

                case ColumnType.NullableInt16:
                case ColumnType.Int16: return sizeof(short);

                case ColumnType.NullableUint16:
                case ColumnType.Uint16: return sizeof(ushort);

                case ColumnType.NullableInt32:
                case ColumnType.Int32: return sizeof(int);

                case ColumnType.NullableUint32:
                case ColumnType.Uint32: return sizeof(uint);

                case ColumnType.NullableInt64:
                case ColumnType.Int64: return sizeof(long);

                case ColumnType.NullableUint64:
                case ColumnType.Uint64: return sizeof(ulong);

                case ColumnType.NullableInt8:
                case ColumnType.Int8: return sizeof(sbyte);

                case ColumnType.NullableUint8:
                case ColumnType.Uint8: return sizeof(byte);

                case ColumnType.String:
                case ColumnType.NullableString:
                    // variable length array composed of int32_t offsets followed by uint8_t's
                    return 4;
                    
                case ColumnType.NullableTimestamp_Microsecond:
                case ColumnType.NullableTimestamp_Millisecond:
                case ColumnType.NullableTimestamp_Nanosecond:
                case ColumnType.NullableTimestamp_Second:
                case ColumnType.NullableTime_Microsecond:
                case ColumnType.NullableTime_Millisecond:
                case ColumnType.NullableTime_Nanosecond:
                case ColumnType.NullableTime_Second:
                case ColumnType.Time_Microsecond:
                case ColumnType.Time_Millisecond:
                case ColumnType.Time_Nanosecond:
                case ColumnType.Time_Second:
                case ColumnType.Timestamp_Microsecond:
                case ColumnType.Timestamp_Millisecond:
                case ColumnType.Timestamp_Nanosecond:
                case ColumnType.Timestamp_Second:
                    return 8;
                    
                default: throw new InvalidOperationException($"Unexpected ColumnType {onDiskType}");
            }
        }

        public static bool CanMapTo(this ColumnType fromType, Type toType, string[] categoryMetadata)
        {
            if (toType == typeof(Value)) return true;

            var hasCategoryMetadata = categoryMetadata != null;

            if (toType == typeof(string) && hasCategoryMetadata)
            {
                if (fromType == ColumnType.Int32 || fromType == ColumnType.NullableInt32) return true;
            }

            var isToTypeEnum = toType.IsEnum;
            var isToTypeNullable = Nullable.GetUnderlyingType(toType) != null;
            var isToTypeNullableEnum = isToTypeNullable && Nullable.GetUnderlyingType(toType).IsEnum;

            if (isToTypeEnum || isToTypeNullableEnum)
            {
                var enumType = isToTypeEnum ? toType : Nullable.GetUnderlyingType(toType);

                if (isToTypeEnum)
                {
                    CategoryEnumMapType _;
                    if (fromType == ColumnType.Int32 && TryCategoriesMapToEnum(enumType, categoryMetadata, out _)) return true;
                }

                if (isToTypeNullableEnum)
                {
                    CategoryEnumMapType _;
                    if ((fromType == ColumnType.Int32 || fromType == ColumnType.NullableInt32) && TryCategoriesMapToEnum(enumType, categoryMetadata, out _)) return true;
                }
            }

            switch (fromType)
            {
                case ColumnType.NullableBinary:
                case ColumnType.Binary:
                    return toType == typeof(byte[]);

                case ColumnType.Bool:
                    return toType == typeof(bool) || toType == typeof(bool?);

                case ColumnType.Category:
                    // categories are handled above
                    return false;

                case ColumnType.Date:
                    return toType == typeof(DateTime) || toType == typeof(DateTime?) || toType == typeof(DateTimeOffset) || toType == typeof(DateTimeOffset?);

                case ColumnType.Double:
                    return toType == typeof(double) || toType == typeof(double?);

                case ColumnType.Float:
                    return toType == typeof(float) || toType == typeof(float?) || toType == typeof(double) || toType == typeof(double?);

                case ColumnType.Int16:
                    return toType == typeof(long) || toType == typeof(long?) || toType == typeof(int) || toType == typeof(int?) || toType == typeof(short) || toType == typeof(short?) || toType == typeof(float) || toType == typeof(float?) || toType == typeof(double) || toType == typeof(double?);

                case ColumnType.Uint16:
                    return toType == typeof(ulong) || toType == typeof(ulong?) || toType == typeof(uint) || toType == typeof(uint?) || toType == typeof(ushort) || toType == typeof(ushort?) || toType == typeof(int) || toType == typeof(uint) || toType == typeof(float) || toType == typeof(float?) || toType == typeof(double) || toType == typeof(double?);

                case ColumnType.Int32:
                    return toType == typeof(long) || toType == typeof(long?) || toType == typeof(int) || toType == typeof(int?) || toType == typeof(float) || toType == typeof(float?) || toType == typeof(double) || toType == typeof(double?);

                case ColumnType.Uint32:
                    return toType == typeof(ulong) || toType == typeof(ulong?) || toType == typeof(uint) || toType == typeof(uint?) || toType == typeof(float) || toType == typeof(float?) || toType == typeof(double) || toType == typeof(double?);

                case ColumnType.Int64:
                    return toType == typeof(long) || toType == typeof(long?) || toType == typeof(float) || toType == typeof(float?) || toType == typeof(double) || toType == typeof(double?);

                case ColumnType.Uint64:
                    return toType == typeof(ulong) || toType == typeof(ulong?) || toType == typeof(float) || toType == typeof(float?) || toType == typeof(double) || toType == typeof(double?);

                case ColumnType.Int8:
                    return toType == typeof(sbyte) || toType == typeof(sbyte?) || toType == typeof(long) || toType == typeof(long?) || toType == typeof(int) || toType == typeof(int?) || toType == typeof(short) || toType == typeof(short?) || toType == typeof(float) || toType == typeof(float?) || toType == typeof(double) || toType == typeof(double?);

                case ColumnType.Uint8:
                    return toType == typeof(byte) || toType == typeof(byte?) || toType == typeof(ulong) || toType == typeof(ulong?) || toType == typeof(uint) || toType == typeof(uint?) || toType == typeof(ushort) || toType == typeof(ushort?) || toType == typeof(int) || toType == typeof(float) || toType == typeof(float?) || toType == typeof(double) || toType == typeof(double?);

                case ColumnType.NullableBool:
                    return toType == typeof(bool?);

                case ColumnType.NullableCategory:
                    // categories are handled above
                    return false;

                case ColumnType.NullableDate:
                    return toType == typeof(DateTime?) || toType == typeof(DateTimeOffset?);

                case ColumnType.NullableDouble:
                    return toType == typeof(double?);

                case ColumnType.NullableFloat:
                    return toType == typeof(float?) || toType == typeof(double?);

                case ColumnType.NullableInt16:
                    return toType == typeof(long?) || toType == typeof(int?) || toType == typeof(short?) || toType == typeof(float?) || toType == typeof(double?);

                case ColumnType.NullableUint16:
                    return toType == typeof(ulong?) || toType == typeof(uint?) || toType == typeof(ushort?) || toType == typeof(float?) || toType == typeof(double?);

                case ColumnType.NullableInt32:
                    return toType == typeof(long?) || toType == typeof(int?) || toType == typeof(float?) || toType == typeof(double?);

                case ColumnType.NullableUint32:
                    return toType == typeof(ulong?) || toType == typeof(uint?) || toType == typeof(float?) || toType == typeof(double?);

                case ColumnType.NullableInt64:
                    return toType == typeof(long?) || toType == typeof(float?) || toType == typeof(double?);

                case ColumnType.NullableUint64:
                    return toType == typeof(ulong?) || toType == typeof(float?) || toType == typeof(double?);

                case ColumnType.NullableInt8:
                    return toType == typeof(sbyte?) || toType == typeof(long?) || toType == typeof(int?) || toType == typeof(short?) || toType == typeof(float) || toType == typeof(float?) || toType == typeof(double) || toType == typeof(double?);

                case ColumnType.NullableUint8:
                    return toType == typeof(byte?) || toType == typeof(ulong?) || toType == typeof(uint?) || toType == typeof(ushort?) || toType == typeof(float) || toType == typeof(float?) || toType == typeof(double) || toType == typeof(double?);

                case ColumnType.String:
                case ColumnType.NullableString:
                    return toType == typeof(string);

                case ColumnType.Timestamp_Microsecond:
                case ColumnType.Timestamp_Millisecond:
                case ColumnType.Timestamp_Nanosecond:
                case ColumnType.Timestamp_Second:
                    return toType == typeof(DateTime) || toType == typeof(DateTime?) || toType == typeof(DateTimeOffset) || toType == typeof(DateTimeOffset?);

                case ColumnType.NullableTimestamp_Microsecond:
                case ColumnType.NullableTimestamp_Millisecond:
                case ColumnType.NullableTimestamp_Nanosecond:
                case ColumnType.NullableTimestamp_Second:
                    return toType == typeof(DateTime?) || toType == typeof(DateTimeOffset?);

                case ColumnType.Time_Microsecond:
                case ColumnType.Time_Millisecond:
                case ColumnType.Time_Nanosecond:
                case ColumnType.Time_Second:
                    return toType == typeof(TimeSpan) || toType == typeof(TimeSpan?);

                case ColumnType.NullableTime_Microsecond:
                case ColumnType.NullableTime_Millisecond:
                case ColumnType.NullableTime_Nanosecond:
                case ColumnType.NullableTime_Second:
                    return toType == typeof(TimeSpan?);

                default: throw new Exception($"Unexpected ColumnType {fromType}");
            }
        }

        public static Type GetMapType(this ColumnType type)
        {
            switch (type)
            {
                case ColumnType.NullableBinary:
                case ColumnType.Binary: return typeof(byte[]);

                case ColumnType.Bool: return typeof(bool);

                case ColumnType.Category: return typeof(Enum);

                case ColumnType.Date: return typeof(DateTime);
                case ColumnType.Double: return typeof(double);
                case ColumnType.Float: return typeof(float);
                case ColumnType.Int16: return typeof(short);
                case ColumnType.Int32: return typeof(int);
                case ColumnType.Int64: return typeof(long);
                case ColumnType.Int8: return typeof(sbyte);

                case ColumnType.Uint16: return typeof(ushort);
                case ColumnType.Uint32: return typeof(uint);
                case ColumnType.Uint64: return typeof(ulong);
                case ColumnType.Uint8: return typeof(byte);

                case ColumnType.Timestamp_Microsecond:
                case ColumnType.Timestamp_Millisecond:
                case ColumnType.Timestamp_Nanosecond:
                case ColumnType.Timestamp_Second:
                    return typeof(DateTime);

                case ColumnType.NullableBool: return typeof(bool?);

                case ColumnType.NullableCategory: return typeof(Enum);

                case ColumnType.NullableDate: return typeof(DateTime?);
                case ColumnType.NullableDouble: return typeof(double?);
                case ColumnType.NullableFloat: return typeof(float?);
                case ColumnType.NullableInt16: return typeof(short?);
                case ColumnType.NullableInt32: return typeof(int?);
                case ColumnType.NullableInt64: return typeof(long?);
                case ColumnType.NullableInt8: return typeof(sbyte?);

                case ColumnType.NullableUint16: return typeof(ushort?);
                case ColumnType.NullableUint32: return typeof(uint?);
                case ColumnType.NullableUint64: return typeof(ulong?);
                case ColumnType.NullableUint8: return typeof(byte?);

                case ColumnType.String:
                case ColumnType.NullableString: return typeof(string);

                case ColumnType.NullableTimestamp_Microsecond:
                case ColumnType.NullableTimestamp_Millisecond:
                case ColumnType.NullableTimestamp_Nanosecond:
                case ColumnType.NullableTimestamp_Second:
                    return typeof(DateTime?);

                case ColumnType.Time_Microsecond:
                case ColumnType.Time_Millisecond:
                case ColumnType.Time_Nanosecond:
                case ColumnType.Time_Second:
                    return typeof(TimeSpan);

                case ColumnType.NullableTime_Microsecond:
                case ColumnType.NullableTime_Millisecond:
                case ColumnType.NullableTime_Nanosecond:
                case ColumnType.NullableTime_Second:
                    return typeof(TimeSpan?);

                default: throw new Exception($"Unexpected ColumnType {type}");

            }
        }

        internal static bool TryCategoriesMapToEnum(Type enumType, string[] categories, out CategoryEnumMapType mapType)
        {
            // Operating under the assumption that the
            //   the number of entries and categories
            //   is normally small so avoiding a
            //   HashSet alloc is the smart choice.
            // Could easily cache this result 
            var allNames = Enum.GetNames(enumType);

            if (allNames.Length == 0)
            {
                mapType = default(CategoryEnumMapType);
                return false;
            }

            if (allNames.Length < categories.Length)
            {
                mapType = default(CategoryEnumMapType);
                return false;
            }

            bool exactNameMatch = true;

            for (var i = 0; i < categories.Length; i++)
            {
                var category = categories[i];

                var found = false;
                for (var j = 0; j < allNames.Length; j++)
                {
                    var enumName = allNames[j];
                    if (enumName.Equals(category, StringComparison.InvariantCultureIgnoreCase))
                    {
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    exactNameMatch = false;
                }
            }

            if (exactNameMatch)
            {
                mapType = CategoryEnumMapType.ByName;
                return true;
            }

            var enumIsByte = Enum.GetUnderlyingType(enumType) == typeof(byte);
            var enumIsSbyte = Enum.GetUnderlyingType(enumType) == typeof(sbyte);
            var enumIsShort = Enum.GetUnderlyingType(enumType) == typeof(short);
            var enumIsUshort = Enum.GetUnderlyingType(enumType) == typeof(ushort);
            var enumIsInt = Enum.GetUnderlyingType(enumType) == typeof(int);
            var enumIsUint = Enum.GetUnderlyingType(enumType) == typeof(uint);
            var enumIsLong = Enum.GetUnderlyingType(enumType) == typeof(long);
            var enumIsUlong = Enum.GetUnderlyingType(enumType) == typeof(ulong);

            bool ordinalMatch = true;
            for (var i = 0; i < categories.Length; i++)
            {
                var ordinal = i;
                if (enumIsByte)
                {
                    if (!Enum.IsDefined(enumType, (byte)i))
                    {
                        ordinalMatch = false;
                        break;
                    }
                    continue;
                }

                if (enumIsSbyte)
                {
                    if (!Enum.IsDefined(enumType, (sbyte)i))
                    {
                        ordinalMatch = false;
                        break;
                    }
                    continue;
                }

                if (enumIsShort)
                {
                    if (!Enum.IsDefined(enumType, (short)i))
                    {
                        ordinalMatch = false;
                        break;
                    }
                    continue;
                }

                if (enumIsUshort)
                {
                    if (!Enum.IsDefined(enumType, (ushort)i))
                    {
                        ordinalMatch = false;
                        break;
                    }
                    continue;
                }

                if (enumIsInt)
                {
                    if (!Enum.IsDefined(enumType, (int)i))
                    {
                        ordinalMatch = false;
                        break;
                    }
                    continue;
                }

                if (enumIsUint)
                {
                    if (!Enum.IsDefined(enumType, (uint)i))
                    {
                        ordinalMatch = false;
                        break;
                    }
                    continue;
                }

                if (enumIsLong)
                {
                    if (!Enum.IsDefined(enumType, (long)i))
                    {
                        ordinalMatch = false;
                        break;
                    }
                    continue;
                }

                if (enumIsUlong)
                {
                    if (!Enum.IsDefined(enumType, (ulong)i))
                    {
                        ordinalMatch = false;
                        break;
                    }
                    continue;
                }

                throw new Exception($"Couldn't make sense of enum type {enumType.Name} to map it");
            }

            mapType = CategoryEnumMapType.ByOrdinal;
            return ordinalMatch;
        }
    }

    struct ColumnSpec
    {
        public Dictionary<Type, CategoryEnumMapType> CategoryEnumMap { get; set; }

        public string Name { get; set; }
        public long Length { get; set; }
        public ColumnType Type { get; set; }
        public Type MappedType => Type.GetMapType();

        public long NullBitmaskOffset { get; set; }
        public long DataOffset { get; set; }

        public string[] CategoryLevels { get; set; }

        public bool CanMapTo(Type type) => Type.CanMapTo(type, CategoryLevels);

        public CategoryEnumMapType GetCategoryEnumMap<TEnumType>()
        {
            if (CategoryEnumMap == null) return CategoryEnumMapType.NONE;

            var enumType = typeof(TEnumType);
            enumType = Nullable.GetUnderlyingType(enumType) ?? enumType;

            if (!enumType.IsEnum) return CategoryEnumMapType.NONE;

            lock (CategoryEnumMap)
            {
                CategoryEnumMapType ret;
                if (CategoryEnumMap.TryGetValue(enumType, out ret)) return ret;

                if (!ColumnTypeExtensionMethods.TryCategoriesMapToEnum(enumType, CategoryLevels, out ret))
                {
                    CategoryEnumMap[enumType] = ret;
                }

                return ret;
            }
        }
    }

    static class DateTimePrecisionTypeExtensionMethods
    {
        public static feather.fbs.TimeUnit MapToDiskType(this DateTimePrecisionType type)
        {
            switch (type)
            {
                case DateTimePrecisionType.Microsecond: return feather.fbs.TimeUnit.MICROSECOND;
                case DateTimePrecisionType.Millisecond: return feather.fbs.TimeUnit.MILLISECOND;
                case DateTimePrecisionType.Nanosecond: return feather.fbs.TimeUnit.NANOSECOND;
                case DateTimePrecisionType.Second: return feather.fbs.TimeUnit.SECOND;
                default: throw new InvalidOperationException($"Unexpected {nameof(DateTimePrecisionType)}: {type}");
            }
        }
    }
}
