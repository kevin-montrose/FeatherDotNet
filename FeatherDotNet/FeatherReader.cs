using feather.fbs;
using FlatBuffers;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using FeatherDotNet.Impl;

namespace FeatherDotNet
{
    /// <summary>
    /// Utility class for reading Feather files.
    /// </summary>
    public static class FeatherReader
    {
        /// <summary>
        /// Create a dataframe from the given file, with the given basis.
        /// 
        /// Throws if the dataframe cannot be created.
        /// </summary>
        public static DataFrame ReadFromFile(string filePath, BasisType basis)
        {
            string errorMessage;
            DataFrame ret;
            if (!TryReadFromFile(filePath, basis, out ret, out errorMessage))
            {
                throw new InvalidOperationException(errorMessage);
            }

            return ret;
        }

        /// <summary>
        /// Create a dataframe from the given file, with the given basis.
        /// 
        /// Returns false if the dataframe cannot be created.
        /// </summary>
        public static bool TryReadFromFile(string filePath, BasisType basis, out DataFrame frame, out string errorMessage)
        {
            var fileInfo = new FileInfo(filePath);
            var fileSize = fileInfo.Length;

            MemoryMappedFile memoryMapped;
            try
            {
                memoryMapped = MemoryMappedFile.CreateFromFile(filePath);
            }
            catch (Exception e)
            {
                errorMessage = $"Encountered {e.GetType().Name} trying to open file \"{filePath}\": {e.Message}";
                frame = null;
                return false;
            }

            var ret = TryRead(memoryMapped, fileSize, basis, out frame, out errorMessage);
            if (!ret)
            {
                memoryMapped.Dispose();
            }

            return ret;
        }

        /// <summary>
        /// Create a dataframe from a memory mapped file with the given name, with the given basis.
        /// 
        /// Throws if the dataframe cannot be created.
        /// </summary>
        public static DataFrame ReadFromName(string name, long size, BasisType basis)
        {
            string errorMessage;
            DataFrame ret;
            if (!TryReadFromName(name, size, basis, out ret, out errorMessage))
            {
                throw new InvalidOperationException(errorMessage);
            }

            return ret;
        }

        /// <summary>
        /// Create a dataframe from a memory mapped file with the given name, with the given basis.
        /// 
        /// Returns false if the dataframe cannot be created.
        /// </summary>
        public static bool TryReadFromName(string name, long size, BasisType basis, out DataFrame frame, out string errorMessage)
        {
            MemoryMappedFile memoryMapped;
            try
            {
                memoryMapped = MemoryMappedFile.OpenExisting(name);
            }
            catch (Exception e)
            {
                errorMessage = $"Encountered {e.GetType().Name} trying to open name \"{name}\": {e.Message}";
                frame = null;
                return false;
            }

            var ret = TryRead(memoryMapped, size, basis, out frame, out errorMessage);
            if (!ret)
            {
                memoryMapped.Dispose();
            }

            return ret;
        }

        /// <summary>
        /// Create a dataframe from the bytes passed, with the given basis.
        /// 
        /// Throws if the dataframe cannot be created.
        /// </summary>
        public static DataFrame ReadFromBytes(byte[] bytes, BasisType basis)
        {
            string errorMessage;
            DataFrame ret;
            if (!TryReadFromBytes(bytes, basis, out ret, out errorMessage))
            {
                throw new InvalidOperationException(errorMessage);
            }

            return ret;
        }

        /// <summary>
        /// Create a dataframe from the give bytes, with the given basis.
        /// 
        /// Returns false if the dataframe cannot be created.
        /// </summary>
        public static bool TryReadFromBytes(byte[] bytes, BasisType basis, out DataFrame frame, out string errorMessage)
        {
            MemoryMappedFile memoryMapped;
            try
            {
                memoryMapped = MakeMemoryMappedProxy(bytes);
            }
            catch (Exception e)
            {
                errorMessage = $"Encoutered {e.GetType().Name} trying to create a memory mapped proxy for passed bytes: {e.Message}";
                frame = null;
                return false;
            }

            var ret = TryRead(memoryMapped, bytes.Length, basis, out frame, out errorMessage);
            if (!ret)
            {
                memoryMapped.Dispose();
            }

            return ret;
        }

        static MemoryMappedFile MakeMemoryMappedProxy(byte[] bytes)
        {
            var newFile = MemoryMappedFile.CreateNew(nameof(FeatherDotNet) + "." + nameof(MakeMemoryMappedProxy) + "." + Guid.NewGuid(), bytes.Length);
            try
            {
                using (var stream = newFile.CreateViewStream())
                {
                    stream.Write(bytes, 0, bytes.Length);
                }
            }
            catch
            {
                newFile?.Dispose();
                throw;
            }
            return newFile;
        }

        static bool TryRead(MemoryMappedFile file, long fileSize, BasisType basis, out DataFrame frame, out string errorMessage)
        {
            switch (basis)
            {
                case BasisType.One:
                case BasisType.Zero: break;

                default:
                    frame = null;
                    errorMessage = $"Unexpected BasisType {basis}";
                    return false;
            }

            Metadata metadata;
            if (!TryReadMetaData(file, fileSize, out metadata, out errorMessage))
            {
                frame = null;
                return false;
            }

            frame = new DataFrame(file, metadata, basis);

            errorMessage = null;
            return true;
        }
        
        static bool TryReadMetaData(MemoryMappedFile file, long size, out Metadata metadata, out string error)
        {
            if (size < FeatherMagic.MAGIC_HEADER_SIZE * 2)
            {
                metadata = default(Metadata);
                error = $"File too small ({size:N0} bytes) to be a valid feather file";
                return false;
            }

            using (var accessor = file.CreateViewAccessor())
            {
                var leadingHeader = accessor.ReadInt32(0);

                if (leadingHeader != FeatherMagic.MAGIC_HEADER)
                {
                    metadata = default(Metadata);
                    error = $"Magic header malformed";
                    return false;
                }

                var trailingHeader = accessor.ReadInt32(size - FeatherMagic.MAGIC_HEADER_SIZE);

                if (trailingHeader != FeatherMagic.MAGIC_HEADER)
                {
                    metadata = default(Metadata);
                    error = $"Magic footer malformed";
                    return false;
                }

                var metadataSize = accessor.ReadUInt32(size - FeatherMagic.MAGIC_HEADER_SIZE - sizeof(uint));

                var metadataStart = size - FeatherMagic.MAGIC_HEADER_SIZE - sizeof(uint) - metadataSize;
                if (metadataStart < FeatherMagic.MAGIC_HEADER_SIZE || metadataSize > int.MaxValue)
                {
                    metadata = default(Metadata);
                    error = $"Metadata size ({metadataSize:N0}) is invalid";
                    return false;
                }

                var metadataBytes = new byte[metadataSize];
                accessor.ReadArray(metadataStart, metadataBytes, 0, (int)metadataSize);
                
                // note: It'd be nice to not actually use flatbuffers for this,
                //   kind of a heavy (re)build dependency for reading, like, 4 
                //   things
                var metadataBuffer = new ByteBuffer(metadataBytes);
                var metadataCTable = CTable.GetRootAsCTable(metadataBuffer);

                if (metadataCTable.Version != FeatherMagic.FEATHER_VERSION)
                {
                    error = $"Unexpected version {metadataCTable.Version}, only {FeatherMagic.FEATHER_VERSION} is supported";
                    metadata = default(Metadata);
                    return false;
                }

                if (metadataCTable.ColumnsLength <= 0)
                {
                    error = $"Invalid number of columns: {metadataCTable.ColumnsLength:N0}";
                    metadata = default(Metadata);
                    return false;
                }

                var columnSpecs = new ColumnSpec[metadataCTable.ColumnsLength];
                for (var i = 0; i < columnSpecs.Length; i++)
                {
                    var metadataColumn = metadataCTable.Columns(i).Value;
                    var name = metadataColumn.Name;
                    var metadataType = metadataColumn.MetadataType;

                    string[] categoryLevels = null;
                    DateTimePrecisionType precision = default(DateTimePrecisionType);
                    
                    var arrayDetails = metadataColumn.Values.Value;
                    var effectiveType = arrayDetails.Type;
                    
                    switch (metadataType)
                    {
                        case TypeMetadata.CategoryMetadata:
                            if (!TryReadCategoryLevels(accessor, ref metadataColumn, out categoryLevels, out error))
                            {
                                metadata = default(Metadata);
                                return false;
                            }
                            break;

                        case TypeMetadata.TimestampMetadata:
                            if (arrayDetails.Type != feather.fbs.Type.INT64)
                            {
                                metadata = default(Metadata);
                                error = $"Column {name} has Timestamp metadata, but isn't backed by an Int64 array";
                                return false;
                            }

                            if (!TryReadTimestampPrecision(ref metadataColumn, out precision, out error))
                            {
                                metadata = default(Metadata);
                                return false;
                            }

                            // note: this type is spec'd (https://github.com/wesm/feather/blob/master/cpp/src/feather/metadata.fbs#L25), 
                            //  but it looks like R always writes it as an int64?
                            // Possibly a bug.
                            effectiveType = feather.fbs.Type.TIMESTAMP;
                            
                            break;

                        case TypeMetadata.TimeMetadata:
                            if (arrayDetails.Type != feather.fbs.Type.INT64)
                            {
                                metadata = default(Metadata);
                                error = $"Column {name} has Time metadata, but isn't backed by an Int64 array";
                                return false;
                            }

                            if (!TryReadTimePrecision(ref metadataColumn, out precision, out error))
                            {
                                metadata = default(Metadata);
                                return false;
                            }

                            // note: this type is spec'd (https://github.com/wesm/feather/blob/master/cpp/src/feather/metadata.fbs#L27), 
                            //  but it looks like R always writes it as an int64?
                            // Possibly a bug.
                            effectiveType = feather.fbs.Type.TIME;

                            break;

                        case TypeMetadata.DateMetadata:
                            if (arrayDetails.Type != feather.fbs.Type.INT32)
                            {
                                metadata = default(Metadata);
                                error = $"Column {name} has Time metadata, but isn't backed by an Int32 array";
                                return false;
                            }

                            // note: this type is spec'd (https://github.com/wesm/feather/blob/master/cpp/src/feather/metadata.fbs#L26), 
                            //  but it looks like R always writes it as an int32?
                            // Possibly a bug.
                            effectiveType = feather.fbs.Type.DATE;

                            break;

                        case TypeMetadata.NONE: break;
                    }

                    ColumnSpec column;
                    if (!TryMakeColumnSpec(name, effectiveType, ref arrayDetails, categoryLevels, precision, out column, out error))
                    {
                        metadata = default(Metadata);
                        return false;
                    }

                    columnSpecs[i] = column;
                }

                metadata =
                    new Metadata
                    {
                        Columns = columnSpecs,
                        NumRows = metadataCTable.NumRows
                    };
                error = null;
                return true;
            }
        }

        static bool TryGetType(feather.fbs.Type effectiveType,ref PrimitiveArray array, DateTimePrecisionType precision, out ColumnType type, out bool isNullable, out string errorMessage)
        {
            isNullable = array.NullCount != 0;

            if (!isNullable)
            {
                switch (effectiveType)
                {
                    case feather.fbs.Type.BINARY:
                        type = ColumnType.Binary;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.BOOL:
                        type = ColumnType.Bool;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.CATEGORY:
                        type = ColumnType.Category;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.DATE:
                        type = ColumnType.Date;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.DOUBLE:
                        type = ColumnType.Double;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.FLOAT:
                        type = ColumnType.Float;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.INT16:
                        type = ColumnType.Int16;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.INT32:
                        type = ColumnType.Int32;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.INT64:
                        type = ColumnType.Int64;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.INT8:
                        type = ColumnType.Int8;
                        errorMessage = null;
                        return true;

                    
                    case feather.fbs.Type.TIMESTAMP:
                        switch (precision)
                        {
                            case DateTimePrecisionType.Microsecond:
                                type = ColumnType.Timestamp_Microsecond;
                                errorMessage = null;
                                return true;
                            case DateTimePrecisionType.Millisecond:
                                type = ColumnType.Timestamp_Millisecond;
                                errorMessage = null;
                                return true;
                            case DateTimePrecisionType.Nanosecond:
                                type = ColumnType.Timestamp_Nanosecond;
                                errorMessage = null;
                                return true;
                            case DateTimePrecisionType.Second:
                                type = ColumnType.Timestamp_Second;
                                errorMessage = null;
                                return true;
                            default:
                                errorMessage = $"Unknown precision {precision}";
                                type = ColumnType.NONE;
                                return false;
                        }

                    case feather.fbs.Type.TIME:
                        switch (precision)
                        {
                            case DateTimePrecisionType.Microsecond:
                                type = ColumnType.Time_Microsecond;
                                errorMessage = null;
                                return true;
                            case DateTimePrecisionType.Millisecond:
                                type = ColumnType.Time_Millisecond;
                                errorMessage = null;
                                return true;
                            case DateTimePrecisionType.Nanosecond:
                                type = ColumnType.Time_Nanosecond;
                                errorMessage = null;
                                return true;
                            case DateTimePrecisionType.Second:
                                type = ColumnType.Time_Second;
                                errorMessage = null;
                                return true;
                            default:
                                errorMessage = $"Unknown precision {precision}";
                                type = ColumnType.NONE;
                                return false;
                        }

                    case feather.fbs.Type.UINT16:
                        type = ColumnType.Uint16;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.UINT32:
                        type = ColumnType.Uint32;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.UINT64:
                        type = ColumnType.Uint64;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.UINT8:
                        type = ColumnType.Uint8;
                        errorMessage = null;
                        return true;

                    case feather.fbs.Type.UTF8:
                        type = ColumnType.String;
                        errorMessage = null;
                        return true;

                    default:
                        errorMessage = $"Unknown column type {array.Type}";
                        type = ColumnType.NONE;
                        return false;
                }
            }
            else
            {
                switch (effectiveType)
                {
                    case feather.fbs.Type.BINARY:
                        type = ColumnType.NullableBinary;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.BOOL:
                        type = ColumnType.NullableBool;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.CATEGORY:
                        type = ColumnType.NullableCategory;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.DATE:
                        type = ColumnType.NullableDate;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.DOUBLE:
                        type = ColumnType.NullableDouble;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.FLOAT:
                        type = ColumnType.NullableFloat;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.INT16:
                        type = ColumnType.NullableInt16;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.INT32:
                        type = ColumnType.NullableInt32;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.INT64:
                        type = ColumnType.NullableInt64;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.INT8:
                        type = ColumnType.NullableInt8;
                        errorMessage = null;
                        return true;

                    case feather.fbs.Type.TIMESTAMP:
                        switch (precision)
                        {
                            case DateTimePrecisionType.Microsecond:
                                type = ColumnType.NullableTimestamp_Microsecond;
                                errorMessage = null;
                                return true;
                            case DateTimePrecisionType.Millisecond:
                                type = ColumnType.NullableTimestamp_Millisecond;
                                errorMessage = null;
                                return true;
                            case DateTimePrecisionType.Nanosecond:
                                type = ColumnType.NullableTimestamp_Nanosecond;
                                errorMessage = null;
                                return true;
                            case DateTimePrecisionType.Second:
                                type = ColumnType.NullableTimestamp_Second;
                                errorMessage = null;
                                return true;
                            default:
                                errorMessage = $"Unknown precision {precision}";
                                type = ColumnType.NONE;
                                return false;
                        }

                    case feather.fbs.Type.TIME:
                        switch (precision)
                        {
                            case DateTimePrecisionType.Microsecond:
                                type = ColumnType.NullableTime_Microsecond;
                                errorMessage = null;
                                return true;
                            case DateTimePrecisionType.Millisecond:
                                type = ColumnType.NullableTime_Millisecond;
                                errorMessage = null;
                                return true;
                            case DateTimePrecisionType.Nanosecond:
                                type = ColumnType.NullableTime_Nanosecond;
                                errorMessage = null;
                                return true;
                            case DateTimePrecisionType.Second:
                                type = ColumnType.NullableTime_Second;
                                errorMessage = null;
                                return true;
                            default:
                                errorMessage = $"Unknown precision {precision}";
                                type = ColumnType.NONE;
                                return false;
                        }

                    case feather.fbs.Type.UINT16:
                        type = ColumnType.NullableUint16;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.UINT32:
                        type = ColumnType.NullableUint32;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.UINT64:
                        type = ColumnType.NullableUint64;
                        errorMessage = null;
                        return true;
                    case feather.fbs.Type.UINT8:
                        type = ColumnType.NullableUint8;
                        errorMessage = null;
                        return true;

                    case feather.fbs.Type.UTF8:
                        type = ColumnType.NullableString;
                        errorMessage = null;
                        return true;

                    default:
                        errorMessage = $"Unknown column type {array.Type}";
                        type = ColumnType.NONE;
                        return false;
                }
            }
        }

        static bool TryReadCategoryLevels(MemoryMappedViewAccessor view, ref feather.fbs.Column column, out string[] categoryLevels, out string errorMessage)
        {
            var metadata = column.Metadata<CategoryMetadata>();
            if (metadata == null)
            {
                categoryLevels = null;
                errorMessage = $"Couldn't read Category levels for column {column.Name}, no CategoryMetadata";
                return false;
            }

            var metadataValue = metadata.Value;
            if (metadataValue.Levels == null)
            {
                categoryLevels = null;
                errorMessage = $"Couldn't read Category levels for column {column.Name}, no Levels";
                return false;
            }

            var levels = metadataValue.Levels.Value;
            if (levels.Type != feather.fbs.Type.UTF8)
            {
                categoryLevels = null;
                errorMessage = $"Found non-string ({levels.Type}) for column {column.Name}";
                return false;
            }

            ColumnSpec categoryColumn;
            if (!TryMakeColumnSpec(nameof(TryReadCategoryLevels) + "__FAKE__", levels.Type, ref levels, null, default(DateTimePrecisionType), out categoryColumn, out errorMessage))
            {
                categoryLevels = null;
                return false;
            }

            categoryLevels = new string[levels.Length];

            for (var i = 0; i < levels.Length; i++)
            {
                var categoryName = DataFrame.ReadString(view, levels.Length, null, ref categoryColumn, i);
                categoryLevels[i] = categoryName;
            }

            errorMessage = null;
            return true;
        }

        static bool TryReadTimestampPrecision(ref feather.fbs.Column column, out DateTimePrecisionType precision, out string errorMessage)
        {
            var metadata = column.Metadata<TimestampMetadata>();
            if(metadata == null)
            {
                precision = default(DateTimePrecisionType);
                errorMessage = $"Couldn't read Precision for column {column.Name}, no TimestampMetadada";
                return false;
            }

            var metadataValue = metadata.Value;
            var timezoneName = metadataValue.Timezone;
            var unit = metadataValue.Unit;

            // note: supporting other timezones would be nice,
            //   but the timezone included appears to just be 
            //   a passthrough from as.POSIXct (in R anyway)
            //   which lets a bunch of ambiguous junk through
            //   (timezone abbreviations are NOT unique, or
            //   properly spec'd even).
            // the only thing we can rely on being the same
            //   everywhere is UTC, so constrain to that.
            var isntUtc =
                timezoneName != null &&
                !timezoneName.Equals("UTC", StringComparison.InvariantCultureIgnoreCase) &&
                !timezoneName.Equals("GMT", StringComparison.InvariantCultureIgnoreCase);

            if (isntUtc)
            {
                errorMessage = $"Cannot read Timestamp in timezone {timezoneName}, only UTC/GMT is supported";
                precision = default(DateTimePrecisionType);
                return false;
            }

            switch (unit)
            {
                case TimeUnit.MICROSECOND: precision = DateTimePrecisionType.Microsecond; break;
                case TimeUnit.MILLISECOND: precision = DateTimePrecisionType.Millisecond; break;
                case TimeUnit.NANOSECOND: precision = DateTimePrecisionType.Nanosecond; break;
                case TimeUnit.SECOND: precision = DateTimePrecisionType.Second; break;
                default:
                    precision = default(DateTimePrecisionType);
                    errorMessage = $"Couldn't understand TimeUnit {unit}";
                    return false;
            }

            errorMessage = null;
            return true;
        }

        static bool TryReadTimePrecision(ref feather.fbs.Column column, out DateTimePrecisionType precision, out string errorMessage)
        {
            var metadata = column.Metadata<TimeMetadata>();
            if(metadata == null)
            {
                precision = default(DateTimePrecisionType);
                errorMessage = $"Couldn't read Precision for column {column.Name}, no TimeMetadata";
                return false;
            }

            var metadataValue = metadata.Value;
            var unit = metadataValue.Unit;

            switch (unit)
            {
                case TimeUnit.MICROSECOND: precision = DateTimePrecisionType.Microsecond; break;
                case TimeUnit.MILLISECOND: precision = DateTimePrecisionType.Millisecond; break;
                case TimeUnit.NANOSECOND: precision = DateTimePrecisionType.Nanosecond; break;
                case TimeUnit.SECOND: precision = DateTimePrecisionType.Second; break;
                default:
                    precision = default(DateTimePrecisionType);
                    errorMessage = $"Couldn't understand TimeUnit {unit}";
                    return false;
            }

            errorMessage = null;
            return true;
        }

        static bool TryMakeColumnSpec(string name, feather.fbs.Type effectiveType, ref PrimitiveArray arrayDetails, string[] categoryLevels, DateTimePrecisionType precision, out ColumnSpec columnSpec, out string errorMessage)
        {
            var arrayOffset = arrayDetails.Offset;
            var arrayLength = arrayDetails.Length;
            var arrayNulls = arrayDetails.NullCount;
            var arrayEncoding = arrayDetails.Encoding;

            // TODO (Dictionary Encoding)
            if (arrayEncoding != feather.fbs.Encoding.PLAIN)
            {
                throw new NotImplementedException();
            }
            // END TODO

            ColumnType type;
            bool isNullable;
            if (!TryGetType(effectiveType, ref arrayDetails, precision, out type, out isNullable, out errorMessage))
            {
                columnSpec = default(ColumnSpec);
                return false;
            }

            long numNullBytes = 0;
            if (isNullable)
            {
                numNullBytes = arrayLength / 8;
                if (arrayLength % 8 != 0)
                {
                    numNullBytes++;
                }
            }
            
            // a naive reading of the spec suggests that the null bitmask should be
            //   aligned based on the _type_ but it appears to always be long
            //   aligned.
            // this may be a bug in the spec
            int nullPadding = 0;
            if ((numNullBytes % FeatherMagic.NULL_BITMASK_ALIGNMENT) != 0)
            {
                nullPadding = FeatherMagic.NULL_BITMASK_ALIGNMENT - (int)(numNullBytes % FeatherMagic.NULL_BITMASK_ALIGNMENT);
            }
            var nullOffset = isNullable ? (arrayOffset) : -1;
            var dataOffset = !isNullable ? (arrayOffset) : (nullOffset + numNullBytes + nullPadding);

            columnSpec =
                new ColumnSpec
                {
                    Name = name,
                    NullBitmaskOffset = nullOffset,
                    DataOffset = dataOffset,
                    Length = arrayLength,
                    Type = type,
                    CategoryLevels = categoryLevels,

                    // only spin up this map if we've got categories to potentially map to
                    CategoryEnumMap = categoryLevels != null ? new Dictionary<System.Type, CategoryEnumMapType>() : null
                };
            errorMessage = null;
            return true;
        }
    }
}