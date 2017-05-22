using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet.Impl
{
    struct ColumnMetadata
    {
        public string Name { get; set; }

        // values
        public ColumnType Type { get; set; }
        public feather.fbs.Encoding Encoding { get; set; }
        public long Offset { get; set; }
        public long Length { get; set; }
        public long NullCount { get; set; }
        public long TotalBytes { get; set; }

        // metadata
        public string[] Levels { get; set; }                // set in Category
        public bool Ordered { get; set; }                   // set in Category
        public DateTimePrecisionType Unit { get; set; }     // set in Time and Timestamp
        public string TimeZone { get; set; }                // set in Timestamp

        // not implemented: user_metadata (see: https://github.com/wesm/feather/blob/master/cpp/src/feather/metadata.fbs#L106)

        public void CreateMetadata(FlatBuffers.FlatBufferBuilder builder, FeatherWriter writer, out int metadataOffset, out feather.fbs.TypeMetadata metadata, out FlatBuffers.Offset<feather.fbs.PrimitiveArray> categoryLevels)
        {
            var isCategoryType =
                Type == ColumnType.Category ||
                Type == ColumnType.NullableCategory;

            var isDateType =
                Type == ColumnType.Date ||
                Type == ColumnType.NullableDate;

            var isTimeType =
                Type == ColumnType.Time_Microsecond ||
                Type == ColumnType.Time_Millisecond ||
                Type == ColumnType.Time_Second ||
                Type == ColumnType.Time_Nanosecond ||
                Type == ColumnType.NullableTime_Microsecond ||
                Type == ColumnType.NullableTime_Millisecond ||
                Type == ColumnType.NullableTime_Second ||
                Type == ColumnType.NullableTime_Nanosecond;

            var isTimestampType =
                Type == ColumnType.Timestamp_Microsecond ||
                Type == ColumnType.Timestamp_Millisecond ||
                Type == ColumnType.Timestamp_Second ||
                Type == ColumnType.Timestamp_Nanosecond ||
                Type == ColumnType.NullableTimestamp_Microsecond ||
                Type == ColumnType.NullableTimestamp_Millisecond ||
                Type == ColumnType.NullableTimestamp_Second ||
                Type == ColumnType.NullableTimestamp_Nanosecond;

            if (isDateType)
            {
                throw new InvalidOperationException($"Mapping to a Date on disk doesn't make sense from .NET");
            }

            if (isTimestampType)
            {
                var offsetMeta =
                    feather.fbs.TimestampMetadata.CreateTimestampMetadata(
                        builder,
                        Unit.MapToDiskType(),
                        builder.CreateString("GMT")
                    );
                metadataOffset = offsetMeta.Value;
                metadata = feather.fbs.TypeMetadata.TimestampMetadata;
                categoryLevels = default(FlatBuffers.Offset<feather.fbs.PrimitiveArray>);
                return;
            }

            if (isTimeType)
            {
                var offsetMeta =
                    feather.fbs.TimeMetadata.CreateTimeMetadata(
                        builder,
                        Unit.MapToDiskType()
                    );
                metadataOffset = offsetMeta.Value;
                metadata = feather.fbs.TypeMetadata.TimeMetadata;
                categoryLevels = default(FlatBuffers.Offset<feather.fbs.PrimitiveArray>);
                return;
            }

            if (isCategoryType)
            {
                long startIx;
                long numBytes;
                writer.WriteLevels(Levels, out startIx, out numBytes);

                categoryLevels =
                    feather.fbs.PrimitiveArray.CreatePrimitiveArray(
                        builder,
                        feather.fbs.Type.UTF8,
                        feather.fbs.Encoding.PLAIN,
                        startIx,
                        Levels.LongLength,
                        0,
                        numBytes
                    );

                var offsetMeta =
                    feather.fbs.CategoryMetadata.CreateCategoryMetadata(
                        builder,
                        categoryLevels,
                        Ordered
                    );

                metadataOffset = offsetMeta.Value;
                metadata = feather.fbs.TypeMetadata.CategoryMetadata;
                return;
            }

            metadataOffset = 0;
            metadata = feather.fbs.TypeMetadata.NONE;
            categoryLevels = default(FlatBuffers.Offset<feather.fbs.PrimitiveArray>);
        }
    }
}
