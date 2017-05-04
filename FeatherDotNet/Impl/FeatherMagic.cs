using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet.Impl
{
    static class FeatherMagic
    {
        public const int ARROW_ALIGNMENT = 8;  // 64-bits per https://arrow.apache.org/docs/memory_layout.html
        public const int FEATHER_VERSION = 2;
        public const int NULL_BITMASK_ALIGNMENT = 8;
        public const int MAGIC_HEADER_SIZE = 4;
        public const int MAGIC_HEADER = ((byte)'F' << (8 * 0)) | ((byte)'E') << (8 * 1) | ((byte)'A' << (8 * 2)) | ((byte)'1' << (8 * 3)); // 'FEA1', little endian

        public static readonly DateTime DATETIME_EPOCH = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    }
}
