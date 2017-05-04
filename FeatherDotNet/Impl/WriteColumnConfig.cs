using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet.Impl
{
    struct WriteColumnConfig
    {
        public string Name { get; set; }
        public Type DotNetType { get; set; }
        public ColumnType OnDiskType { get; set; }
        public long Length { get; set; }
        public System.Collections.IEnumerable Data { get; set; }
        public long NullCount { get; set; }

        public WriteColumnConfig(string name, Type dotNetType, ColumnType onDiskType, long length, System.Collections.IEnumerable data, long nullCount)
        {
            Name = name;
            DotNetType = dotNetType;
            OnDiskType = onDiskType;
            Length = length;
            Data = data;
            NullCount = nullCount;
        }
    }
}
