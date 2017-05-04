using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet
{
    /// <summary>
    /// Utility class for addressing a dataframe's rows.
    /// </summary>
    public struct RowMap
    {
        DataFrame Parent;

        /// <summary>
        /// Number of rows in the dataframe
        /// </summary>
        public long Count => Parent.RowCount;

        /// <summary>
        /// Returns the row at the given index (in the dataframe's basis).
        /// 
        /// Throws if the index is out of range.
        /// </summary>
        public Row this[long index]
        {
            get
            {
                var translatedIndex = Parent.TranslateIndex(index);

                if (translatedIndex < 0 || translatedIndex >= Parent.Metadata.NumRows)
                {
                    long minLegal;
                    long maxLegal;
                    switch (Parent.Basis)
                    {
                        case BasisType.One:
                            minLegal = 1;
                            maxLegal = Parent.Metadata.NumRows;
                            break;
                        case BasisType.Zero:
                            minLegal = 0;
                            maxLegal = Parent.Metadata.NumRows - 1;
                            break;
                        default: throw new InvalidOperationException($"Unexpected Basis: {Parent.Basis}");
                    }

                    throw new ArgumentOutOfRangeException(nameof(index), $"Row index out of range, valid between [{minLegal}, {maxLegal}] found {index}");
                }

                return new Row(Parent, translatedIndex);
            }
        }

        internal RowMap(DataFrame parent)
        {
            Parent = parent;
        }
    }
}
