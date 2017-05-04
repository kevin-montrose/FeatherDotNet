using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet
{
    /// <summary>
    /// Utility class for addressing a dataframes columns.
    /// </summary>
    public struct ColumnMap
    {
        DataFrame Parent;

        /// <summary>
        /// Number of columns in the dataframe
        /// </summary>
        public long Count => Parent.ColumnCount;

        /// <summary>
        /// Returns the column at the given index (in the dataframe's basis).
        /// 
        /// Throws if the index is out of range.
        /// </summary>
        public Column this[long index]
        {
            get
            {
                var translatedIndex = Parent.TranslateIndex(index);

                if (translatedIndex < 0 || translatedIndex >= Parent.Metadata.Columns.Length)
                {
                    long minLegal;
                    long maxLegal;
                    switch (Parent.Basis)
                    {
                        case BasisType.One:
                            minLegal = 1;
                            maxLegal = Parent.Metadata.Columns.Length;
                            break;
                        case BasisType.Zero:
                            minLegal = 0;
                            maxLegal = Parent.Metadata.Columns.Length - 1;
                            break;
                        default: throw new InvalidOperationException($"Unexpected Basis: {Parent.Basis}");
                    }

                    throw new ArgumentOutOfRangeException(nameof(index), $"Column index out of range, valid between [{minLegal}, {maxLegal}] found {index}");
                }

                return
                    new Column
                    {
                        Parent = Parent,
                        TranslatedColumnIndex = translatedIndex
                    };
            }
        }

        /// <summary>
        /// Returns the column with the given name.
        /// 
        /// Throws if no column has the given name.
        /// </summary>
        public Column this[string columnName]
        {
            get
            {
                long translatedIndex;
                if (!Parent.TryLookupTranslatedColumnIndex(columnName, out translatedIndex))
                {
                    throw new KeyNotFoundException($"Could not find column with name \"{columnName}\"");
                }

                return new Column(Parent, translatedIndex);
            }
        }

        internal ColumnMap(DataFrame parent)
        {
            Parent = parent;
        }
    }
}
