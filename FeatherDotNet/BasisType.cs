using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet
{
    /// <summary>
    /// Represents whether a dataframe is using 0-based or 1-based indexing.
    /// </summary>
    public enum BasisType
    {
        /// <summary>
        /// 1-based indexing - intended for ports from 1-based languages
        /// </summary>
        One = 1,
        /// <summary>
        /// 0-based indexing - the C# standard
        /// </summary>
        Zero = 2
    }
}
