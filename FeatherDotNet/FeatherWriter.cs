using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FeatherDotNet.Impl;

namespace FeatherDotNet
{
    /// <summary>
    /// Indicates how a FeatherWriter should schedule writing to disk.
    /// </summary>
    public enum WriteMode
    {
        /// <summary>
        /// Writes are queued, but not actually executed until the writer is being discarded.
        /// </summary>
        Lazy = 1,
        /// <summary>
        /// Writes occur immediately, and references to any parameters to the writer are discarded where possible.
        /// </summary>
        Eager = 2
    }

    /// <summary>
    /// Class for writing dataframes (or equivalently, sets of columns) to a file in the Feather format.
    /// 
    /// Supports writing lazily (the default) or eagerly.
    /// 
    /// Eager writes are meant for the cases where individual inputs consume considerable resources, and
    /// it is undesirable for a FeatherWriter to hold references to inputs.  An example would be a case
    /// where there are many billions of rows in a dataframe, and the GC being able to reclaim whole columns
    /// during dataframe persisting is necessary for adequate performance.
    /// </summary>
    public sealed class FeatherWriter :
        IDisposable
    {
        long NullIndex;
        long DataIndex;
        long VariableIndex;

        BinaryWriter NullStream;
        BinaryWriter DataStream;
        BinaryWriter VariableStream;

        LinkedList<WriteColumnConfig> PendingColumns;
        LinkedList<ColumnMetadata> Metadata;

        /// <summary>
        /// WriteMode this FeatherWriter is configured in.
        /// </summary>
        public WriteMode Mode { get; private set; }

        /// <summary>
        /// Number of rows in the dataframe being written
        /// </summary>
        public long NumRows { get; private set; }
        /// <summary>
        /// Number of columns added to this dataframe
        /// </summary>
        public int NumColumns => Metadata.Count;

        /// <summary>
        /// Create a new FeatherWriter that will persist to the given file.
        /// Writes are performed lazily.
        /// 
        /// Throws if not able to create the file.
        /// </summary>
        public FeatherWriter(string filePath) : this(filePath, WriteMode.Lazy) { }

        /// <summary>
        /// Create a new FeatherWriter that will persist to the given file.
        /// 
        /// Throws if not able to create the file.
        /// </summary>
        public FeatherWriter(string filePath, WriteMode mode)
        {
            if (filePath == null) throw new ArgumentNullException(nameof(filePath));

            switch (mode)
            {
                case WriteMode.Eager:
                case WriteMode.Lazy:
                    break;

                default: throw new ArgumentException($"Unexpected Write Mode {mode}", nameof(mode));
            }

            try
            {
                DataStream = new BinaryWriter(File.Open(filePath, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite));
                NullStream = new BinaryWriter(File.Open(filePath, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite));
                VariableStream = new BinaryWriter(File.Open(filePath, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite));

                Setup(mode);
            }
            catch
            {
                DataStream?.Dispose();
                NullStream?.Dispose();
                VariableStream?.Dispose();
                DataStream = NullStream = VariableStream = null;

                throw;
            }
        }

        /// <summary>
        /// Create a new FeatherWriter that will write to the given stream.
        /// </summary>
        public FeatherWriter(Stream outputStream, WriteMode mode)
        {
            if (outputStream == null) throw new ArgumentNullException(nameof(outputStream));
            switch (mode)
            {
                case WriteMode.Eager:
                case WriteMode.Lazy:
                    break;

                default: throw new ArgumentException($"Unexpected Write Mode {mode}", nameof(mode));
            }

            var multiplexer = new MultiStreamProvider(outputStream);

            try
            {
                DataStream = new BinaryWriter(multiplexer.CreateChildStream());
                NullStream = new BinaryWriter(multiplexer.CreateChildStream());
                VariableStream = new BinaryWriter(multiplexer.CreateChildStream());

                Setup(mode);
            }
            catch
            {
                DataStream?.Dispose();
                NullStream?.Dispose();
                VariableStream?.Dispose();
                DataStream = NullStream = VariableStream = null;

                throw;
            }
        }

        void Setup(WriteMode mode)
        {
            Mode = mode;
            DataIndex = 0;
            NullIndex = 0;
            VariableIndex = 0;
            PendingColumns = new LinkedList<WriteColumnConfig>();
            Metadata = new LinkedList<ColumnMetadata>();

        }

        /// <summary>
        /// Append a single column to the the dataframe.
        /// </summary>
        public void AddColumn<T>(string name, IEnumerable<T> column)
        {
            if (name == null) throw new ArgumentNullException(nameof(column));
            if (column == null) throw new ArgumentNullException(nameof(column));
            long length;
            long numNulls;
            var effectiveType = typeof(T);
            if (effectiveType == typeof(object))
            {
                // untyped requires inferencing
                DetermineTypeLengthAndNullCount(column, out effectiveType, out length, out numNulls);
            }
            else
            {
                if (IsNonNullable(effectiveType))
                {
                    numNulls = 0;
                    length = DetermineLength(column);
                }
                else
                {
                    DetermineLengthAndNullCount(column, out length, out numNulls);
                }
            }

            AddColumnImpl(name, effectiveType, column, length, numNulls);
        }

        /// <summary>
        /// Append a single column with a known length to the the dataframe.
        /// </summary>
        public void AddColumn<T>(string name, IEnumerable<T> column, long length)
        {
            if (name == null) throw new ArgumentNullException(nameof(column));
            if (column == null) throw new ArgumentNullException(nameof(column));
            if (length < 0) throw new ArgumentOutOfRangeException(nameof(length), $"Expected value >= 0, found {length}");

            long numNulls;
            var effectiveType = typeof(T);
            if (effectiveType == typeof(object))
            {
                DetermineTypeAndNullCount(column, out effectiveType, out numNulls);
            }
            else
            {
                if (IsNonNullable(effectiveType))
                {
                    numNulls = 0;
                }
                else
                {
                    numNulls = CountNulls(column);
                }
            }

            AddColumnImpl(name, effectiveType, column, length, numNulls);
        }

        /// <summary>
        /// Append a single column to the the dataframe.
        /// </summary>
        public void AddColumn(string name, System.Collections.IEnumerable column)
        {
            if (column == null) throw new ArgumentNullException(nameof(column));

            Type type;
            long length;
            long nullCount;
            DetermineTypeLengthAndNullCount(column, out type, out length, out nullCount);

            AddColumnImpl(name, type, column, length, nullCount);
        }

        /// <summary>
        /// Append a single column with a known length to the the dataframe.
        /// </summary>
        public void AddColumn(string name, System.Collections.IEnumerable column, long length)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (column == null) throw new ArgumentNullException(nameof(column));
            if (length < 0) throw new ArgumentOutOfRangeException(nameof(length), $"Expected value >= 0, found {length}");

            Type type;
            long nullCount;
            DetermineTypeAndNullCount(column, out type, out nullCount);

            AddColumnImpl(name, type, column, length, nullCount);
        }

        /// <summary>
        /// Append a set of columns to the dataframe.
        /// </summary>
        public void AddColumns(IEnumerable<string> names, IEnumerable<System.Collections.IEnumerable> columns)
        {
            if (names == null) throw new ArgumentNullException(nameof(names));
            if (columns == null) throw new ArgumentNullException(nameof(columns));

            using (var namesE = names.GetEnumerator())
            using (var columnE = columns.GetEnumerator())
            {
                while (true)
                {
                    var cMoveNext = columnE.MoveNext();
                    var nMoveNext = namesE.MoveNext();

                    if (!cMoveNext && !nMoveNext) return;

                    if (!cMoveNext)
                    {
                        throw new InvalidOperationException($"Columns enumerable ran out before names enumerable");
                    }

                    if (!nMoveNext)
                    {
                        throw new InvalidOperationException($"Names enumerable ran out before columns enumerable");
                    }

                    var name = namesE.Current;
                    var column = columnE.Current;

                    Type type;
                    long length;
                    long nullCount;
                    DetermineTypeLengthAndNullCount(column, out type, out length, out nullCount);

                    AddColumnImpl(name, type, column, length, nullCount);
                }
            }
        }

        /// <summary>
        /// Append a set of columns with known lengths to the dataframe.
        /// </summary>
        public void AddColumns(IEnumerable<string> names, IEnumerable<System.Collections.IEnumerable> columns, IEnumerable<long> lengths)
        {
            if (names == null) throw new ArgumentNullException(nameof(columns));
            if (columns == null) throw new ArgumentNullException(nameof(columns));
            if (lengths == null) throw new ArgumentNullException(nameof(columns));

            using (var namesE = names.GetEnumerator())
            using (var columnE = columns.GetEnumerator())
            using (var lengthE = lengths.GetEnumerator())
            {
                while (true)
                {
                    var nMoveNext = namesE.MoveNext();
                    var cMoveNext = columnE.MoveNext();
                    var lMoveNext = lengthE.MoveNext();

                    if (!nMoveNext && !cMoveNext && !lMoveNext) return;

                    if (!nMoveNext)
                    {
                        throw new InvalidOperationException($"Names enumerable ran out before columns and lengths enumerables");
                    }

                    if (!cMoveNext)
                    {
                        throw new InvalidOperationException($"Columns enumerable ran out before names and lengths enumerables");
                    }

                    if (!lMoveNext)
                    {
                        throw new InvalidOperationException($"Lengths enumerable ran out before columns and data enumerables");
                    }

                    var name = namesE.Current;
                    var column = columnE.Current;
                    var length = lengthE.Current;
                    Type type;
                    long nullCount;
                    DetermineTypeAndNullCount(column, out type, out nullCount);

                    AddColumnImpl(name, type, column, length, nullCount);
                }
            }
        }

        /// <summary>
        /// Append a set of columns to the dataframe.
        /// </summary>
        public void AddColumns<T>(IEnumerable<string> names, IEnumerable<IEnumerable<T>> columns)
        {
            if (names == null) throw new ArgumentNullException(nameof(names));
            if (columns == null) throw new ArgumentNullException(nameof(columns));

            var type = typeof(T);

            using (var namesE = names.GetEnumerator())
            using (var columnE = columns.GetEnumerator())
            {
                while (true)
                {
                    var nMoveNext = namesE.MoveNext();
                    var cMoveNext = columnE.MoveNext();

                    if (!nMoveNext && !cMoveNext) return;

                    if (!nMoveNext)
                    {
                        throw new InvalidOperationException($"Names enumerable ran out before columns and lengths enumerables");
                    }

                    if (!cMoveNext)
                    {
                        throw new InvalidOperationException($"Columns enumerable ran out before names and lengths enumerables");
                    }

                    var name = namesE.Current;
                    var column = columnE.Current;
                    long length;
                    long nullCount;
                    var effectiveType = type;

                    if (effectiveType == typeof(object))
                    {
                        // have to do this inferencing per-column
                        DetermineTypeLengthAndNullCount(column, out effectiveType, out length, out nullCount);
                    }
                    else
                    {
                        if (IsNonNullable(effectiveType))
                        {
                            length = DetermineLength(column);
                            nullCount = 0;
                        }
                        else
                        {
                            DetermineLengthAndNullCount(column, out length, out nullCount);
                        }
                    }

                    AddColumnImpl(name, effectiveType, column, length, nullCount);
                }
            }
        }

        /// <summary>
        /// Append a set of columns with known lengths to the dataframe.
        /// </summary>
        public void AddColumns<T>(IEnumerable<string> names, IEnumerable<IEnumerable<T>> columns, IEnumerable<long> lengths)
        {
            if (names == null) throw new ArgumentNullException(nameof(names));
            if (columns == null) throw new ArgumentNullException(nameof(columns));
            if (lengths == null) throw new ArgumentNullException(nameof(lengths));

            var type = typeof(T);

            using (var namesE = names.GetEnumerator())
            using (var columnE = columns.GetEnumerator())
            using (var lengthE = lengths.GetEnumerator())
            {
                while (true)
                {
                    var nMoveNext = namesE.MoveNext();
                    var cMoveNext = columnE.MoveNext();
                    var lMoveNext = lengthE.MoveNext();

                    if (!nMoveNext && !cMoveNext && !lMoveNext) return;

                    if (!nMoveNext)
                    {
                        throw new InvalidOperationException($"Names enumerable ran out before columns and lengths enumerables");
                    }

                    if (!cMoveNext)
                    {
                        throw new InvalidOperationException($"Columns enumerable ran out before names and lengths enumerables");
                    }

                    if (!lMoveNext)
                    {
                        throw new InvalidOperationException($"Lengths enumerable ran out before columns and data enumerables");
                    }

                    var name = namesE.Current;
                    var column = columnE.Current;
                    var length = lengthE.Current;
                    long nullCount;
                    var effectiveType = type;

                    if (effectiveType == typeof(object))
                    {
                        // have to do this inferencing per-column
                        DetermineTypeLengthAndNullCount(column, out effectiveType, out length, out nullCount);
                    }
                    else
                    {
                        if (IsNonNullable(effectiveType))
                        {
                            nullCount = 0;
                        }
                        else
                        {
                            nullCount = CountNulls(column);
                        }
                    }

                    AddColumnImpl(name, effectiveType, column, length, nullCount);
                }
            }
        }

        /// <summary>
        /// <see cref="IDisposable.Dispose"/>
        /// </summary>
        public void Dispose()
        {
            var dataCopy = DataStream;
            var nullCopy = NullStream;
            var variableCopy = VariableStream;

            if (dataCopy != null && nullCopy != null && variableCopy != null)
            {
                SerializeAndDiscard();
            }
        }

        static void DetermineTypeLengthAndNullCount(System.Collections.IEnumerable data, out Type type, out long length, out long numNulls)
        {
            var realType = data.GetType();
            if (realType.IsArray)
            {
                type = realType.GetElementType();

                if(type == typeof(object))
                {
                    // doesn't matter that we can short-circuit length,
                    //   we have to enumerate the whole thing for type
                    //   anyway
                    goto inferFromUntyped;
                }

                length = ((Array)data).LongLength;

                if (IsNonNullable(type))
                {
                    numNulls = 0;
                }
                else
                {
                    numNulls = CountNulls(data);
                }

                return;
            }

            // this should cover all the System.Collections.Generic types
            var realInterfaces = realType.GetInterfaces();
            var iCollection = realInterfaces.FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICollection<>));
            if (iCollection != null)
            {
                type = iCollection.GetGenericArguments()[0];

                if(type == typeof(object))
                {
                    // doesn't matter that we can short-circuit length,
                    //   we have to enumerate the whole thing for type
                    //   anyway
                    goto inferFromUntyped;
                }

                length = CollectionLengthLookup.GetLength(type, data);

                if (IsNonNullable(type))
                {
                    numNulls = 0;
                }
                else
                {
                    numNulls = CountNulls(data);
                }

                return;
            }

            // we have no clue, so inspect every element to figure out what's
            //   actually in the column
            inferFromUntyped:
            long nulls = 0;
            long count = 0;
            var observedTypes = new HashSet<Type>();
            foreach (var elem in data)
            {
                count++;

                if (elem == null)
                {
                    nulls++;
                    continue;
                }

                var elemType = elem.GetType();
                observedTypes.Add(elemType);
            }

            var widestType = DetermineCoveringType(observedTypes, nulls > 0);

            if (widestType == null) throw new InvalidOperationException($"Could not infer column");

            type = widestType;
            length = count;
            numNulls = nulls;
        }

        static Type DetermineCoveringType(IEnumerable<Type> seenTypes, bool mustBeNullable)
        {
            bool hasDateTime = false;
            bool hasTimeSpan = false;
            bool hasEnums = false;

            var mostToLeastPermissive =
                seenTypes
                    .OrderBy(
                        rawType =>
                        {
                            var t = Nullable.GetUnderlyingType(rawType) ?? rawType;

                            if (t == typeof(string)) return 0;
                            if (t == typeof(double)) return 1;
                            if (t == typeof(float)) return 2;
                            if (t == typeof(long)) return 3;
                            if (t == typeof(ulong)) return 4;
                            if (t == typeof(int)) return 5;
                            if (t == typeof(uint)) return 6;
                            if (t == typeof(short)) return 7;
                            if (t == typeof(ushort)) return 8;
                            if (t == typeof(byte)) return 9;
                            if (t == typeof(sbyte)) return 10;

                            if (t == typeof(DateTime))
                            {
                                hasDateTime = true;
                                return 11;
                            }
                            if (t == typeof(DateTimeOffset))
                            {
                                hasDateTime = true;
                                return 11;
                            }

                            if(t == typeof(TimeSpan))
                            {
                                hasTimeSpan = true;
                                return 12;
                            }

                            if (t.IsEnum)
                            {
                                hasEnums = true;
                                return 13;
                            }

                            throw new InvalidOperationException($"Unexpected type found when trying to determing covering type for untyped column {rawType.Name}");
                        }
                    )
                    .ToList();

            if (mostToLeastPermissive.Count == 0) return null;

            var widest = mostToLeastPermissive.First();

            if (hasEnums)
            {
                var allEnums = seenTypes.All(t => (Nullable.GetUnderlyingType(t) ?? t).IsEnum);
                if (allEnums)
                {
                    if(seenTypes.Count() == 1)
                    {
                        // ready made enum, go for it
                        var target = seenTypes.Single();
                        if (mustBeNullable)
                        {
                            target = AssureNullable(target);
                        }

                        return target;
                    }

                    // multiple enums, so let's merge them all together but maintain the category nature of it
                    var synthEnum = SyntheticEnum.Lookup(seenTypes.Select(t => (Nullable.GetUnderlyingType(t) ?? t)));

                    if (mustBeNullable)
                    {
                        synthEnum = AssureNullable(synthEnum);
                    }

                    return synthEnum;
                }

                // if there's an enum _AND ANYTHING ELSE_ it's gotta be a string column
                return typeof(string);
            }

            if (hasTimeSpan)
            {
                // it's OK to convert TimeSpan to a string
                if(widest == typeof(string))
                {
                    return typeof(string);
                }

                var otherWidest = Nullable.GetUnderlyingType(widest) ?? widest;
                if (otherWidest != typeof(TimeSpan))
                {
                    throw new InvalidOperationException($"Found a mix of types in addition to TimeSpan.  With time values, only TimeSpan and strings may appear in the column data.");
                }

                var target = typeof(TimeSpan);
                if (mustBeNullable)
                {
                    target = AssureNullable(target);
                }

                return target;
            }

            if (hasDateTime)
            {
                // it's OK to convert DateTime's to a string
                if (widest == typeof(string))
                {
                    return typeof(string);
                }

                var otherWidest = Nullable.GetUnderlyingType(widest) ?? widest;
                if (otherWidest != typeof(DateTime) && otherWidest != typeof(DateTimeOffset))
                {
                    throw new InvalidOperationException($"Found a mix of types in addition to DateTime-y types.  With date time values, only DateTime, DateTimeOffset, and string values may appear in the column data.");
                }

                // force it to DateTime, not bothering with a special adapter for DateTimeOffset
                var target = typeof(DateTime);
                if (mustBeNullable)
                {
                    target = AssureNullable(target);
                }

                return target;
            }

            var signed = 0;
            var unsigned = 0;
            
            foreach(var type in mostToLeastPermissive)
            {
                if(type == typeof(long) || type == typeof(int) || type == typeof(short) || type == typeof(sbyte))
                {
                    signed++;
                    continue;
                }

                if(type == typeof(ulong) || type == typeof(uint) || type == typeof(ushort) || type == typeof(byte))
                {
                    unsigned++;
                    continue;
                }
            }

            Type basicType;
            if(widest == typeof(string))
            {
                basicType = typeof(string);
            }
            else
            {
                if (IsFloating(widest))
                {
                    basicType = widest;
                }
                else
                {
                    // widest is integral now
                    if(signed > 0 && unsigned > 0){
                        basicType = typeof(double);     // expand
                    }
                    else
                    {
                        // all the same sign, so just take the biggest one
                        basicType = widest;
                    }
                }
            }

            var ret = mustBeNullable ? AssureNullable(basicType) : basicType;

            return ret;
        }

        static Type AssureNullable(Type t)
        {
            if (t == null) return null;

            if (!t.IsValueType) return t;
            if (Nullable.GetUnderlyingType(t) != null) return t;

            return typeof(Nullable<>).MakeGenericType(t);
        }

        static Type Widen(Type a, Type b)
        {
            // same type? keep it
            if (a == b) return a;

            // one is the Type? version of the other, take the Type?
            if (AssureNullable(a) == b) return b;
            if (AssureNullable(b) == a) return a;

            // one is an enum or enum?, the other is a string - take the string
            if (a.IsEnum && b == typeof(string)) return typeof(string);
            if (Nullable.GetUnderlyingType(a)?.IsEnum ?? false && b == typeof(string)) return typeof(string);
            if (b.IsEnum && a == typeof(string)) return typeof(string);
            if (Nullable.GetUnderlyingType(b)?.IsEnum ?? false && a == typeof(string)) return typeof(string);

            // they're both enums (but different ones by definition now) - so convert to string
            if (
                (a.IsEnum || (Nullable.GetUnderlyingType(a)?.IsEnum ?? false)) &&
                (b.IsEnum || (Nullable.GetUnderlyingType(b)?.IsEnum ?? false))
              ) return typeof(string);

            // DateTime widens to DateTimeOffset
            if (a == typeof(DateTime) && b == typeof(DateTimeOffset)) return typeof(DateTimeOffset);
            if (a == typeof(DateTime) && b == typeof(DateTimeOffset?)) return typeof(DateTimeOffset?);
            if (a == typeof(DateTime?) && b == typeof(DateTimeOffset)) return typeof(DateTimeOffset?);
            if (a == typeof(DateTime) && b == typeof(DateTimeOffset?)) return typeof(DateTimeOffset?);
            if (a == typeof(DateTime?) && b == typeof(DateTimeOffset?)) return typeof(DateTimeOffset?);
            if (b == typeof(DateTime) && a == typeof(DateTimeOffset)) return typeof(DateTimeOffset);
            if (b == typeof(DateTime) && a == typeof(DateTimeOffset?)) return typeof(DateTimeOffset?);
            if (b == typeof(DateTime?) && a == typeof(DateTimeOffset)) return typeof(DateTimeOffset?);
            if (b == typeof(DateTime) && a == typeof(DateTimeOffset?)) return typeof(DateTimeOffset?);
            if (b == typeof(DateTime?) && a == typeof(DateTimeOffset?)) return typeof(DateTimeOffset?);

            // all that's left is number types now
            var aIsIntegral = IsIntegral(a);
            var aIsFloating = IsFloating(a);
            var bIsIntegral = IsIntegral(b);
            var bIsFloating = IsFloating(b);

            if (!aIsIntegral && !aIsFloating) throw new InvalidOperationException($"Cannot map type {a.Name} to a Feather column type");
            if (!bIsIntegral && !bIsFloating) throw new InvalidOperationException($"Cannot map type {b.Name} to a Feather column type");

            // can always widen to a floating point from an integral
            if (aIsIntegral && bIsFloating) return b;
            if (bIsIntegral && aIsFloating) return a;

            var aSize = NumericSize(a);
            var bSize = NumericSize(b);

            if (aIsFloating && bIsFloating)
            {
                if (aSize > bSize) return a;
                if (bSize > aSize) return b;

                throw new InvalidOperationException($"How can {a.Name} be the same size as {b.Name}; for floating point types");
            }

            var aSign = IntegralSign(a);
            var bSign = IntegralSign(b);

            if (aSign != bSign)
            {
                throw new InvalidOperationException($"Cannot coerce values of types {a.Name} and {b.Name} to coexist in the same column; signs don't match");
            }

            if (aSize > bSize) return a;
            if (bSize > aSize) return b;

            throw new InvalidOperationException($"How can {a.Name} be the same size as {b.Name}; for integral types");
        }

        static bool IsIntegral(Type t) => t == typeof(byte) || t == typeof(sbyte) || t == typeof(short) || t == typeof(ushort) || t == typeof(int) || t == typeof(uint) || t == typeof(long) || t == typeof(ulong);
        static bool IsFloating(Type t) => t == typeof(float) || t == typeof(double);

        static byte NumericSize(Type t)
        {
            if (t == typeof(byte) || t == typeof(sbyte)) return 1;
            if (t == typeof(short) || t == typeof(ushort)) return 2;
            if (t == typeof(int) || t == typeof(uint)) return 4;
            if (t == typeof(long) || t == typeof(ulong)) return 8;
            if (t == typeof(float)) return 4;
            if (t == typeof(double)) return 8;

            throw new InvalidOperationException($"Cannot size unexpected non-numeric type {t.Name}");
        }

        static int IntegralSign(Type t)
        {
            if (t == typeof(sbyte) || t == typeof(short) || t == typeof(int) || t == typeof(long)) return -1;
            if (t == typeof(byte) || t == typeof(ushort) || t == typeof(uint) || t == typeof(ulong)) return 1;

            throw new InvalidOperationException($"Cannot determine sign of non-integral type {t.Name}");
        }

        static bool IsNonNullable(Type t) => t.IsValueType && Nullable.GetUnderlyingType(t) == null;

        static long DetermineLength(System.Collections.IEnumerable data)
        {
            // arrays!
            var realType = data.GetType();
            if (realType.IsArray)
            {
                return ((Array)data).GetLongLength(0);
            }

            // this should cover all the System.Collections.Generic types
            var realInterfaces = realType.GetInterfaces();
            var iCollection = realInterfaces.FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICollection<>));
            if (iCollection != null)
            {
                return CollectionLengthLookup.GetLength(iCollection.GetGenericArguments()[0], data);
            }

            // handling the System.Collection types
            var arrList = data as System.Collections.ArrayList;
            if (arrList != null) return arrList.Count;

            var bitArr = data as System.Collections.BitArray;
            if (bitArr != null) return bitArr.Count;

            var hashTable = data as System.Collections.Hashtable;
            if (hashTable != null) return hashTable.Count;

            var queue = data as System.Collections.Queue;
            if (queue != null) return queue.Count;

            var sortedList = data as System.Collections.SortedList;
            if (sortedList != null) return sortedList.Count;

            var stack = data as System.Collections.Stack;
            if (stack != null) return stack.Count;

            // out of tricks for an O(1), just iterate over the whole thing now
            long ret = 0;
            foreach (var _ in data)
            {
                ret++;
            }

            return ret;
        }

        static void DetermineTypeAndNullCount(System.Collections.IEnumerable data, out Type type, out long nullCount)
        {
            long _;
            DetermineTypeLengthAndNullCount(data, out type, out _, out nullCount);
        }

        static void DetermineLengthAndNullCount(System.Collections.IEnumerable data, out long length, out long nullCount)
        {
            // arrays!
            var realType = data.GetType();
            if (realType.IsArray)
            {
                var elemType = realType.GetElementType();
                length = ((Array)data).GetLongLength(0);

                if (IsNonNullable(elemType))
                {
                    nullCount = 0;
                    return;
                }

                nullCount = CountNulls(data);
                return;
            }

            // this should cover all the System.Collections.Generic types
            var realInterfaces = realType.GetInterfaces();
            var iCollection = realInterfaces.FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICollection<>));
            if (iCollection != null)
            {
                var elemType = iCollection.GetGenericArguments()[0];
                length = CollectionLengthLookup.GetLength(elemType, data);

                if (IsNonNullable(elemType))
                {
                    nullCount = 0;
                }
                else
                {
                    nullCount = CountNulls(data);
                }

                return;
            }

            long nulls = 0;
            long count = 0;
            foreach (var elem in data)
            {
                count++;

                if (elem == null)
                {
                    nulls++;
                    continue;
                }
            }

            length = count;
            nullCount = nulls;
        }

        static long CountNulls(System.Collections.IEnumerable data)
        {
            long nulls = 0;
            foreach (var obj in data)
            {
                // boxed Nullable<T> will also == null, so no need for more sophistication
                if (obj == null)
                {
                    nulls++;
                }
            }

            return nulls;
        }

        void AddColumnImpl(string name, Type type, System.Collections.IEnumerable data, long length, long nullCount)
        {
            var onDiskType = MapToDiskType(type, nullCount);

            if (PendingColumns.Count == 0)
            {
                NumRows = length;
            }
            else
            {
                if (NumRows != length)
                {
                    throw new InvalidOperationException($"Tried to add a column without the correct number of rows.  Expected {NumRows:N0}, found {length:N0}");
                }
            }

            PendingColumns.AddLast(new WriteColumnConfig(name, type, onDiskType, length, data, nullCount));

            if (Mode == WriteMode.Eager)
            {
                SerializePending();
            }
        }

        static ColumnType MapToDiskType(Type type, long nullCount)
        {
            // TODO: Binary & NullableBinary

            if (type == typeof(bool) || type == typeof(bool?))
            {
                return nullCount > 0 ? ColumnType.NullableBool : ColumnType.Bool;
            }

            if (type == typeof(double) || type == typeof(double?))
            {
                return nullCount > 0 ? ColumnType.NullableDouble : ColumnType.Double;
            }

            if (type == typeof(float) || type == typeof(float?))
            {
                return nullCount > 0 ? ColumnType.NullableFloat : ColumnType.Float;
            }

            if (type == typeof(byte) || type == typeof(byte?))
            {
                return nullCount > 0 ? ColumnType.NullableUint8 : ColumnType.Uint8;
            }

            if (type == typeof(sbyte) || type == typeof(sbyte?))
            {
                return nullCount > 0 ? ColumnType.NullableInt8 : ColumnType.Int8;
            }

            if (type == typeof(sbyte) || type == typeof(sbyte?))
            {
                return nullCount > 0 ? ColumnType.NullableInt8 : ColumnType.Int8;
            }

            if (type == typeof(short) || type == typeof(short?))
            {
                return nullCount > 0 ? ColumnType.NullableInt16 : ColumnType.Int16;
            }

            if (type == typeof(ushort) || type == typeof(ushort?))
            {
                return nullCount > 0 ? ColumnType.NullableUint16 : ColumnType.Uint16;
            }

            if (type == typeof(int) || type == typeof(int?))
            {
                return nullCount > 0 ? ColumnType.NullableInt32 : ColumnType.Int32;
            }

            if (type == typeof(uint) || type == typeof(uint?))
            {
                return nullCount > 0 ? ColumnType.NullableUint32 : ColumnType.Uint32;
            }

            if (type == typeof(long) || type == typeof(long?))
            {
                return nullCount > 0 ? ColumnType.NullableInt64 : ColumnType.Int64;
            }

            if (type == typeof(ulong) || type == typeof(ulong?))
            {
                return nullCount > 0 ? ColumnType.NullableUint64 : ColumnType.Uint64;
            }

            if (type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(DateTime?) || type == typeof(DateTimeOffset?))
            {
                // .NET only tracks out to .1 microseconds anyway, so no reason to record at Nanosecond scale
                return nullCount > 0 ? ColumnType.NullableTimestamp_Microsecond : ColumnType.Timestamp_Microsecond;
            }

            if (type == typeof(TimeSpan) || type == typeof(TimeSpan?))
            {
                // .NET only tracks out to .1 microseconds anyway, so no reason to record at Nanosecond scale
                return nullCount > 0 ? ColumnType.NullableTime_Microsecond : ColumnType.Time_Microsecond;
            }

            if (type == typeof(string))
            {
                return nullCount > 0 ? ColumnType.NullableString : ColumnType.String;
            }

            if (type.IsEnum || (Nullable.GetUnderlyingType(type)?.IsEnum ?? false))
            {
                return nullCount > 0 ? ColumnType.NullableCategory : ColumnType.Category;
            }

            throw new InvalidOperationException($"Couldn't map {type.Name} to a Feather type");
        }

        void SerializePending()
        {
            if (DataIndex == 0)
            {
                WriteMagic();
            }

            while (PendingColumns.Count > 0)
            {
                AlignIndexToArrowAlignment(DataStream, ref DataIndex);

                var toProcess = PendingColumns.First.Value;
                PendingColumns.RemoveFirst();

                var metadataEntry = WriteColumn(ref toProcess);
                Metadata.AddLast(metadataEntry);
            }
        }

        static void AlignIndexToArrowAlignment(BinaryWriter stream, ref long currentIndex)
        {
            var advanceBy = currentIndex % FeatherMagic.ARROW_ALIGNMENT;
            if (advanceBy == 0) return;

            if (stream.BaseStream.CanSeek)
            {
                stream.BaseStream.Seek(advanceBy, SeekOrigin.Current);
            }
            else
            {
                // handle streams we can't seek on -_-
                for (var i = 0; i < advanceBy; i++)
                {
                    stream.BaseStream.ReadByte();
                }
            }

            currentIndex += advanceBy;
        }

        void AdvanceNullStreamTo(long position) => AdvanceStreamTo(NullStream, ref NullIndex, position);
        void AdvanceDataStreamTo(long position) => AdvanceStreamTo(DataStream, ref DataIndex, position);
        void AdvanceVariableStreamTo(long position) => AdvanceStreamTo(VariableStream, ref VariableIndex, position);

        static void AdvanceStreamTo(BinaryWriter stream, ref long index, long position)
        {
            var advanceBy = position - index;

            if (advanceBy < 0) throw new Exception($"Shouldn't be possible, tried to move stream back in the file; From: {index:N0} To: {position:N0}");

            if (advanceBy == 0) return;

            if (stream.BaseStream.CanSeek)
            {
                stream.BaseStream.Seek(advanceBy, SeekOrigin.Current);
            }
            else
            {
                // handle streams we can't seek on -_-
                for (var i = 0; i < advanceBy; i++)
                {
                    stream.BaseStream.ReadByte();
                }
            }

            stream.Flush();

            index = position;
        }

        ColumnMetadata WriteColumn(ref WriteColumnConfig column)
        {
            long dataOffset;

            string[] levels;
            string timezone;
            DateTimePrecisionType unit;

            // advance all streams to latest point, so any backing buffers know they can flush
            var furtherPoint = Math.Max(Math.Max(DataIndex, NullIndex), VariableIndex);
            AdvanceDataStreamTo(furtherPoint);
            AdvanceNullStreamTo(furtherPoint);
            AdvanceVariableStreamTo(furtherPoint);

            var onDiskType = column.OnDiskType;

            var hasNulls =
               onDiskType == ColumnType.NullableBinary ||
               onDiskType == ColumnType.NullableBool ||
               onDiskType == ColumnType.NullableCategory ||
               onDiskType == ColumnType.NullableDate ||
               onDiskType == ColumnType.NullableDouble ||
               onDiskType == ColumnType.NullableFloat ||
               onDiskType == ColumnType.NullableInt16 ||
               onDiskType == ColumnType.NullableInt32 ||
               onDiskType == ColumnType.NullableInt64 ||
               onDiskType == ColumnType.NullableInt8 ||
               onDiskType == ColumnType.NullableString ||
               onDiskType == ColumnType.NullableTimestamp_Microsecond ||
               onDiskType == ColumnType.NullableTimestamp_Millisecond ||
               onDiskType == ColumnType.NullableTimestamp_Nanosecond ||
               onDiskType == ColumnType.NullableTimestamp_Second ||
               onDiskType == ColumnType.NullableTime_Microsecond ||
               onDiskType == ColumnType.NullableTime_Millisecond ||
               onDiskType == ColumnType.NullableTime_Nanosecond ||
               onDiskType == ColumnType.NullableTime_Second ||
               onDiskType == ColumnType.NullableUint16 ||
               onDiskType == ColumnType.NullableUint32 ||
               onDiskType == ColumnType.NullableUint64 ||
               onDiskType == ColumnType.NullableUint8;

            var isVariableSized =
                onDiskType == ColumnType.Binary ||
                onDiskType == ColumnType.String ||
                onDiskType == ColumnType.NullableBinary ||
                onDiskType == ColumnType.NullableString;

            long bytesWritten;

            if (!isVariableSized)
            {
                if (hasNulls)
                {
                    WriteNullableData(column.DotNetType, column.Data, column.Length, column.OnDiskType, out dataOffset, out bytesWritten, out levels, out timezone, out unit);
                }
                else
                {
                    WriteData(column.DotNetType, column.Data, column.Length, column.OnDiskType, out dataOffset, out bytesWritten, out levels, out timezone, out unit);
                }
            }
            else
            {
                if (hasNulls)
                {
                    WriteNullableVariableSizedData(column.DotNetType, column.Data, column.Length, column.OnDiskType, out dataOffset, out bytesWritten, out levels, out timezone, out unit);
                }
                else
                {
                    WriteVariableSizedData(column.DotNetType, column.Data, column.Length, column.OnDiskType, out dataOffset, out bytesWritten, out levels, out timezone, out unit);
                }
            }

            return
                new ColumnMetadata
                {
                    Encoding = feather.fbs.Encoding.PLAIN,
                    Length = column.Length,
                    Levels = levels,
                    Name = column.Name,
                    NullCount = column.NullCount,
                    Offset = dataOffset,
                    Ordered = false,
                    TimeZone = timezone,
                    TotalBytes = bytesWritten,
                    Type = column.OnDiskType,
                    Unit = unit
                };
        }

        internal void WriteLevels(string[] levels, out long startIndex, out long numBytes)
        {
            AlignIndexToArrowAlignment(DataStream, ref DataIndex);

            startIndex = DataIndex;

            string[] _;
            string __;
            DateTimePrecisionType ___;
            WriteVariableSizedData(typeof(string), levels, levels.Length, ColumnType.String, out startIndex, out numBytes, out _, out __, out ___);
        }

        void WriteVariableSizedData(Type dataType, System.Collections.IEnumerable data, long length, ColumnType onDiskType, out long dataOffset, out long bytesWritten, out string[] categoryLevels, out string timestampTimezone, out DateTimePrecisionType timeUnit)
        {
            dataOffset = DataIndex;

            var dataStartIndex = DataIndex;
            AdvanceDataStreamTo(dataStartIndex);

            var neededDataEntries = length + 1;                         // need the last index to read into when parsing the strings out
            var neededDataBytes = neededDataEntries * sizeof(int);

            var variableStartIndex = dataStartIndex + neededDataBytes;

            var variableStartPadding = 0;
            if (variableStartIndex % FeatherMagic.ARROW_ALIGNMENT != 0)
            {
                variableStartPadding = FeatherMagic.ARROW_ALIGNMENT - (int)(variableStartIndex % FeatherMagic.ARROW_ALIGNMENT);
            }
            variableStartIndex += variableStartPadding;

            AdvanceVariableStreamTo(variableStartIndex);

            var adapter = WriterAdapterLookup.LookupAdapter(data.GetType(), dataType, onDiskType);
            adapter(this, data);

            var finalIndex = VariableIndex;

            // skip past all the data we just wrote
            AdvanceDataStreamTo(VariableIndex);

            // nothing variable sized ever has metadata
            categoryLevels = null;
            timestampTimezone = null;
            timeUnit = DateTimePrecisionType.NONE;

            bytesWritten = finalIndex - dataOffset;
        }

        void WriteNullableVariableSizedData(Type dataType, System.Collections.IEnumerable data, long length, ColumnType onDiskType, out long dataOffset, out long bytesWritten, out string[] categoryLevels, out string timestampTimezone, out DateTimePrecisionType timeUnit)
        {
            dataOffset = DataIndex;

            var nullStartIndex = DataIndex;
            AdvanceNullStreamTo(nullStartIndex);

            var neededNullBytes = length / 8;
            if (length % 8 != 0) neededNullBytes++;

            int nullPadding = 0;
            if ((neededNullBytes % FeatherMagic.NULL_BITMASK_ALIGNMENT) != 0)
            {
                nullPadding = FeatherMagic.NULL_BITMASK_ALIGNMENT - (int)(neededNullBytes % FeatherMagic.NULL_BITMASK_ALIGNMENT);
            }

            var dataStartIndex = nullStartIndex + neededNullBytes + nullPadding;
            AdvanceDataStreamTo(dataStartIndex);

            var neededDataEntries = length + 1;                         // need the last index to read into when parsing the strings out
            var neededDataBytes = neededDataEntries * sizeof(int);

            var variableStartIndex = dataStartIndex + neededDataBytes;

            var variableStartPadding = 0;
            if (variableStartIndex % FeatherMagic.ARROW_ALIGNMENT != 0)
            {
                variableStartPadding = FeatherMagic.ARROW_ALIGNMENT - (int)(variableStartIndex % FeatherMagic.ARROW_ALIGNMENT);
            }
            variableStartIndex += variableStartPadding;

            AdvanceVariableStreamTo(variableStartIndex);

            var adapter = WriterAdapterLookup.LookupAdapter(data.GetType(), dataType, onDiskType);
            adapter(this, data);

            var finalIndex = VariableIndex;

            // skip past all the crap we just wrote
            AdvanceDataStreamTo(VariableIndex);

            // nothing variable sized ever has metadata
            categoryLevels = null;
            timestampTimezone = null;
            timeUnit = DateTimePrecisionType.NONE;

            bytesWritten = finalIndex - dataOffset;
        }

        void WriteNullableData(Type dataType, System.Collections.IEnumerable data, long length, ColumnType onDiskType, out long dataOffset, out long bytesWritten, out string[] categoryLevels, out string timestampTimezone, out DateTimePrecisionType timeUnit)
        {
            dataOffset = DataIndex;

            var nullStartIndex = DataIndex;
            AdvanceNullStreamTo(nullStartIndex);

            var neededNullBytes = length / 8;
            if (length % 8 != 0) neededNullBytes++;

            int nullPadding = 0;
            if ((neededNullBytes % FeatherMagic.NULL_BITMASK_ALIGNMENT) != 0)
            {
                nullPadding = FeatherMagic.NULL_BITMASK_ALIGNMENT - (int)(neededNullBytes % FeatherMagic.NULL_BITMASK_ALIGNMENT);
            }

            var dataStartIndex = nullStartIndex + neededNullBytes + nullPadding;
            AdvanceDataStreamTo(dataStartIndex);

            var adapter = WriterAdapterLookup.LookupAdapter(data.GetType(), dataType, onDiskType);
            adapter(this, data);

            var finalIndex = DataIndex;

            var isDateTime =
                onDiskType == ColumnType.Date ||
                onDiskType == ColumnType.NullableDate ||
                onDiskType == ColumnType.Timestamp_Microsecond ||
                onDiskType == ColumnType.Timestamp_Millisecond ||
                onDiskType == ColumnType.Timestamp_Nanosecond ||
                onDiskType == ColumnType.Timestamp_Second ||
                onDiskType == ColumnType.NullableTimestamp_Microsecond ||
                onDiskType == ColumnType.NullableTimestamp_Millisecond ||
                onDiskType == ColumnType.NullableTimestamp_Nanosecond ||
                onDiskType == ColumnType.NullableTimestamp_Second;

            var isTime =
                onDiskType == ColumnType.Time_Microsecond ||
                onDiskType == ColumnType.Time_Millisecond ||
                onDiskType == ColumnType.Time_Nanosecond ||
                onDiskType == ColumnType.Time_Second ||
                onDiskType == ColumnType.NullableTime_Microsecond ||
                onDiskType == ColumnType.NullableTime_Millisecond ||
                onDiskType == ColumnType.NullableTime_Nanosecond ||
                onDiskType == ColumnType.NullableTime_Second;

            var isEnum =
                onDiskType == ColumnType.Category ||
                onDiskType == ColumnType.NullableCategory;

            if (isDateTime)
            {
                categoryLevels = null;
                timestampTimezone = "GMT";                          // always UTC
                timeUnit = DateTimePrecisionType.Microsecond;       // always microsecond (.1 tick) precision
            }
            else
            {
                if (isTime)
                {
                    categoryLevels = null;
                    timestampTimezone = null;
                    timeUnit = DateTimePrecisionType.Microsecond;       // always microsecond (.1 tick) precision
                }
                else
                {
                    if (isEnum)
                    {
                        timestampTimezone = null;
                        timeUnit = DateTimePrecisionType.NONE;
                        categoryLevels = EnumDetails.GetLevels(dataType);
                    }
                    else
                    {
                        categoryLevels = null;
                        timestampTimezone = null;
                        timeUnit = DateTimePrecisionType.NONE;
                    }
                }
            }

            bytesWritten = finalIndex - dataOffset;
        }

        void WriteData(Type dataType, System.Collections.IEnumerable data, long length, ColumnType onDiskType, out long dataOffset, out long bytesWritten, out string[] categoryLevels, out string timestampTimezone, out DateTimePrecisionType timeUnit)
        {
            dataOffset = DataIndex;

            var adapter = WriterAdapterLookup.LookupAdapter(data.GetType(), dataType, onDiskType);
            adapter(this, data);
            var finalIndex = DataIndex;

            var isDateTime =
                onDiskType == ColumnType.Date ||
                onDiskType == ColumnType.NullableDate ||
                onDiskType == ColumnType.Timestamp_Microsecond ||
                onDiskType == ColumnType.Timestamp_Millisecond ||
                onDiskType == ColumnType.Timestamp_Nanosecond ||
                onDiskType == ColumnType.Timestamp_Second ||
                onDiskType == ColumnType.NullableTimestamp_Microsecond ||
                onDiskType == ColumnType.NullableTimestamp_Millisecond ||
                onDiskType == ColumnType.NullableTimestamp_Nanosecond ||
                onDiskType == ColumnType.NullableTimestamp_Second;

            var isTime =
                onDiskType == ColumnType.Time_Microsecond ||
                onDiskType == ColumnType.Time_Millisecond ||
                onDiskType == ColumnType.Time_Nanosecond ||
                onDiskType == ColumnType.Time_Second ||
                onDiskType == ColumnType.NullableTime_Microsecond ||
                onDiskType == ColumnType.NullableTime_Millisecond ||
                onDiskType == ColumnType.NullableTime_Nanosecond ||
                onDiskType == ColumnType.NullableTime_Second;

            var isEnum =
                onDiskType == ColumnType.Category ||
                onDiskType == ColumnType.NullableCategory;

            if (isDateTime)
            {
                categoryLevels = null;
                timestampTimezone = "GMT";                          // always UTC
                timeUnit = DateTimePrecisionType.Microsecond;       // always microsecond (.1 tick) precision
            }
            else
            {
                if (isTime)
                {
                    categoryLevels = null;
                    timestampTimezone = null;
                    timeUnit = DateTimePrecisionType.Microsecond;       // always microsecond (.1 tick) precision
                }
                else
                {
                    if (isEnum)
                    {
                        timestampTimezone = null;
                        timeUnit = DateTimePrecisionType.NONE;
                        categoryLevels = EnumDetails.GetLevels(dataType);
                    }
                    else
                    {
                        categoryLevels = null;
                        timestampTimezone = null;
                        timeUnit = DateTimePrecisionType.NONE;
                    }
                }
            }

            bytesWritten = finalIndex - dataOffset;
        }

        static long NumberOfBytesForNullMask(ref WriteColumnConfig config)
        {
            long numNullBytes;
            if (config.NullCount == 0)
            {
                return 0;
            }
            else
            {
                numNullBytes = config.Length / 8;
                if (config.Length % 8 != 0)
                {
                    numNullBytes++;
                }
            }

            long numNullBytesWithPadding;
            if (numNullBytes % FeatherMagic.NULL_BITMASK_ALIGNMENT == 0)
            {
                numNullBytesWithPadding = numNullBytes;
            }
            else
            {
                numNullBytesWithPadding = numNullBytes + numNullBytes % FeatherMagic.NULL_BITMASK_ALIGNMENT;
            }

            return numNullBytes;
        }

        void CloseAndDiscard()
        {
            WriteMetadata();

            WriteTrailingMagic();

            DataStream.Dispose();
            NullStream.Dispose();
            VariableStream.Dispose();

            DataStream = NullStream = VariableStream = null;
        }

        void WriteMetadata()
        {
            var bufferBuilder = new FlatBuffers.FlatBufferBuilder(128);

            // note: R & Python feather impls don't handle the default (ForceDefaults = false)
            //    configuration correctly so always write default values
            bufferBuilder.ForceDefaults = true;

            var columns = new List<FlatBuffers.Offset<feather.fbs.Column>>();
            foreach (var col in Metadata)
            {
                var colName = bufferBuilder.CreateString(col.Name);

                var primArr =
                    feather.fbs.PrimitiveArray.CreatePrimitiveArray(
                        bufferBuilder,
                        col.Type.MapToFeatherEnum(),
                        col.Encoding,
                        col.Offset,
                        col.Length,
                        col.NullCount,
                        col.TotalBytes
                    );

                feather.fbs.TypeMetadata metadataType;
                FlatBuffers.Offset<feather.fbs.PrimitiveArray> levels;
                int metaDataOffset;

                col.CreateMetadata(bufferBuilder, this, out metaDataOffset, out metadataType, out levels);

                var colRef = feather.fbs.Column.CreateColumn(
                    bufferBuilder,
                    colName,
                    primArr,
                    metadataType,
                    metaDataOffset,
                    default(FlatBuffers.StringOffset)
                );

                columns.Add(colRef);
            }

            var columnsVec = feather.fbs.CTable.CreateColumnsVector(bufferBuilder, columns.ToArray());

            var ctable =
                feather.fbs.CTable.CreateCTable(
                    bufferBuilder,
                    default(FlatBuffers.StringOffset),
                    NumRows,
                    columnsVec,
                    FeatherMagic.FEATHER_VERSION,
                    default(FlatBuffers.StringOffset)
                );

            bufferBuilder.Finish(ctable.Value);

            var bytes = bufferBuilder.SizedByteArray();

            AlignIndexToArrowAlignment(DataStream, ref DataIndex);
            DataStream.Write(bytes, 0, bytes.Length);
            DataStream.Write(bytes.Length);

            DataIndex += bytes.Length;
            DataIndex += sizeof(int);
        }

        void WriteMagic()
        {
            DataStream.Write(FeatherMagic.MAGIC_HEADER);
            DataIndex += FeatherMagic.MAGIC_HEADER_SIZE;
        }

        void WriteLeadingMagic() => WriteMagic();
        void WriteTrailingMagic() => WriteMagic();

        void SerializeAndDiscard()
        {
            SerializePending();
            CloseAndDiscard();
        }

        internal void BlitNonNullableBoolArray(bool[] arr)
        {
            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < arr.LongLength; i++)
            {
                var b = arr[i];

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    DataStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }
            }

            if (ix != 0)
            {
                DataStream.Write(currentByte);
            }

            DataIndex += arr.LongLength / 8;
            if (arr.LongLength % 8 != 0)
            {
                DataIndex++;
            }
        }

        internal void CopyNonNullableBoolCollection(ICollection<bool> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var b in col)
            {
                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    DataStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }
            }

            if (ix != 0)
            {
                DataStream.Write(currentByte);
            }

            DataIndex += col.Count / 8;
            if (col.Count % 8 != 0)
            {
                DataIndex++;
            }
        }

        internal void CopyNonNullableBoolIEnumerable(IEnumerable<bool> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var b in col)
            {
                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    DataStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                    DataIndex++;
                }
            }

            if (ix != 0)
            {
                DataStream.Write(currentByte);
                DataIndex++;
            }
        }

        internal void BlitNullableBoolArray(bool?[] arr)
        {
            byte nullableByte = 0;
            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < arr.LongLength; i++)
            {
                var b = arr[i];

                if (b.HasValue)
                {
                    nullableByte |= (byte)(1 << ix);
                    if (b.Value)
                    {
                        currentByte |= (byte)(1 << ix);
                    }
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(nullableByte);
                    DataStream.Write(currentByte);
                    ix = 0;
                    nullableByte = 0;
                    currentByte = 0;
                }
            }

            if (ix != 0)
            {
                NullStream.Write(nullableByte);
                DataStream.Write(currentByte);
            }

            var bytesSpace = arr.LongLength / 8; ;

            NullIndex += bytesSpace;
            DataIndex += bytesSpace;
            if (arr.LongLength % 8 != 0)
            {
                NullIndex++;
                DataIndex++;
            }
        }

        internal void CopyNullableBoolCollection(ICollection<bool?> col)
        {
            byte nullableByte = 0;
            byte currentByte = 0;
            var ix = 0;
            foreach (var b in col)
            {
                if (b.HasValue)
                {
                    nullableByte |= (byte)(1 << ix);
                    if (b.Value)
                    {
                        currentByte |= (byte)(1 << ix);
                    }
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(nullableByte);
                    DataStream.Write(currentByte);
                    ix = 0;
                    nullableByte = 0;
                    currentByte = 0;
                }
            }

            if (ix != 0)
            {
                NullStream.Write(nullableByte);
                DataStream.Write(currentByte);
            }

            var bytesSpace = col.Count / 8;

            NullIndex += bytesSpace;
            DataIndex += bytesSpace;
            if (col.Count % 8 != 0)
            {
                NullIndex++;
                DataIndex++;
            }
        }

        internal void CopyNullableBoolIEnumerable(IEnumerable<bool?> col)
        {
            byte nullableByte = 0;
            byte currentByte = 0;
            var ix = 0;
            foreach (var b in col)
            {
                if (b.HasValue)
                {
                    nullableByte |= (byte)(1 << ix);
                    if (b.Value)
                    {
                        currentByte |= (byte)(1 << ix);
                    }
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(nullableByte);
                    DataStream.Write(currentByte);
                    ix = 0;
                    nullableByte = 0;
                    currentByte = 0;

                    NullIndex++;
                    DataIndex++;
                }
            }

            if (ix != 0)
            {
                NullStream.Write(nullableByte);
                DataStream.Write(currentByte);
                NullIndex++;
                DataIndex++;
            }
        }

        internal void BlitNonNullableArray(double[] arr)
        {
            foreach (var val in arr)
            {
                var asLong = BitConverter.DoubleToInt64Bits(val);
                DataStream.Write(asLong);
            }

            DataIndex += arr.Length * sizeof(double);
        }

        internal void CopyNonNullableCollection(ICollection<double> col)
        {
            foreach (var val in col)
            {
                var asLong = BitConverter.DoubleToInt64Bits(val);
                DataStream.Write(asLong);
            }

            DataIndex += col.Count * sizeof(double);
        }

        internal void CopyNonNullableIEnumerable(IEnumerable<double> col)
        {
            foreach (var val in col)
            {
                var asLong = BitConverter.DoubleToInt64Bits(val);
                DataStream.Write(asLong);
                DataIndex += sizeof(double);
            }
        }

        internal void BlitNullableArray(double?[] arr)
        {
            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < arr.LongLength; i++)
            {
                var val = arr[i];
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(BitConverter.DoubleToInt64Bits(val ?? double.NaN));
            }

            DataIndex += arr.LongLength * sizeof(double);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += arr.Length / 8;
            if (arr.Length % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableCollection(ICollection<double?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(BitConverter.DoubleToInt64Bits(val ?? double.NaN));
            }

            DataIndex += col.Count * sizeof(double);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += col.Count / 8;
            if (col.Count % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableIEnumerable(IEnumerable<double?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                    NullIndex++;
                }

                DataStream.Write(BitConverter.DoubleToInt64Bits(val ?? double.NaN));
                DataIndex += sizeof(double);
            }


            if (ix != 0)
            {
                NullStream.Write(currentByte);
                NullIndex++;
            }
        }

        // Curses: BitConverter doesn't have this, so implement it ourselves
        [System.Security.SecuritySafeCritical]
        static unsafe int SingleToInt32Bits(float value)
        {
            return *((int*)&value);
        }

        internal void BlitNonNullableArray(float[] arr)
        {
            foreach (var val in arr)
            {
                var asInt = SingleToInt32Bits(val);
                DataStream.Write(asInt);
            }

            DataIndex += arr.Length * sizeof(float);
        }

        internal void CopyNonNullableCollection(ICollection<float> col)
        {
            foreach (var val in col)
            {
                var asInt = SingleToInt32Bits(val);
                DataStream.Write(asInt);
            }

            DataIndex += col.Count * sizeof(float);
        }

        internal void CopyNonNullableIEnumerable(IEnumerable<float> col)
        {
            foreach (var val in col)
            {
                var asLong = SingleToInt32Bits(val);
                DataStream.Write(asLong);
                DataIndex += sizeof(float);
            }
        }

        internal void BlitNullableArray(float?[] arr)
        {
            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < arr.LongLength; i++)
            {
                var val = arr[i];
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(SingleToInt32Bits(val ?? float.NaN));
            }

            DataIndex += arr.LongLength * sizeof(float);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += arr.Length / 8;
            if (arr.Length % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableCollection(ICollection<float?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(SingleToInt32Bits(val ?? float.NaN));
            }

            DataIndex += col.Count * sizeof(float);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += col.Count / 8;
            if (col.Count % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableIEnumerable(IEnumerable<float?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                    NullIndex++;
                }

                DataStream.Write(SingleToInt32Bits(val ?? float.NaN));
                DataIndex += sizeof(float);
            }


            if (ix != 0)
            {
                NullStream.Write(currentByte);
                NullIndex++;
            }
        }

        internal void BlitNonNullableArray(long[] arr)
        {
            foreach (var val in arr)
            {
                DataStream.Write(val);
            }

            DataIndex += arr.Length * sizeof(long);
        }

        internal void CopyNonNullableCollection(ICollection<long> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
            }

            DataIndex += col.Count * sizeof(long);
        }

        internal void CopyNonNullableIEnumerable(IEnumerable<long> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
                DataIndex += sizeof(long);
            }
        }

        internal void BlitNullableArray(long?[] arr)
        {
            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < arr.LongLength; i++)
            {
                var val = arr[i];
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? long.MaxValue);
            }

            DataIndex += arr.LongLength * sizeof(long);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += arr.Length / 8;
            if (arr.Length % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableCollection(ICollection<long?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? long.MaxValue);
            }

            DataIndex += col.Count * sizeof(long);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += col.Count / 8;
            if (col.Count % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableIEnumerable(IEnumerable<long?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                    NullIndex++;
                }

                DataStream.Write(val ?? long.MaxValue);
                DataIndex += sizeof(long);
            }


            if (ix != 0)
            {
                NullStream.Write(currentByte);
                NullIndex++;
            }
        }

        internal void BlitNonNullableArray(ulong[] arr)
        {
            foreach (var val in arr)
            {
                DataStream.Write(val);
            }

            DataIndex += arr.Length * sizeof(ulong);
        }

        internal void CopyNonNullableCollection(ICollection<ulong> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
            }

            DataIndex += col.Count * sizeof(ulong);
        }

        internal void CopyNonNullableIEnumerable(IEnumerable<ulong> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
                DataIndex += sizeof(ulong);
            }
        }

        internal void BlitNullableArray(ulong?[] arr)
        {
            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < arr.LongLength; i++)
            {
                var val = arr[i];
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? ulong.MaxValue);
            }

            DataIndex += arr.LongLength * sizeof(ulong);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += arr.Length / 8;
            if (arr.Length % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableCollection(ICollection<ulong?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? ulong.MaxValue);
            }

            DataIndex += col.Count * sizeof(ulong);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += col.Count / 8;
            if (col.Count % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableIEnumerable(IEnumerable<ulong?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                    NullIndex++;
                }

                DataStream.Write(val ?? ulong.MaxValue);
                DataIndex += sizeof(ulong);
            }


            if (ix != 0)
            {
                NullStream.Write(currentByte);
                NullIndex++;
            }
        }

        internal void BlitNonNullableArray(int[] arr)
        {
            foreach (var val in arr)
            {
                DataStream.Write(val);
            }

            DataIndex += arr.Length * sizeof(int);
        }

        internal void CopyNonNullableCollection(ICollection<int> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
            }

            DataIndex += col.Count * sizeof(int);
        }

        internal void CopyNonNullableIEnumerable(IEnumerable<int> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
                DataIndex += sizeof(int);
            }
        }

        internal void BlitNullableArray(int?[] arr)
        {
            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < arr.LongLength; i++)
            {
                var val = arr[i];
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? int.MaxValue);
            }

            DataIndex += arr.LongLength * sizeof(int);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += arr.Length / 8;
            if (arr.Length % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableCollection(ICollection<int?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? int.MaxValue);
            }

            DataIndex += col.Count * sizeof(int);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += col.Count / 8;
            if (col.Count % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableIEnumerable(IEnumerable<int?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                    NullIndex++;
                }

                DataStream.Write(val ?? int.MaxValue);
                DataIndex += sizeof(int);
            }


            if (ix != 0)
            {
                NullStream.Write(currentByte);
                NullIndex++;
            }
        }

        internal void BlitNonNullableArray(uint[] arr)
        {
            foreach (var val in arr)
            {
                DataStream.Write(val);
            }

            DataIndex += arr.Length * sizeof(uint);
        }

        internal void CopyNonNullableCollection(ICollection<uint> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
            }

            DataIndex += col.Count * sizeof(uint);
        }

        internal void CopyNonNullableIEnumerable(IEnumerable<uint> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
                DataIndex += sizeof(uint);
            }
        }

        internal void BlitNullableArray(uint?[] arr)
        {
            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < arr.LongLength; i++)
            {
                var val = arr[i];
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? uint.MaxValue);
            }

            DataIndex += arr.LongLength * sizeof(uint);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += arr.Length / 8;
            if (arr.Length % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableCollection(ICollection<uint?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? uint.MaxValue);
            }

            DataIndex += col.Count * sizeof(uint);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += col.Count / 8;
            if (col.Count % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableIEnumerable(IEnumerable<uint?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                    NullIndex++;
                }

                DataStream.Write(val ?? uint.MaxValue);
                DataIndex += sizeof(uint);
            }


            if (ix != 0)
            {
                NullStream.Write(currentByte);
                NullIndex++;
            }
        }

        internal void BlitNonNullableArray(short[] arr)
        {
            foreach (var val in arr)
            {
                DataStream.Write(val);
            }

            DataIndex += arr.Length * sizeof(short);
        }

        internal void CopyNonNullableCollection(ICollection<short> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
            }

            DataIndex += col.Count * sizeof(short);
        }

        internal void CopyNonNullableIEnumerable(IEnumerable<short> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
                DataIndex += sizeof(short);
            }
        }

        internal void BlitNullableArray(short?[] arr)
        {
            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < arr.LongLength; i++)
            {
                var val = arr[i];
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? short.MaxValue);
            }

            DataIndex += arr.LongLength * sizeof(short);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += arr.Length / 8;
            if (arr.Length % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableCollection(ICollection<short?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? short.MaxValue);
            }

            DataIndex += col.Count * sizeof(short);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += col.Count / 8;
            if (col.Count % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableIEnumerable(IEnumerable<short?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                    NullIndex++;
                }

                DataStream.Write(val ?? short.MaxValue);
                DataIndex += sizeof(short);
            }


            if (ix != 0)
            {
                NullStream.Write(currentByte);
                NullIndex++;
            }
        }

        internal void BlitNonNullableArray(ushort[] arr)
        {
            foreach (var val in arr)
            {
                DataStream.Write(val);
            }

            DataIndex += arr.Length * sizeof(ushort);
        }

        internal void CopyNonNullableCollection(ICollection<ushort> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
            }

            DataIndex += col.Count * sizeof(ushort);
        }

        internal void CopyNonNullableIEnumerable(IEnumerable<ushort> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
                DataIndex += sizeof(ushort);
            }
        }

        internal void BlitNullableArray(ushort?[] arr)
        {
            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < arr.LongLength; i++)
            {
                var val = arr[i];
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? ushort.MaxValue);
            }

            DataIndex += arr.LongLength * sizeof(ushort);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += arr.Length / 8;
            if (arr.Length % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableCollection(ICollection<ushort?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? ushort.MaxValue);
            }

            DataIndex += col.Count * sizeof(ushort);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += col.Count / 8;
            if (col.Count % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableIEnumerable(IEnumerable<ushort?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                    NullIndex++;
                }

                DataStream.Write(val ?? ushort.MaxValue);
                DataIndex += sizeof(ushort);
            }


            if (ix != 0)
            {
                NullStream.Write(currentByte);
                NullIndex++;
            }
        }

        internal void BlitNonNullableArray(byte[] arr)
        {
            DataStream.Write(arr, 0, arr.Length);
            DataIndex += arr.Length * sizeof(byte);
        }

        internal void CopyNonNullableCollection(ICollection<byte> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
            }

            DataIndex += col.Count * sizeof(byte);
        }

        internal void CopyNonNullableIEnumerable(IEnumerable<byte> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
                DataIndex += sizeof(byte);
            }
        }

        internal void BlitNullableArray(byte?[] arr)
        {
            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < arr.LongLength; i++)
            {
                var val = arr[i];
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? byte.MaxValue);
            }

            DataIndex += arr.LongLength * sizeof(byte);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += arr.Length / 8;
            if (arr.Length % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableCollection(ICollection<byte?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? byte.MaxValue);
            }

            DataIndex += col.Count * sizeof(byte);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += col.Count / 8;
            if (col.Count % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableIEnumerable(IEnumerable<byte?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                    NullIndex++;
                }

                DataStream.Write(val ?? byte.MaxValue);
                DataIndex += sizeof(byte);
            }


            if (ix != 0)
            {
                NullStream.Write(currentByte);
                NullIndex++;
            }
        }

        internal void BlitNonNullableArray(sbyte[] arr)
        {
            foreach (var val in arr)
            {
                DataStream.Write(val);
            }

            DataIndex += arr.Length * sizeof(sbyte);
        }

        internal void CopyNonNullableCollection(ICollection<sbyte> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
            }

            DataIndex += col.Count * sizeof(sbyte);
        }

        internal void CopyNonNullableIEnumerable(IEnumerable<sbyte> col)
        {
            foreach (var val in col)
            {
                DataStream.Write(val);
                DataIndex += sizeof(sbyte);
            }
        }

        internal void BlitNullableArray(sbyte?[] arr)
        {
            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < arr.LongLength; i++)
            {
                var val = arr[i];
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? sbyte.MaxValue);
            }

            DataIndex += arr.LongLength * sizeof(sbyte);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += arr.Length / 8;
            if (arr.Length % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableCollection(ICollection<sbyte?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }

                DataStream.Write(val ?? sbyte.MaxValue);
            }

            DataIndex += col.Count * sizeof(sbyte);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += col.Count / 8;
            if (col.Count % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableIEnumerable(IEnumerable<sbyte?> col)
        {
            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                    NullIndex++;
                }

                DataStream.Write(val ?? sbyte.MaxValue);
                DataIndex += sizeof(sbyte);
            }

            if (ix != 0)
            {
                NullStream.Write(currentByte);
                NullIndex++;
            }
        }

        internal void CopyStringArray(string[] col)
        {
            var utf8 = Encoding.UTF8;
            var offset = 0;

            for (var i = 0; i < col.Length; i++)
            {
                var str = col[i];
                var bytes = utf8.GetBytes(str);
                VariableStream.Write(bytes);
                VariableIndex += bytes.Length;

                DataStream.Write(offset);

                offset += bytes.Length;
            }

            // write the last index out
            DataStream.Write(offset);
            DataIndex += (col.LongLength + 1) * sizeof(int);
        }

        internal void CopyStringCollection(ICollection<string> col)
        {
            var utf8 = Encoding.UTF8;
            var offset = 0;

            foreach (var str in col)
            {
                var bytes = utf8.GetBytes(str);
                VariableStream.Write(bytes);
                VariableIndex += bytes.Length;

                DataStream.Write(offset);

                offset += bytes.Length;
            }

            // write the last index out
            DataStream.Write(offset);
            DataIndex += (col.Count + 1) * sizeof(int);
        }

        internal void CopyStringIEnumerable(IEnumerable<string> col)
        {
            var utf8 = Encoding.UTF8;
            var offset = 0;

            foreach (var str in col)
            {
                var bytes = utf8.GetBytes(str);
                VariableStream.Write(bytes);
                VariableIndex += bytes.Length;

                DataStream.Write(offset);
                DataIndex += sizeof(int);

                offset += bytes.Length;
            }

            // write the last index out
            DataStream.Write(offset);
            DataIndex += sizeof(int);
        }

        internal void CopyNullableStringArray(string[] col)
        {
            var utf8 = Encoding.UTF8;
            var offset = 0;

            byte currentNullByte = 0;
            var ix = 0;

            for (var i = 0; i < col.Length; i++)
            {
                var writtenVariableBytes = 0;
                var str = col[i];
                if (str != null)
                {
                    currentNullByte |= (byte)(1 << ix);

                    var bytes = utf8.GetBytes(str);
                    VariableStream.Write(bytes);
                    VariableIndex += bytes.Length;
                    writtenVariableBytes = bytes.Length;
                }
                DataStream.Write(offset);

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentNullByte);
                    ix = 0;
                    currentNullByte = 0;
                    NullIndex++;
                }

                offset += writtenVariableBytes;
            }

            // write the last index out
            DataStream.Write(offset);
            DataIndex += (col.LongLength + 1) * sizeof(int);

            if (ix != 0)
            {
                NullStream.Write(currentNullByte);
                NullIndex++;
            }
        }

        internal void CopyNullableStringCollection(ICollection<string> col)
        {
            var utf8 = Encoding.UTF8;
            var offset = 0;

            byte currentNullByte = 0;
            var ix = 0;

            foreach (var str in col)
            {
                var writtenVariableBytes = 0;
                if (str != null)
                {
                    currentNullByte |= (byte)(1 << ix);

                    var bytes = utf8.GetBytes(str);
                    VariableStream.Write(bytes);
                    VariableIndex += bytes.Length;
                    writtenVariableBytes = bytes.Length;
                }
                DataStream.Write(offset);

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentNullByte);
                    ix = 0;
                    currentNullByte = 0;
                    NullIndex++;
                }

                offset += writtenVariableBytes;
            }

            // write the last index out
            DataStream.Write(offset);
            DataIndex += (col.Count + 1) * sizeof(int);

            if (ix != 0)
            {
                NullStream.Write(currentNullByte);
                NullIndex++;
            }
        }

        internal void CopyNullableStringIEnumerable(IEnumerable<string> col)
        {
            var utf8 = Encoding.UTF8;
            var offset = 0;

            byte currentNullByte = 0;
            var ix = 0;

            foreach (var str in col)
            {
                var writtenVariableBytes = 0;
                if (str != null)
                {
                    currentNullByte |= (byte)(1 << ix);

                    var bytes = utf8.GetBytes(str);
                    VariableStream.Write(bytes);
                    VariableIndex += bytes.Length;
                    writtenVariableBytes = bytes.Length;
                }
                DataStream.Write(offset);
                DataIndex += sizeof(int);

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentNullByte);
                    ix = 0;
                    currentNullByte = 0;
                    NullIndex++;
                }

                offset += writtenVariableBytes;
            }

            // write the last index out
            DataStream.Write(offset);
            DataIndex += sizeof(int);

            if (ix != 0)
            {
                NullStream.Write(currentNullByte);
                NullIndex++;
            }
        }

        const double SECONDS_PER_TICK = 0.0000001;
        const double MILLISECONDS_PER_TICK = 0.0001;
        const double MICROSECONDS_PER_TICK = 0.1;
        const double NANOSECONDS_PER_TICK = 100;
        static long MapToDiskType(long elapsedTicks)
        {
            return (long)Math.Round(MICROSECONDS_PER_TICK * elapsedTicks);
        }

        internal void BlitDateTimeArray(DateTime[] col)
        {
            for (var i = 0; i < col.Length; i++)
            {
                var dt = col[i];
                if (dt.Kind != DateTimeKind.Utc)
                {
                    dt = dt.ToUniversalTime();
                }

                var timeSinceEpoch = dt - FeatherMagic.DATETIME_EPOCH;
                var elapsedTicks = timeSinceEpoch.Ticks;

                var value = MapToDiskType(elapsedTicks);
                DataStream.Write(value);
            }

            DataIndex += col.LongLength * sizeof(long);
        }

        internal void BlitDateTimeOffsetArray(DateTimeOffset[] col)
        {
            for (var i = 0; i < col.Length; i++)
            {
                var dt = col[i].UtcDateTime;

                var timeSinceEpoch = dt - FeatherMagic.DATETIME_EPOCH;
                var elapsedTicks = timeSinceEpoch.Ticks;

                var value = MapToDiskType(elapsedTicks);
                DataStream.Write(value);
            }

            DataIndex += col.LongLength * sizeof(long);
        }

        internal void CopyDateTimeCollection(ICollection<DateTime> col)
        {
            foreach (var dt in col)
            {
                var dtCopy = dt;
                if (dtCopy.Kind != DateTimeKind.Utc)
                {
                    dtCopy = dtCopy.ToUniversalTime();
                }

                var timeSinceEpoch = dtCopy - FeatherMagic.DATETIME_EPOCH;
                var elapsedTicks = timeSinceEpoch.Ticks;

                var value = MapToDiskType(elapsedTicks);
                DataStream.Write(value);
            }

            DataIndex += col.Count * sizeof(long);
        }

        internal void CopyDateTimeOffsetCollection(ICollection<DateTimeOffset> col)
        {
            foreach (var dt in col)
            {
                var dtCopy = dt.UtcDateTime;

                var timeSinceEpoch = dtCopy - FeatherMagic.DATETIME_EPOCH;
                var elapsedTicks = timeSinceEpoch.Ticks;

                var value = MapToDiskType(elapsedTicks);
                DataStream.Write(value);
            }

            DataIndex += col.Count * sizeof(long);
        }

        internal void CopyDateTimeIEnumerable(IEnumerable<DateTime> col)
        {
            foreach (var dt in col)
            {
                var dtCopy = dt;
                if (dtCopy.Kind != DateTimeKind.Utc)
                {
                    dtCopy = dtCopy.ToUniversalTime();
                }

                var timeSinceEpoch = dtCopy - FeatherMagic.DATETIME_EPOCH;
                var elapsedTicks = timeSinceEpoch.Ticks;

                var value = MapToDiskType(elapsedTicks);
                DataStream.Write(value);
                DataIndex += sizeof(long);
            }
        }

        internal void CopyDateTimeOffsetIEnumerable(IEnumerable<DateTimeOffset> col)
        {
            foreach (var dt in col)
            {
                var dtCopy = dt.UtcDateTime;

                var timeSinceEpoch = dtCopy - FeatherMagic.DATETIME_EPOCH;
                var elapsedTicks = timeSinceEpoch.Ticks;

                var value = MapToDiskType(elapsedTicks);
                DataStream.Write(value);
                DataIndex += sizeof(long);
            }
        }

        internal void BlitNullableDateTimeArray(DateTime?[] col)
        {
            byte currentNullByte = 0;
            var ix = 0;

            for (var i = 0; i < col.Length; i++)
            {
                var dt = col[i];
                if (dt != null)
                {
                    currentNullByte |= (byte)(1 << ix);

                    var dtValue = dt.Value;

                    if (dtValue.Kind != DateTimeKind.Utc)
                    {
                        dtValue = dtValue.ToUniversalTime();
                    }

                    var timeSinceEpoch = dtValue - FeatherMagic.DATETIME_EPOCH;
                    var elapsedTicks = timeSinceEpoch.Ticks;

                    var value = MapToDiskType(elapsedTicks);
                    DataStream.Write(value);
                }
                else
                {
                    DataStream.Write(long.MaxValue);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentNullByte);
                    ix = 0;
                    currentNullByte = 0;
                    NullIndex++;
                }
            }

            if (ix != 0)
            {
                NullStream.Write(currentNullByte);
                NullIndex++;
            }

            DataIndex += col.LongLength * sizeof(long);
        }

        internal void BlitNullableDateTimeOffsetArray(DateTimeOffset?[] col)
        {
            byte currentNullByte = 0;
            var ix = 0;

            for (var i = 0; i < col.Length; i++)
            {
                var dt = col[i];
                if (dt != null)
                {
                    currentNullByte |= (byte)(1 << ix);

                    var dtValue = dt.Value.UtcDateTime;

                    var timeSinceEpoch = dtValue - FeatherMagic.DATETIME_EPOCH;
                    var elapsedTicks = timeSinceEpoch.Ticks;

                    var value = MapToDiskType(elapsedTicks);
                    DataStream.Write(value);
                }
                else
                {
                    DataStream.Write(long.MaxValue);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentNullByte);
                    ix = 0;
                    currentNullByte = 0;
                    NullIndex++;
                }
            }

            if (ix != 0)
            {
                NullStream.Write(currentNullByte);
                NullIndex++;
            }

            DataIndex += col.LongLength * sizeof(long);
        }

        internal void CopyNullableDateTimeCollection(ICollection<DateTime?> col)
        {
            byte currentNullByte = 0;
            var ix = 0;

            foreach (var dt in col)
            {
                if (dt != null)
                {
                    currentNullByte |= (byte)(1 << ix);

                    var dtValue = dt.Value;

                    if (dtValue.Kind != DateTimeKind.Utc)
                    {
                        dtValue = dtValue.ToUniversalTime();
                    }

                    var timeSinceEpoch = dtValue - FeatherMagic.DATETIME_EPOCH;
                    var elapsedTicks = timeSinceEpoch.Ticks;

                    var value = MapToDiskType(elapsedTicks);
                    DataStream.Write(value);
                }
                else
                {
                    DataStream.Write(long.MaxValue);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentNullByte);
                    ix = 0;
                    currentNullByte = 0;
                    NullIndex++;
                }
            }

            if (ix != 0)
            {
                NullStream.Write(currentNullByte);
                NullIndex++;
            }

            DataIndex += col.Count * sizeof(long);
        }

        internal void CopyNullableDateTimeOffsetCollection(ICollection<DateTimeOffset?> col)
        {
            byte currentNullByte = 0;
            var ix = 0;

            foreach (var dt in col)
            {
                if (dt != null)
                {
                    currentNullByte |= (byte)(1 << ix);

                    var dtValue = dt.Value.UtcDateTime;

                    var timeSinceEpoch = dtValue - FeatherMagic.DATETIME_EPOCH;
                    var elapsedTicks = timeSinceEpoch.Ticks;

                    var value = MapToDiskType(elapsedTicks);
                    DataStream.Write(value);
                }
                else
                {
                    DataStream.Write(long.MaxValue);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentNullByte);
                    ix = 0;
                    currentNullByte = 0;
                    NullIndex++;
                }
            }

            if (ix != 0)
            {
                NullStream.Write(currentNullByte);
                NullIndex++;
            }

            DataIndex += col.Count * sizeof(long);
        }

        internal void CopyNullableDateTimeIEnumerable(IEnumerable<DateTime?> col)
        {
            byte currentNullByte = 0;
            var ix = 0;

            foreach (var dt in col)
            {
                if (dt != null)
                {
                    currentNullByte |= (byte)(1 << ix);

                    var dtValue = dt.Value;

                    if (dtValue.Kind != DateTimeKind.Utc)
                    {
                        dtValue = dtValue.ToUniversalTime();
                    }

                    var timeSinceEpoch = dtValue - FeatherMagic.DATETIME_EPOCH;
                    var elapsedTicks = timeSinceEpoch.Ticks;

                    var value = MapToDiskType(elapsedTicks);
                    DataStream.Write(value);
                }
                else
                {
                    DataStream.Write(long.MaxValue);
                }

                DataIndex += sizeof(long);

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentNullByte);
                    ix = 0;
                    currentNullByte = 0;
                    NullIndex++;
                }
            }

            if (ix != 0)
            {
                NullStream.Write(currentNullByte);
                NullIndex++;
            }
        }

        internal void CopyNullableDateTimeOffsetIEnumerable(IEnumerable<DateTimeOffset?> col)
        {
            byte currentNullByte = 0;
            var ix = 0;

            foreach (var dt in col)
            {
                if (dt != null)
                {
                    currentNullByte |= (byte)(1 << ix);

                    var dtValue = dt.Value.UtcDateTime;

                    var timeSinceEpoch = dtValue - FeatherMagic.DATETIME_EPOCH;
                    var elapsedTicks = timeSinceEpoch.Ticks;

                    var value = MapToDiskType(elapsedTicks);
                    DataStream.Write(value);
                }
                else
                {
                    DataStream.Write(long.MaxValue);
                }

                DataIndex += sizeof(long);

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentNullByte);
                    ix = 0;
                    currentNullByte = 0;
                    NullIndex++;
                }
            }

            if (ix != 0)
            {
                NullStream.Write(currentNullByte);
                NullIndex++;
            }
        }

        internal void BlitTimeSpanArray(TimeSpan[] col)
        {
            for (var i = 0; i < col.Length; i++)
            {
                var ts = col[i];
                var ticks = ts.Ticks;

                var value = MapToDiskType(ticks);
                DataStream.Write(value);
            }

            DataIndex += col.LongLength * sizeof(long);
        }

        internal void CopyTimeSpanCollection(ICollection<TimeSpan> col)
        {
            foreach (var ts in col)
            {
                var ticks = ts.Ticks;

                var value = MapToDiskType(ticks);
                DataStream.Write(value);
            }

            DataIndex += col.Count * sizeof(long);
        }

        internal void CopyTimeSpanIEnumerable(IEnumerable<TimeSpan> col)
        {
            foreach (var ts in col)
            {
                var ticks = ts.Ticks;

                var value = MapToDiskType(ticks);
                DataStream.Write(value);
                DataIndex += sizeof(long);
            }
        }

        internal void BlitNullableTimeSpanArray(TimeSpan?[] col)
        {
            byte currentNullByte = 0;
            var ix = 0;

            for (var i = 0; i < col.Length; i++)
            {
                var ts = col[i];
                if (ts != null)
                {
                    currentNullByte |= (byte)(1 << ix);

                    var elapsedTicks = ts.Value.Ticks;

                    var value = MapToDiskType(elapsedTicks);
                    DataStream.Write(value);
                }
                else
                {
                    DataStream.Write(long.MaxValue);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentNullByte);
                    ix = 0;
                    currentNullByte = 0;
                    NullIndex++;
                }
            }

            if (ix != 0)
            {
                NullStream.Write(currentNullByte);
                NullIndex++;
            }

            DataIndex += col.LongLength * sizeof(long);
        }

        internal void CopyNullableTimeSpanCollection(ICollection<TimeSpan?> col)
        {
            byte currentNullByte = 0;
            var ix = 0;

            foreach (var ts in col)
            {
                if (ts != null)
                {
                    currentNullByte |= (byte)(1 << ix);

                    var elapsedTicks = ts.Value.Ticks;

                    var value = MapToDiskType(elapsedTicks);
                    DataStream.Write(value);
                }
                else
                {
                    DataStream.Write(long.MaxValue);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentNullByte);
                    ix = 0;
                    currentNullByte = 0;
                    NullIndex++;
                }
            }

            if (ix != 0)
            {
                NullStream.Write(currentNullByte);
                NullIndex++;
            }

            DataIndex += col.Count * sizeof(long);
        }

        internal void CopyNullableTimeSpanIEnumerable(IEnumerable<TimeSpan?> col)
        {
            byte currentNullByte = 0;
            var ix = 0;

            foreach (var ts in col)
            {
                if (ts != null)
                {
                    currentNullByte |= (byte)(1 << ix);

                    var elapsedTicks = ts.Value.Ticks;

                    var value = MapToDiskType(elapsedTicks);
                    DataStream.Write(value);
                    DataIndex += sizeof(long);
                }
                else
                {
                    DataStream.Write(long.MaxValue);
                    DataIndex += sizeof(long);
                }

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentNullByte);
                    ix = 0;
                    currentNullByte = 0;
                    NullIndex++;
                }
            }

            if (ix != 0)
            {
                NullStream.Write(currentNullByte);
                NullIndex++;
            }
        }

        internal void BlitNonNullableEnumArray<TEnum>(TEnum[] col)
            where TEnum : struct
        {
            var map = EnumDetails.GetLevelIndexLookup(typeof(TEnum));
            var convert = EnumDetails.GetConvertToLongDelegate<TEnum>();

            for (var i = 0; i < col.LongLength; i++)
            {
                var val = col[i];
                var asLong = convert(val);

                int categoryIndex;
                if (!map.TryGetValue(asLong, out categoryIndex))
                {
                    throw new InvalidOperationException($"Found undefined value {val} for enum {typeof(TEnum).Name}");
                }

                DataStream.Write(categoryIndex);
            }

            DataIndex += col.LongLength * sizeof(int);
        }

        internal void CopyNonNullableEnumCollection<TEnum>(ICollection<TEnum> col)
            where TEnum : struct
        {
            var map = EnumDetails.GetLevelIndexLookup(typeof(TEnum));
            var convert = EnumDetails.GetConvertToLongDelegate<TEnum>();

            foreach (var val in col)
            {
                var asLong = convert(val);

                int categoryIndex;
                if (!map.TryGetValue(asLong, out categoryIndex))
                {
                    throw new InvalidOperationException($"Found undefined value {val} for enum {typeof(TEnum).Name}");
                }

                DataStream.Write(categoryIndex);
            }

            DataIndex += col.Count * sizeof(int);
        }

        internal void CopyNonNullableEnumIEnumerable<TEnum>(IEnumerable<TEnum> col)
            where TEnum : struct
        {
            var map = EnumDetails.GetLevelIndexLookup(typeof(TEnum));
            var convert = EnumDetails.GetConvertToLongDelegate<TEnum>();

            foreach (var val in col)
            {
                var asLong = convert(val);

                int categoryIndex;
                if (!map.TryGetValue(asLong, out categoryIndex))
                {
                    throw new InvalidOperationException($"Found undefined value {val} for enum {typeof(TEnum).Name}");
                }

                DataStream.Write(categoryIndex);
                DataIndex += sizeof(int);
            }
        }

        internal void BlitNullableEnumArray<TEnum>(TEnum?[] col)
            where TEnum : struct
        {
            var map = EnumDetails.GetLevelIndexLookup(typeof(TEnum));
            var convert = EnumDetails.GetConvertToLongDelegate<TEnum>();

            byte currentByte = 0;
            var ix = 0;
            for (var i = 0L; i < col.LongLength; i++)
            {
                var val = col[i];
                var b = val != null;

                int asInt;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);

                    var asLong = convert(val.Value);
                    if (!map.TryGetValue(asLong, out asInt))
                    {
                        throw new InvalidOperationException($"Found undefined value {val.Value} for enum {typeof(TEnum).Name}");
                    }
                }
                else
                {
                    asInt = int.MaxValue;
                }

                DataStream.Write(asInt);

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }
            }

            DataIndex += col.LongLength * sizeof(int);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += col.LongLength / 8;
            if (col.LongLength % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableEnumCollection<TEnum>(ICollection<TEnum?> col)
            where TEnum : struct
        {
            var map = EnumDetails.GetLevelIndexLookup(typeof(TEnum));
            var convert = EnumDetails.GetConvertToLongDelegate<TEnum>();

            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                int asInt;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);

                    var asLong = convert(val.Value);
                    if (!map.TryGetValue(asLong, out asInt))
                    {
                        throw new InvalidOperationException($"Found undefined value {val.Value} for enum {typeof(TEnum).Name}");
                    }
                }
                else
                {
                    asInt = int.MaxValue;
                }

                DataStream.Write(asInt);

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                }
            }

            DataIndex += col.Count * sizeof(int);

            if (ix != 0)
            {
                NullStream.Write(currentByte);
            }

            NullIndex += col.Count / 8;
            if (col.Count % 8 != 0)
            {
                NullIndex++;
            }
        }

        internal void CopyNullableEnumIEnumerable<TEnum>(IEnumerable<TEnum?> col)
            where TEnum : struct
        {
            var map = EnumDetails.GetLevelIndexLookup(typeof(TEnum));
            var convert = EnumDetails.GetConvertToLongDelegate<TEnum>();

            byte currentByte = 0;
            var ix = 0;
            foreach (var val in col)
            {
                var b = val != null;

                int asInt;

                if (b)
                {
                    currentByte |= (byte)(1 << ix);

                    var asLong = convert(val.Value);
                    if (!map.TryGetValue(asLong, out asInt))
                    {
                        throw new InvalidOperationException($"Found undefined value {val.Value} for enum {typeof(TEnum).Name}");
                    }
                }
                else
                {
                    asInt = int.MaxValue;
                }

                DataStream.Write(asInt);
                DataIndex += sizeof(int);

                ix++;
                if (ix == 8)
                {
                    NullStream.Write(currentByte);
                    ix = 0;
                    currentByte = 0;
                    NullIndex += sizeof(byte);
                }
            }

            if (ix != 0)
            {
                NullStream.Write(currentByte);
                NullIndex++;
            }
        }
    }
}