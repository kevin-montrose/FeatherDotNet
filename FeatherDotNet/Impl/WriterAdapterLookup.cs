using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet.Impl
{
    static class WriterAdapterLookup
    {
        struct AdapterKey : IEquatable<AdapterKey>
        {
            public Type FromType { get; set; }
            public ColumnType ToType { get; set; }

            public override bool Equals(object obj)
            {
                if (!(obj is AdapterKey)) return false;

                return Equals((AdapterKey)obj);
            }

            public override int GetHashCode()
            {
                return
                    (FromType.GetHashCode() * 17) +
                    ToType.GetHashCode();
            }

            public bool Equals(AdapterKey other)
            {
                return
                    other.FromType == FromType &&
                    other.ToType == ToType;
            }
        }

        public class WriterMethodLookup
        {
            string NonNullBoolMethodName;
            string NonNullDateTimeMethodName;
            string NonNullDateTimeOffsetMethodName;
            string NonNullTimeSpanMethodName;
            string NonNullCollectionMethodName;
            string NonNullStringMethodName;
            string NonNullEnumMethodName;

            string NullBoolMethodName;
            string NullDateTimeMethodName;
            string NullDateTimeOffsetMethodName;
            string NullTimeSpanMethodName;
            string NullCollectionMethodName;
            string NullStringMethodName;
            string NullEnumMethodName;

            public WriterMethodLookup(
                string nonNullBoolMethod, 
                string nonNullDateTimeMethod,
                string nonNullDateTimeOffsetMethod,
                string nonNullTimeSpanMethod,
                string nonNullCollectionMethod,
                string nonNullStringMethod,
                string nonNullEnumMethod,

                string nullBoolMethod,
                string nullDateTimeMethod,
                string nullDateTimeOffsetMethod,
                string nullTimeSpanMethod,
                string nullCollectionMethod,
                string nullStringMethod,
                string nullEnumMethod
            )
            {
                NonNullBoolMethodName = nonNullBoolMethod;
                NonNullDateTimeMethodName = nonNullDateTimeMethod;
                NonNullDateTimeOffsetMethodName = nonNullDateTimeOffsetMethod;
                NonNullTimeSpanMethodName = nonNullTimeSpanMethod;
                NonNullCollectionMethodName = nonNullCollectionMethod;
                NonNullStringMethodName = nonNullStringMethod;
                NonNullEnumMethodName = nonNullEnumMethod;

                NullBoolMethodName = nullBoolMethod;
                NullDateTimeMethodName = nullDateTimeMethod;
                NullDateTimeOffsetMethodName = nullDateTimeOffsetMethod;
                NullTimeSpanMethodName = nullTimeSpanMethod;
                NullCollectionMethodName = nullCollectionMethod;
                NullStringMethodName = nullStringMethod;
                NullEnumMethodName = nullEnumMethod;
            }

            public MethodInfo GetBlitterMethod(Type elementType, Type columnEnumerableType, ColumnType toType)
            {
                MethodInfo blit;

                if (elementType.IsValueType)
                {
                    if (Nullable.GetUnderlyingType(elementType) == null)
                    {
                        // non-nullable primitives

                        if (elementType == typeof(bool))
                        {
                            blit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NonNullBoolMethodName);
                        }
                        else
                        {
                            if (elementType == typeof(TimeSpan))
                            {
                                blit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NonNullTimeSpanMethodName);
                            }
                            else
                            {
                                if (elementType == typeof(DateTime))
                                {
                                    blit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NonNullDateTimeMethodName);
                                }
                                else
                                {
                                    if (elementType == typeof(DateTimeOffset))
                                    {
                                        blit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NonNullDateTimeOffsetMethodName);
                                    }
                                    else
                                    {
                                        if (elementType.IsEnum)
                                        {
                                            var genBlit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NonNullEnumMethodName);
                                            blit = genBlit.MakeGenericMethod(elementType);
                                        }
                                        else
                                        {
                                            blit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NonNullCollectionMethodName && d.GetParameters()[0].ParameterType == columnEnumerableType);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        // nullable primitives
                        if (elementType == typeof(bool?))
                        {
                            blit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NullBoolMethodName);
                        }
                        else
                        {
                            if (elementType == typeof(TimeSpan?))
                            {
                                blit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NullTimeSpanMethodName);
                            }
                            else
                            {
                                if (elementType == typeof(DateTime?))
                                {
                                    blit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NullDateTimeMethodName);
                                }
                                else
                                {
                                    if (elementType == typeof(DateTimeOffset?))
                                    {
                                        blit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NullDateTimeOffsetMethodName);
                                    }
                                    else
                                    {
                                        if (Nullable.GetUnderlyingType(elementType).IsEnum)
                                        {
                                            var genBlit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NullEnumMethodName);
                                            blit = genBlit.MakeGenericMethod(Nullable.GetUnderlyingType(elementType));
                                        }
                                        else
                                        {
                                            blit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NullCollectionMethodName && d.GetParameters()[0].ParameterType == columnEnumerableType);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                else
                {
                    if (elementType == typeof(string))
                    {
                        if (toType == ColumnType.String)
                        {
                            blit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NonNullStringMethodName);
                        }
                        else
                        {
                            if (toType == ColumnType.NullableString)
                            {
                                blit = typeof(FeatherWriter).GetMethods(BindingFlags.NonPublic | BindingFlags.Instance).Single(d => d.Name == NullStringMethodName && d.GetParameters()[0].ParameterType == columnEnumerableType);
                            }
                            else
                            {
                                throw new InvalidOperationException($"Unexpected request for a blit method from a string ({elementType.Name}, {columnEnumerableType.Name}, {toType}");
                            }
                        }
                    }
                    else
                    {
                        if(elementType == typeof(Enum))
                        {
                            throw new NotImplementedException();
                        }
                        else
                        {
                            throw new InvalidOperationException($"Unexpected request for a blit method from a nullable type ({elementType.Name}, {columnEnumerableType.Name}, {toType}");
                        }
                    }
                }

                return blit;
            }
        }

        public static Action<FeatherWriter, System.Collections.IEnumerable> LookupAdapter(Type dataSource, Type elementType, ColumnType toType)
        {
            var interfaces = dataSource.GetInterfaces();
            var isArray = dataSource.IsArray;
            var iCollection = interfaces.FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICollection<>));
            var iEnumerable = interfaces.FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));

            if (isArray && dataSource.GetElementType() == elementType)
            {
                return LookupArrayAdapter(elementType, toType);
            }

            if (iCollection != null && iCollection.GetGenericArguments()[0] == elementType)
            {
                return LookupCollectionAdapter(elementType, toType);
            }

            if (iEnumerable != null && iEnumerable.GetGenericArguments()[0] == elementType)
            {
                return LookupEnumerableAdapter(elementType, toType);
            }

            // life just got interesting, need to do some work per-element
            return LookupDefaultAdapter(elementType, toType);
        }

        static readonly Dictionary<AdapterKey, Action<FeatherWriter, System.Collections.IEnumerable>> ArrayAdapters = new Dictionary<AdapterKey, Action<FeatherWriter, System.Collections.IEnumerable>>();
        static Action<FeatherWriter, System.Collections.IEnumerable> LookupArrayAdapter(Type elementType, ColumnType toType)
        {
            var key = new AdapterKey { FromType = elementType, ToType = toType };

            // making an assumption that actually using the adapter
            //   dominates creating it, making this lock acceptable
            lock (ArrayAdapters)
            {
                Action<FeatherWriter, System.Collections.IEnumerable> ret;
                if (ArrayAdapters.TryGetValue(key, out ret)) return ret;

                ArrayAdapters[key] = ret = ArrayAdapter.Create(elementType, toType);

                return ret;
            }
        }

        static readonly Dictionary<AdapterKey, Action<FeatherWriter, System.Collections.IEnumerable>> CollectionAdapters = new Dictionary<AdapterKey, Action<FeatherWriter, System.Collections.IEnumerable>>();
        static Action<FeatherWriter, System.Collections.IEnumerable> LookupCollectionAdapter(Type elementType, ColumnType toType)
        {
            var key = new AdapterKey { FromType = elementType, ToType = toType };

            // making an assumption that actually using the adapter
            //   dominates creating it, making this lock acceptable
            lock (CollectionAdapters)
            {
                Action<FeatherWriter, System.Collections.IEnumerable> ret;
                if (CollectionAdapters.TryGetValue(key, out ret)) return ret;

                CollectionAdapters[key] = ret = CollectionAdapter.Create(elementType, toType);

                return ret;
            }
        }

        static readonly Dictionary<AdapterKey, Action<FeatherWriter, System.Collections.IEnumerable>> EnumerableAdapters = new Dictionary<AdapterKey, Action<FeatherWriter, System.Collections.IEnumerable>>();
        static Action<FeatherWriter, System.Collections.IEnumerable> LookupEnumerableAdapter(Type elementType, ColumnType toType)
        {
            var key = new AdapterKey { FromType = elementType, ToType = toType };

            // making an assumption that actually using the adapter
            //   dominates creating it, making this lock acceptable
            lock (EnumerableAdapters)
            {
                Action<FeatherWriter, System.Collections.IEnumerable> ret;
                if (EnumerableAdapters.TryGetValue(key, out ret)) return ret;

                EnumerableAdapters[key] = ret = EnumerableAdapter.Create(elementType, toType);

                return ret;
            }
        }

        static readonly Dictionary<AdapterKey, Action<FeatherWriter, System.Collections.IEnumerable>> UntypedEnumerableAdapters = new Dictionary<AdapterKey, Action<FeatherWriter, System.Collections.IEnumerable>>();
        static Action<FeatherWriter, System.Collections.IEnumerable> LookupDefaultAdapter(Type elementType, ColumnType toType)
        {
            var key = new AdapterKey { FromType = elementType, ToType = toType };

            lock (UntypedEnumerableAdapters)
            {
                Action<FeatherWriter, System.Collections.IEnumerable> ret;
                if (UntypedEnumerableAdapters.TryGetValue(key, out ret)) return ret;

                var widener = DataWidener.GetWidener(elementType);

                var typedAdapter = LookupEnumerableAdapter(elementType, toType);

                UntypedEnumerableAdapters[key] = ret =
                    (writer, data) =>
                    {
                        var widend = widener(data);
                        typedAdapter(writer, widend);
                    };

                return ret;
            }
        }
    }

    static class ArrayAdapter
    {
        static readonly WriterAdapterLookup.WriterMethodLookup Lookup =
            new WriterAdapterLookup.WriterMethodLookup(
                nameof(FeatherWriter.BlitNonNullableBoolArray),
                nameof(FeatherWriter.BlitDateTimeArray),
                nameof(FeatherWriter.BlitDateTimeOffsetArray),
                nameof(FeatherWriter.BlitTimeSpanArray),
                nameof(FeatherWriter.BlitNonNullableArray),
                nameof(FeatherWriter.CopyStringArray),
                nameof(FeatherWriter.BlitNonNullableEnumArray),

                nameof(FeatherWriter.BlitNullableBoolArray),
                nameof(FeatherWriter.BlitNullableDateTimeArray),
                nameof(FeatherWriter.BlitNullableDateTimeOffsetArray),
                nameof(FeatherWriter.BlitNullableTimeSpanArray),
                nameof(FeatherWriter.BlitNullableArray),
                nameof(FeatherWriter.CopyNullableStringArray),
                nameof(FeatherWriter.BlitNullableEnumArray)
            );

        public static Action<FeatherWriter, System.Collections.IEnumerable> Create(Type elementType, ColumnType toType)
        {
            var dyn = new DynamicMethod("ArrayAdapter_" + elementType.Name + "_" + toType, null, new[] { typeof(FeatherWriter), typeof(System.Collections.IEnumerable) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            var arrType = elementType.MakeArrayType();
            var blit = Lookup.GetBlitterMethod(elementType, arrType, toType);

            var arr = il.DeclareLocal(arrType);

            il.Emit(OpCodes.Ldarg_1);                               // IEnumerable
            il.Emit(OpCodes.Castclass, arrType);                    // elementType[]
            il.Emit(OpCodes.Stloc, arr);                            // --empty--

            il.Emit(OpCodes.Ldarg_0);                               // FeatherWriter
            il.Emit(OpCodes.Ldloc, arr);                            // FeatherWriter elementType[]
            il.Emit(OpCodes.Call, blit);                            // --empty--
            il.Emit(OpCodes.Ret);                                   // --empty--

            var ret = (Action<FeatherWriter, System.Collections.IEnumerable>)dyn.CreateDelegate(typeof(Action<FeatherWriter, System.Collections.IEnumerable>));
            return ret;
        }
    }

    static class CollectionAdapter
    {
        static readonly WriterAdapterLookup.WriterMethodLookup Lookup =
            new WriterAdapterLookup.WriterMethodLookup(
                nameof(FeatherWriter.CopyNonNullableBoolCollection),
                nameof(FeatherWriter.CopyDateTimeCollection),
                nameof(FeatherWriter.CopyDateTimeOffsetCollection),
                nameof(FeatherWriter.CopyTimeSpanCollection),
                nameof(FeatherWriter.CopyNonNullableCollection),
                nameof(FeatherWriter.CopyStringCollection),
                nameof(FeatherWriter.CopyNonNullableEnumCollection),

                nameof(FeatherWriter.CopyNullableBoolCollection),
                nameof(FeatherWriter.CopyNullableDateTimeCollection),
                nameof(FeatherWriter.CopyNullableDateTimeOffsetCollection),
                nameof(FeatherWriter.CopyNullableTimeSpanCollection),
                nameof(FeatherWriter.CopyNullableCollection),
                nameof(FeatherWriter.CopyNullableStringCollection),
                nameof(FeatherWriter.CopyNullableEnumCollection)
            );

        public static Action<FeatherWriter, System.Collections.IEnumerable> Create(Type elementType, ColumnType toType)
        {
            var dyn = new DynamicMethod("CollectionAdapter_" + elementType.Name + "_" + toType, null, new[] { typeof(FeatherWriter), typeof(System.Collections.IEnumerable) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            var collectionType = typeof(ICollection<>).MakeGenericType(elementType);

            var arr = il.DeclareLocal(collectionType);

            il.Emit(OpCodes.Ldarg_1);                                                   // IEnumerable
            il.Emit(OpCodes.Castclass, collectionType);                                 // ICollection<elementType>
            il.Emit(OpCodes.Stloc, arr);                                                // --empty--

            var blit = Lookup.GetBlitterMethod(elementType, collectionType, toType);

            il.Emit(OpCodes.Ldarg_0);                                                   // FeatherWriter
            il.Emit(OpCodes.Ldloc, arr);                                                // FeatherWriter ICollection<elementType>
            il.Emit(OpCodes.Call, blit);                                                // --empty--
            il.Emit(OpCodes.Ret);                                                       // --empty--
            
            var ret = (Action<FeatherWriter, System.Collections.IEnumerable>)dyn.CreateDelegate(typeof(Action<FeatherWriter, System.Collections.IEnumerable>));
            return ret;
        }
    }

    static class EnumerableAdapter
    {
        static readonly WriterAdapterLookup.WriterMethodLookup Lookup =
            new WriterAdapterLookup.WriterMethodLookup(
                nameof(FeatherWriter.CopyNonNullableBoolIEnumerable),
                nameof(FeatherWriter.CopyDateTimeIEnumerable),
                nameof(FeatherWriter.CopyDateTimeOffsetIEnumerable),
                nameof(FeatherWriter.CopyTimeSpanIEnumerable),
                nameof(FeatherWriter.CopyNonNullableIEnumerable),
                nameof(FeatherWriter.CopyStringIEnumerable),
                nameof(FeatherWriter.CopyNonNullableEnumIEnumerable),

                nameof(FeatherWriter.CopyNullableBoolIEnumerable),
                nameof(FeatherWriter.CopyNullableDateTimeIEnumerable),
                nameof(FeatherWriter.CopyNullableDateTimeOffsetIEnumerable),
                nameof(FeatherWriter.CopyNullableTimeSpanIEnumerable),
                nameof(FeatherWriter.CopyNullableIEnumerable),
                nameof(FeatherWriter.CopyNullableStringIEnumerable),
                nameof(FeatherWriter.CopyNullableEnumIEnumerable)
            );

        public static Action<FeatherWriter, System.Collections.IEnumerable> Create(Type elementType, ColumnType toType)
        {
            var dyn = new DynamicMethod("CollectionAdapter_" + elementType.Name + "_" + toType, null, new[] { typeof(FeatherWriter), typeof(System.Collections.IEnumerable) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            var collectionType = typeof(IEnumerable<>).MakeGenericType(elementType);

            var blit = Lookup.GetBlitterMethod(elementType, collectionType, toType);

            var arr = il.DeclareLocal(collectionType);

            il.Emit(OpCodes.Ldarg_1);                                   // IEnumerable
            il.Emit(OpCodes.Castclass, collectionType);                 // IEnumerable<elementType>
            il.Emit(OpCodes.Stloc, arr);                                // --empty--

            il.Emit(OpCodes.Ldarg_0);                                   // FeatherWriter
            il.Emit(OpCodes.Ldloc, arr);                                // FeatherWriter IEnumerable<elementType>
            il.Emit(OpCodes.Call, blit);                                // --empty--
            il.Emit(OpCodes.Ret);                                       // --empty--

            var ret = (Action<FeatherWriter, System.Collections.IEnumerable>)dyn.CreateDelegate(typeof(Action<FeatherWriter, System.Collections.IEnumerable>));
            return ret;
        }
    }
}
