using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet.Impl
{
    static class DataWidener
    {
        static readonly Dictionary<Type, Func<IEnumerable, IEnumerable>> WidenerLookup =
            new Dictionary<Type, Func<IEnumerable, IEnumerable>>
            {
                [typeof(string)] = d => WidenToString(d),
                [typeof(double)] = d => WidenToDouble(d),
                [typeof(double?)] = d => WidenToNullableDouble(d),
                [typeof(float)] = d => WidenToFloat(d),
                [typeof(float?)] = d => WidenToNullableFloat(d),
                [typeof(long)] = d => WidenToLong(d),
                [typeof(long?)] = d => WidenToNullableLong(d),
                [typeof(ulong)] = d => WidenToULong(d),
                [typeof(ulong?)] = d => WidenToNullableULong(d),
                [typeof(int)] = d => WidenToInt(d),
                [typeof(int?)] = d => WidenToNullableInt(d),
                [typeof(uint)] = d => WidenToUInt(d),
                [typeof(uint?)] = d => WidenToNullableUInt(d),
                [typeof(short)] = d => WidenToShort(d),
                [typeof(short?)] = d => WidenToNullableShort(d),
                [typeof(ushort)] = d => WidenToUShort(d),
                [typeof(ushort?)] = d => WidenToNullableUShort(d),
                [typeof(sbyte)] = d => WidenToSByte(d),
                [typeof(sbyte?)] = d => WidenToNullableSByte(d),
                [typeof(byte)] = d => WidenToByte(d),
                [typeof(byte?)] = d => WidenToNullableByte(d),
                [typeof(DateTime)] = d => WidenToDateTime(d),
                [typeof(DateTime?)] = d => WidenToNullableDateTime(d),
                [typeof(DateTimeOffset)] = d => WidenToDateTime(d),
                [typeof(DateTimeOffset?)] = d => WidenToNullableDateTime(d),
                [typeof(TimeSpan)] = d => WidenToTimeSpan(d),
                [typeof(TimeSpan?)] = d => WidenToNullableTimeSpan(d)
            };
        
        public static Func<IEnumerable, IEnumerable> GetWidener(Type widestType)
        {
            var nonNull = Nullable.GetUnderlyingType(widestType) ?? widestType;

            Func<IEnumerable, IEnumerable> widener;
            if (!WidenerLookup.TryGetValue(widestType, out widener))
            {
                if(nonNull.IsEnum)
                {
                    return GetEnumWidener(widestType);
                }

                throw new InvalidOperationException($"No widener available for {widestType.Name}");
            }

            return widener;
        }

        static readonly Dictionary<Type, Func<IEnumerable, IEnumerable>> WidenToEnumLookup = new Dictionary<Type, Func<IEnumerable, IEnumerable>>();
        static readonly Dictionary<Type, Func<IEnumerable, IEnumerable>> WidenToNullableEnumLookup = new Dictionary<Type, Func<IEnumerable, IEnumerable>>();

        static Func<IEnumerable, IEnumerable> GetEnumWidener(Type widestType)
        {
            var isNullable = Nullable.GetUnderlyingType(widestType) != null;
            var nonNullable = Nullable.GetUnderlyingType(widestType) ?? widestType;

            if (SyntheticEnum.IsSynthetic(nonNullable))
            {
                return GetSyntheticEnumWidener(widestType);
            }

            if (isNullable)
            {
                // assumed to be a low contention lock
                lock (WidenToNullableEnumLookup)
                {
                    Func<IEnumerable, IEnumerable> ret;
                    if (WidenToNullableEnumLookup.TryGetValue(nonNullable, out ret)) return ret;

                    WidenToNullableEnumLookup[nonNullable] = ret = CreateNullableEnumWidener(nonNullable);

                    return ret;
                }
            }
            else
            {
                // assumed to be a low contention lock
                lock (WidenToEnumLookup)
                {
                    Func<IEnumerable, IEnumerable> ret;
                    if (WidenToEnumLookup.TryGetValue(nonNullable, out ret)) return ret;

                    WidenToEnumLookup[nonNullable] = ret = CreateEnumWidener(nonNullable);

                    return ret;
                }
            }
        }

        static readonly Dictionary<Type, Func<IEnumerable, IEnumerable>> WidenToSyntehticEnumLookup = new Dictionary<Type, Func<IEnumerable, IEnumerable>>();
        static readonly Dictionary<Type, Func<IEnumerable, IEnumerable>> WidenToNullableSyntheticEnumLookup = new Dictionary<Type, Func<IEnumerable, IEnumerable>>();
        static Func<IEnumerable, IEnumerable> GetSyntheticEnumWidener(Type widestType)
        {
            var isNullable = Nullable.GetUnderlyingType(widestType) != null;
            var nonNullable = Nullable.GetUnderlyingType(widestType) ?? widestType;

            if (isNullable)
            {
                // assumed to be a low contention lock
                lock (WidenToNullableSyntheticEnumLookup)
                {
                    Func<IEnumerable, IEnumerable> ret;
                    if (WidenToNullableSyntheticEnumLookup.TryGetValue(nonNullable, out ret)) return ret;

                    WidenToNullableSyntheticEnumLookup[nonNullable] = ret = CreateNullableSyntheticEnumWidener(nonNullable);

                    return ret;
                }
            }
            else
            {
                // assumed to be a low contention lock
                lock (WidenToSyntehticEnumLookup)
                {
                    Func<IEnumerable, IEnumerable> ret;
                    if (WidenToSyntehticEnumLookup.TryGetValue(nonNullable, out ret)) return ret;

                    WidenToSyntehticEnumLookup[nonNullable] = ret = CreateSyntheticEnumWidener(nonNullable);

                    return ret;
                }
            }
        }

        static Func<IEnumerable, IEnumerable> CreateSyntheticEnumWidener(Type enumType)
        {
            var dyn = new DynamicMethod(nameof(CreateSyntheticEnumWidener) + "_" + enumType.FullName, typeof(IEnumerable), new[] { typeof(IEnumerable) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            var widenerGen = typeof(DataWidener).GetMethod(nameof(WidenToSyntheticEnum), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            var widener = widenerGen.MakeGenericMethod(enumType);

            il.Emit(OpCodes.Ldarg_0);           // IEnumerable
            il.Emit(OpCodes.Call, widener);     // IEnumerable<enumType>
            il.Emit(OpCodes.Ret);               // --empty--

            return (Func<IEnumerable, IEnumerable>)dyn.CreateDelegate(typeof(Func<IEnumerable, IEnumerable>));
        }

        static Func<IEnumerable, IEnumerable> CreateNullableSyntheticEnumWidener(Type enumType)
        {
            var dyn = new DynamicMethod(nameof(CreateNullableEnumWidener) + "_" + enumType.FullName, typeof(IEnumerable), new[] { typeof(IEnumerable) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            var widenerGen = typeof(DataWidener).GetMethod(nameof(WidenToNullableSyntheticEnum), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            var widener = widenerGen.MakeGenericMethod(enumType);

            il.Emit(OpCodes.Ldarg_0);           // IEnumerable
            il.Emit(OpCodes.Call, widener);     // IEnumerable<enumType?>
            il.Emit(OpCodes.Ret);               // --empty--

            return (Func<IEnumerable, IEnumerable>)dyn.CreateDelegate(typeof(Func<IEnumerable, IEnumerable>));
        }

        static IEnumerable<TSyntheticEnum> WidenToSyntheticEnum<TSyntheticEnum>(IEnumerable untyped)
            where TSyntheticEnum: struct
        {
            foreach(var val in untyped)
            {
                var asStr = val.ToString();

                yield return (TSyntheticEnum)Enum.Parse(typeof(TSyntheticEnum), asStr, ignoreCase: true);
            }
        }

        static IEnumerable<TSyntheticEnum?> WidenToNullableSyntheticEnum<TSyntheticEnum>(IEnumerable untyped)
           where TSyntheticEnum : struct
        {
            foreach (var val in untyped)
            {
                if(val == null)
                {
                    yield return null;
                    continue;
                }

                var asStr = val.ToString();
                yield return (TSyntheticEnum)Enum.Parse(typeof(TSyntheticEnum), asStr, ignoreCase: true);
            }
        }

        static Func<IEnumerable, IEnumerable> CreateEnumWidener(Type enumType)
        {
            var dyn = new DynamicMethod(nameof(CreateEnumWidener) + "_" + enumType.FullName, typeof(IEnumerable), new[] { typeof(IEnumerable) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            var widenerGen = typeof(DataWidener).GetMethod(nameof(WidenToEnum), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            var widener = widenerGen.MakeGenericMethod(enumType);
            
            il.Emit(OpCodes.Ldarg_0);           // IEnumerable
            il.Emit(OpCodes.Call, widener);     // IEnumerable<enumType>
            il.Emit(OpCodes.Ret);               // --empty--

            return (Func<IEnumerable, IEnumerable>)dyn.CreateDelegate(typeof(Func<IEnumerable, IEnumerable>));
        }

        static Func<IEnumerable, IEnumerable> CreateNullableEnumWidener(Type enumType)
        {
            var dyn = new DynamicMethod(nameof(CreateNullableEnumWidener) + "_" + enumType.FullName, typeof(IEnumerable), new[] { typeof(IEnumerable) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            var widenerGen = typeof(DataWidener).GetMethod(nameof(WidenToNullableEnum), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            var widener = widenerGen.MakeGenericMethod(enumType);

            il.Emit(OpCodes.Ldarg_0);           // IEnumerable
            il.Emit(OpCodes.Call, widener);     // IEnumerable<enumType?>
            il.Emit(OpCodes.Ret);               // --empty--

            return (Func<IEnumerable, IEnumerable>)dyn.CreateDelegate(typeof(Func<IEnumerable, IEnumerable>));
        }

        static IEnumerable<TEnum> WidenToEnum<TEnum>(IEnumerable untyped)
            where TEnum : struct
        {
            foreach (var val in untyped)
            {
                yield return (TEnum)Enum.Parse(typeof(TEnum), Enum.GetName(typeof(TEnum), val));
            }
        }

        static IEnumerable<TEnum?> WidenToNullableEnum<TEnum>(IEnumerable untyped)
           where TEnum : struct
        {
            foreach (var val in untyped)
            {
                if(val == null)
                {
                    yield return null;
                    continue;
                }

                yield return (TEnum)Enum.Parse(typeof(TEnum), Enum.GetName(typeof(TEnum), val));
            }
        }

        static IEnumerable<string> WidenToString(IEnumerable untyped)
        {
            foreach(var val in untyped)
            {
                if(val == null)
                {
                    yield return null;
                    continue;
                }

                if(val is string)
                {
                    yield return (string)val;
                    continue;
                }

                if(val is DateTime)
                {
                    yield return ((DateTime)val).ToString("u");
                    continue;
                }

                if (val is DateTime?)
                {
                    yield return ((DateTime?)val).Value.ToString("u");
                    continue;
                }

                if (val is DateTimeOffset)
                {
                    yield return ((DateTimeOffset)val).ToString("u");
                    continue;
                }

                if (val is DateTimeOffset?)
                {
                    yield return ((DateTimeOffset?)val).Value.ToString("u");
                    continue;
                }

                if (val is TimeSpan)
                {
                    yield return ((TimeSpan)val).ToString("c");
                    continue;
                }

                if (val is TimeSpan?)
                {
                    yield return ((TimeSpan?)val).Value.ToString("c");
                    continue;
                }

                if(val is double)
                {
                    yield return ((double)val).ToString("R");
                    continue;
                }

                if (val is double?)
                {
                    yield return ((double?)val).Value.ToString("R");
                    continue;
                }

                if (val is float)
                {
                    yield return ((float)val).ToString("R");
                    continue;
                }

                if (val is float?)
                {
                    yield return ((float?)val).Value.ToString("R");
                    continue;
                }

                if(val is long)
                {
                    yield return ((long)val).ToString();
                    continue;
                }

                if (val is long?)
                {
                    yield return ((long?)val).Value.ToString();
                    continue;
                }

                if (val is ulong)
                {
                    yield return ((ulong)val).ToString();
                    continue;
                }

                if (val is ulong?)
                {
                    yield return ((ulong?)val).Value.ToString();
                    continue;
                }

                if (val is int)
                {
                    yield return ((int)val).ToString();
                    continue;
                }

                if (val is int?)
                {
                    yield return ((int?)val).Value.ToString();
                    continue;
                }

                if (val is uint)
                {
                    yield return ((uint)val).ToString();
                    continue;
                }

                if (val is uint?)
                {
                    yield return ((uint?)val).Value.ToString();
                    continue;
                }

                if (val is short)
                {
                    yield return ((short)val).ToString();
                    continue;
                }

                if (val is short?)
                {
                    yield return ((short?)val).Value.ToString();
                    continue;
                }

                if (val is ushort)
                {
                    yield return ((ushort)val).ToString();
                    continue;
                }

                if (val is ushort?)
                {
                    yield return ((ushort?)val).Value.ToString();
                    continue;
                }

                if (val is sbyte)
                {
                    yield return ((sbyte)val).ToString();
                    continue;
                }

                if (val is sbyte?)
                {
                    yield return ((sbyte?)val).Value.ToString();
                    continue;
                }

                if (val is byte)
                {
                    yield return ((byte)val).ToString();
                    continue;
                }

                if (val is byte?)
                {
                    yield return ((byte?)val).Value.ToString();
                    continue;
                }

                // assuming this is an enum
                yield return val.ToString(); 
            }
        }

        static IEnumerable<TimeSpan> WidenToTimeSpan(IEnumerable untyped)
        {
            foreach(var val in untyped)
            {
                if(val is TimeSpan)
                {
                    yield return (TimeSpan)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to TimeSpan");
            }
        }

        static IEnumerable<TimeSpan?> WidenToNullableTimeSpan(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val == null)
                {
                    yield return null;
                    continue;
                }

                if (val is TimeSpan)
                {
                    yield return (TimeSpan)val;
                    continue;
                }

                if(val is TimeSpan?)
                {
                    yield return (TimeSpan?)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to TimeSpan");
            }
        }

        static IEnumerable<DateTime> WidenToDateTime(IEnumerable untyped)
        {
            foreach(var val in untyped)
            {
                if(val is DateTime)
                {
                    yield return (DateTime)val;
                    continue;
                }

                if(val is DateTimeOffset)
                {
                    yield return ((DateTimeOffset)val).UtcDateTime;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to DateTime");
            }
        }

        static IEnumerable<DateTime?> WidenToNullableDateTime(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val == null)
                {
                    yield return null;
                    continue;
                }

                if (val is DateTime)
                {
                    yield return (DateTime)val;
                    continue;
                }

                if (val is DateTime?)
                {
                    yield return (DateTime?)val;
                    continue;
                }

                if (val is DateTimeOffset)
                {
                    yield return ((DateTimeOffset)val).UtcDateTime;
                    continue;
                }

                if (val is DateTimeOffset?)
                {
                    yield return ((DateTimeOffset?)val)?.UtcDateTime;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to DateTime?");
            }
        }

        static IEnumerable<double> WidenToDouble(IEnumerable untyped)
        {
            foreach(var val in untyped)
            {
                if(val is double)
                {
                    yield return (double)val;
                    continue;
                }

                if(val is float)
                {
                    yield return (float)val;
                    continue;
                }

                if (val is long)
                {
                    yield return (long)val;
                    continue;
                }

                if (val is ulong)
                {
                    yield return (ulong)val;
                    continue;
                }

                if (val is int)
                {
                    yield return (int)val;
                    continue;
                }

                if (val is uint)
                {
                    yield return (uint)val;
                    continue;
                }

                if (val is short)
                {
                    yield return (short)val;
                    continue;
                }

                if (val is ushort)
                {
                    yield return (ushort)val;
                    continue;
                }

                if (val is byte)
                {
                    yield return (byte)val;
                    continue;
                }

                if (val is sbyte)
                {
                    yield return (sbyte)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to double");
            }
        }

        static IEnumerable<double?> WidenToNullableDouble(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if(val == null)
                {
                    yield return null;
                    continue;
                }

                if (val is double)
                {
                    yield return (double)val;
                    continue;
                }

                if(val is double?)
                {
                    yield return (double?)val;
                    continue;
                }

                if (val is float)
                {
                    yield return (float)val;
                    continue;
                }

                if (val is float?)
                {
                    yield return (float?)val;
                    continue;
                }

                if (val is long)
                {
                    yield return (long)val;
                    continue;
                }

                if (val is long?)
                {
                    yield return (long?)val;
                    continue;
                }

                if (val is ulong)
                {
                    yield return (ulong)val;
                    continue;
                }

                if (val is ulong?)
                {
                    yield return (ulong?)val;
                    continue;
                }

                if (val is int)
                {
                    yield return (int)val;
                    continue;
                }

                if (val is int?)
                {
                    yield return (int?)val;
                    continue;
                }

                if (val is uint)
                {
                    yield return (uint)val;
                    continue;
                }

                if (val is uint?)
                {
                    yield return (uint?)val;
                    continue;
                }

                if (val is short)
                {
                    yield return (short)val;
                    continue;
                }

                if (val is short?)
                {
                    yield return (short?)val;
                    continue;
                }

                if (val is ushort)
                {
                    yield return (ushort)val;
                    continue;
                }

                if (val is ushort?)
                {
                    yield return (ushort?)val;
                    continue;
                }

                if (val is byte)
                {
                    yield return (byte)val;
                    continue;
                }

                if (val is byte?)
                {
                    yield return (byte?)val;
                    continue;
                }

                if (val is sbyte)
                {
                    yield return (sbyte)val;
                    continue;
                }

                if (val is sbyte?)
                {
                    yield return (sbyte?)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to double?");
            }
        }

        static IEnumerable<float> WidenToFloat(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val is float)
                {
                    yield return (float)val;
                    continue;
                }

                if (val is long)
                {
                    yield return (long)val;
                    continue;
                }

                if (val is ulong)
                {
                    yield return (ulong)val;
                    continue;
                }

                if (val is int)
                {
                    yield return (int)val;
                    continue;
                }

                if (val is uint)
                {
                    yield return (uint)val;
                    continue;
                }

                if (val is short)
                {
                    yield return (short)val;
                    continue;
                }

                if (val is ushort)
                {
                    yield return (ushort)val;
                    continue;
                }

                if (val is byte)
                {
                    yield return (byte)val;
                    continue;
                }

                if (val is sbyte)
                {
                    yield return (sbyte)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to float");
            }
        }

        static IEnumerable<float?> WidenToNullableFloat(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val == null)
                {
                    yield return null;
                    continue;
                }

                if (val is float)
                {
                    yield return (float)val;
                    continue;
                }

                if (val is float?)
                {
                    yield return (float?)val;
                    continue;
                }

                if (val is long)
                {
                    yield return (long)val;
                    continue;
                }

                if (val is long?)
                {
                    yield return (long?)val;
                    continue;
                }

                if (val is ulong)
                {
                    yield return (ulong)val;
                    continue;
                }

                if (val is ulong?)
                {
                    yield return (ulong?)val;
                    continue;
                }

                if (val is int)
                {
                    yield return (int)val;
                    continue;
                }

                if (val is int?)
                {
                    yield return (int?)val;
                    continue;
                }

                if (val is uint)
                {
                    yield return (uint)val;
                    continue;
                }

                if (val is uint?)
                {
                    yield return (uint?)val;
                    continue;
                }

                if (val is short)
                {
                    yield return (short)val;
                    continue;
                }

                if (val is short?)
                {
                    yield return (short?)val;
                    continue;
                }

                if (val is ushort)
                {
                    yield return (ushort)val;
                    continue;
                }

                if (val is ushort?)
                {
                    yield return (ushort?)val;
                    continue;
                }

                if (val is byte)
                {
                    yield return (byte)val;
                    continue;
                }

                if (val is byte?)
                {
                    yield return (byte?)val;
                    continue;
                }

                if (val is sbyte)
                {
                    yield return (sbyte)val;
                    continue;
                }

                if (val is sbyte?)
                {
                    yield return (sbyte?)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to float?");
            }
        }

        static IEnumerable<long> WidenToLong(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val is long)
                {
                    yield return (long)val;
                    continue;
                }
                
                if (val is int)
                {
                    yield return (int)val;
                    continue;
                }
                
                if (val is short)
                {
                    yield return (short)val;
                    continue;
                }
                
                if (val is sbyte)
                {
                    yield return (sbyte)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to long");
            }
        }

        static IEnumerable<long?> WidenToNullableLong(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val == null)
                {
                    yield return null;
                    continue;
                }

                if (val is long)
                {
                    yield return (long)val;
                    continue;
                }

                if (val is long?)
                {
                    yield return (long?)val;
                    continue;
                }
                
                if (val is int)
                {
                    yield return (int)val;
                    continue;
                }

                if (val is int?)
                {
                    yield return (int?)val;
                    continue;
                }

                if (val is short)
                {
                    yield return (short)val;
                    continue;
                }

                if (val is short?)
                {
                    yield return (short?)val;
                    continue;
                }

                if (val is sbyte)
                {
                    yield return (sbyte)val;
                    continue;
                }

                if (val is sbyte?)
                {
                    yield return (sbyte?)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to long?");
            }
        }
        
        static IEnumerable<ulong> WidenToULong(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val is ulong)
                {
                    yield return (ulong)val;
                    continue;
                }

                if (val is uint)
                {
                    yield return (uint)val;
                    continue;
                }

                if (val is ushort)
                {
                    yield return (ushort)val;
                    continue;
                }

                if (val is byte)
                {
                    yield return (byte)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to ulong");
            }
        }

        static IEnumerable<ulong?> WidenToNullableULong(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val == null)
                {
                    yield return null;
                    continue;
                }
                
                if (val is ulong)
                {
                    yield return (ulong)val;
                    continue;
                }

                if (val is ulong?)
                {
                    yield return (ulong?)val;
                    continue;
                }
                
                if (val is uint)
                {
                    yield return (uint)val;
                    continue;
                }

                if (val is uint?)
                {
                    yield return (uint?)val;
                    continue;
                }
                
                if (val is ushort)
                {
                    yield return (ushort)val;
                    continue;
                }

                if (val is ushort?)
                {
                    yield return (ushort?)val;
                    continue;
                }

                if (val is byte)
                {
                    yield return (byte)val;
                    continue;
                }

                if (val is byte?)
                {
                    yield return (byte?)val;
                    continue;
                }
                
                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to ulong?");
            }
        }

        static IEnumerable<int> WidenToInt(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val is int)
                {
                    yield return (int)val;
                    continue;
                }

                if (val is short)
                {
                    yield return (short)val;
                    continue;
                }

                if (val is sbyte)
                {
                    yield return (sbyte)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to int");
            }
        }

        static IEnumerable<int?> WidenToNullableInt(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val == null)
                {
                    yield return null;
                    continue;
                }
                
                if (val is int)
                {
                    yield return (int)val;
                    continue;
                }

                if (val is int?)
                {
                    yield return (int?)val;
                    continue;
                }

                if (val is short)
                {
                    yield return (short)val;
                    continue;
                }

                if (val is short?)
                {
                    yield return (short?)val;
                    continue;
                }

                if (val is sbyte)
                {
                    yield return (sbyte)val;
                    continue;
                }

                if (val is sbyte?)
                {
                    yield return (sbyte?)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to int?");
            }
        }

        static IEnumerable<uint> WidenToUInt(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val is uint)
                {
                    yield return (uint)val;
                    continue;
                }

                if (val is ushort)
                {
                    yield return (ushort)val;
                    continue;
                }

                if (val is byte)
                {
                    yield return (byte)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to uint");
            }
        }

        static IEnumerable<uint?> WidenToNullableUInt(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val == null)
                {
                    yield return null;
                    continue;
                }
                
                if (val is uint)
                {
                    yield return (uint)val;
                    continue;
                }

                if (val is uint?)
                {
                    yield return (uint?)val;
                    continue;
                }

                if (val is ushort)
                {
                    yield return (ushort)val;
                    continue;
                }

                if (val is ushort?)
                {
                    yield return (ushort?)val;
                    continue;
                }

                if (val is byte)
                {
                    yield return (byte)val;
                    continue;
                }

                if (val is byte?)
                {
                    yield return (byte?)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to uint?");
            }
        }

        static IEnumerable<short> WidenToShort(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val is short)
                {
                    yield return (short)val;
                    continue;
                }

                if (val is sbyte)
                {
                    yield return (sbyte)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to short");
            }
        }

        static IEnumerable<short?> WidenToNullableShort(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val == null)
                {
                    yield return null;
                    continue;
                }
                
                if (val is short)
                {
                    yield return (short)val;
                    continue;
                }

                if (val is short?)
                {
                    yield return (short?)val;
                    continue;
                }

                if (val is sbyte)
                {
                    yield return (sbyte)val;
                    continue;
                }

                if (val is sbyte?)
                {
                    yield return (sbyte?)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to short?");
            }
        }

        static IEnumerable<ushort> WidenToUShort(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val is ushort)
                {
                    yield return (ushort)val;
                    continue;
                }

                if (val is byte)
                {
                    yield return (byte)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to uint");
            }
        }

        static IEnumerable<ushort?> WidenToNullableUShort(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val == null)
                {
                    yield return null;
                    continue;
                }

                if (val is ushort)
                {
                    yield return (ushort)val;
                    continue;
                }

                if (val is ushort?)
                {
                    yield return (ushort?)val;
                    continue;
                }

                if (val is byte)
                {
                    yield return (byte)val;
                    continue;
                }

                if (val is byte?)
                {
                    yield return (byte?)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to ushort?");
            }
        }

        static IEnumerable<sbyte> WidenToSByte(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val is sbyte)
                {
                    yield return (sbyte)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to sbyte");
            }
        }

        static IEnumerable<sbyte?> WidenToNullableSByte(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val == null)
                {
                    yield return null;
                    continue;
                }

                if (val is sbyte)
                {
                    yield return (sbyte)val;
                    continue;
                }

                if (val is sbyte?)
                {
                    yield return (sbyte?)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to sbyte?");
            }
        }

        static IEnumerable<byte> WidenToByte(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val is byte)
                {
                    yield return (byte)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to byte");
            }
        }

        static IEnumerable<byte?> WidenToNullableByte(IEnumerable untyped)
        {
            foreach (var val in untyped)
            {
                if (val == null)
                {
                    yield return null;
                    continue;
                }

                if (val is byte)
                {
                    yield return (byte)val;
                    continue;
                }

                if (val is byte?)
                {
                    yield return (byte?)val;
                    continue;
                }

                throw new InvalidOperationException($"Tried to widen {val?.GetType()?.Name ?? "--NULL--"} to byte?");
            }
        }
    }
}
