using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet.Impl
{
    static class EnumDetails
    {
        static readonly Dictionary<Type, string[]> LevelsLookup = new Dictionary<Type, string[]>();
        static readonly Dictionary<Type, Dictionary<long, int>> LevelIndexLookupLookup = new Dictionary<Type, Dictionary<long, int>>();
        static readonly Dictionary<Type, Delegate> ConversionLookup = new Dictionary<Type, Delegate>();

        public static string[] GetLevels(Type enumType)
        {
            // should be a low contention lock
            lock (LevelsLookup)
            {
                string[] levels;
                if(!LevelsLookup.TryGetValue(enumType, out levels))
                {
                    LevelsLookup[enumType] = levels = LoadLevels(enumType);
                }

                return levels;
            }
        }

        public static Dictionary<long, int> GetLevelIndexLookup(Type enumType)
        {
            // should be a low contention lock
            lock (LevelIndexLookupLookup)
            {
                Dictionary<long, int> levelIndexLookup;
                if(!LevelIndexLookupLookup.TryGetValue(enumType, out levelIndexLookup))
                {
                    LevelIndexLookupLookup[enumType] = levelIndexLookup = MakeLevelLookup(enumType);
                }

                return levelIndexLookup;
            }
        }

        public static Func<TEnum, long> GetConvertToLongDelegate<TEnum>() 
            where TEnum: struct
        {
            // should be a low contention lock
            lock (ConversionLookup)
            {
                Delegate convert;
                if(!ConversionLookup.TryGetValue(typeof(TEnum), out convert))
                {
                    ConversionLookup[typeof(TEnum)] = convert = MakeConvertToLongDelegate<TEnum>();
                }

                return (Func<TEnum, long>)convert;
            }
        }

        static string[] LoadLevels(Type enumType)
        {
            enumType = Nullable.GetUnderlyingType(enumType) ?? enumType;

            return Enum.GetNames(enumType).OrderBy(_ => _).ThenBy(_ => Enum.Parse(enumType, _)).ToArray();
        }

        static Dictionary<long, int> MakeLevelLookup(Type enumType)
        {
            var underlyingType = Enum.GetUnderlyingType(enumType);
            Func<object, long> makeLong;
            if(underlyingType == typeof(byte))
            {
                makeLong = o => (byte)o;
            }
            else
            {
                if(underlyingType == typeof(sbyte))
                {
                    makeLong = o => (sbyte)o;
                }
                else
                {
                    if(underlyingType == typeof(short))
                    {
                        makeLong = o => (short)o;
                    }
                    else
                    {
                        if(underlyingType == typeof(ushort))
                        {
                            makeLong = o => (ushort)o;
                        }
                        else
                        {
                            if(underlyingType == typeof(int))
                            {
                                makeLong = o => (int)o;
                            }
                            else
                            {
                                if (underlyingType == typeof(uint))
                                {
                                    makeLong = o => (uint)o;
                                }
                                else
                                {
                                    if (underlyingType == typeof(long))
                                    {
                                        makeLong = o => (long)o;
                                    }
                                    else
                                    {
                                        if (underlyingType == typeof(ulong))
                                        {
                                            makeLong = o => (long)(ulong)o;
                                        }
                                        else
                                        {
                                            throw new InvalidOperationException($"Unexpected type underlying an enum {underlyingType.Name}");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            

            var keys = LoadLevels(enumType);
            
            var ret = new Dictionary<long, int>();

            for(var i = 0; i < keys.Length; i++)
            {
                var key = keys[i];
                var val = Enum.Parse(enumType, key);
                var asLong = makeLong(val);

                ret[asLong] = i;
            }

            return ret;
        }

        static Func<TEnum, long> MakeConvertToLongDelegate<TEnum>()
            where TEnum: struct
        {
            var dyn = new DynamicMethod("Convert_" + typeof(TEnum).Name + "_ToLong", typeof(long), new[] { typeof(TEnum) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Conv_I8);
            il.Emit(OpCodes.Ret);

            return (Func<TEnum, long>)dyn.CreateDelegate(typeof(Func<TEnum, long>));
        }
    }
}
