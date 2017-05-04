using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet.Impl
{
    static class EnumMapper<T>
    {
        static readonly Func<int, T> Delegate;
        static EnumMapper()
        {
            var dyn = new System.Reflection.Emit.DynamicMethod("Map_Enum_" + typeof(T).Name, typeof(T), new[] { typeof(int) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            var underlying = Enum.GetUnderlyingType(typeof(T));

            il.Emit(System.Reflection.Emit.OpCodes.Ldarg_0);            // ---int---
            if (underlying == typeof(byte))
            {
                il.Emit(System.Reflection.Emit.OpCodes.Conv_U1);
            }
            else
            {
                if (underlying == typeof(sbyte))
                {
                    il.Emit(System.Reflection.Emit.OpCodes.Conv_I1);
                }
                else
                {
                    if (underlying == typeof(short))
                    {
                        il.Emit(System.Reflection.Emit.OpCodes.Conv_I2);
                    }
                    else
                    {
                        if (underlying == typeof(ushort))
                        {
                            il.Emit(System.Reflection.Emit.OpCodes.Conv_U2);
                        }
                        else
                        {
                            if (underlying == typeof(int))
                            {
                                // Nothing to be done
                            }
                            else
                            {
                                if (underlying == typeof(uint))
                                {
                                    il.Emit(System.Reflection.Emit.OpCodes.Conv_U4);
                                }
                                else
                                {
                                    if (underlying == typeof(long))
                                    {
                                        il.Emit(System.Reflection.Emit.OpCodes.Conv_I8);
                                    }
                                    else
                                    {
                                        if (underlying == typeof(ulong))
                                        {
                                            il.Emit(System.Reflection.Emit.OpCodes.Conv_U8);
                                        }
                                        else
                                        {
                                            throw new InvalidOperationException($"\"Impossible\" (don't get cute) enum found, underlying type isn't an integral type {typeof(T).Name}");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            il.Emit(System.Reflection.Emit.OpCodes.Ret);

            Delegate = (Func<int, T>)dyn.CreateDelegate(typeof(Func<int, T>));
        }

        public static T Map(int underlying) => Delegate(underlying);
    }
}
