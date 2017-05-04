using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

namespace FeatherDotNet.Impl
{
    static class ValueCaster<T>
    {
        static readonly Func<Value, CategoryEnumMapType, T> Delegate;

        static ValueCaster()
        {
            var dyn = new DynamicMethod("Cast_Value_To_" + typeof(T).Name, typeof(T), new[] { typeof(Value), typeof(CategoryEnumMapType) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            bool needsCategoryList;

            if (typeof(T) == typeof(Value))
            {

                il.Emit(OpCodes.Ldarg_0);                 // Value
                il.Emit(OpCodes.Ret);                     // --empty--
            }
            else
            {
                var tIsEnum = typeof(T).IsEnum;
                var tIsNullableEnum = Nullable.GetUnderlyingType(typeof(T))?.IsEnum ?? false;

                MethodInfo conversionStaticMethod;

                if (tIsEnum)
                {
                    var convertEnumGen = typeof(Value).GetMethod("ConvertEnum", BindingFlags.NonPublic | BindingFlags.Static);
                    var convertEnum = convertEnumGen.MakeGenericMethod(typeof(T));

                    needsCategoryList = true;
                    conversionStaticMethod = convertEnum;
                }
                else
                {
                    if (tIsNullableEnum)
                    {
                        var enumType = Nullable.GetUnderlyingType(typeof(T));
                        var convertNullableEnumGen = typeof(Value).GetMethod("ConvertNullableEnum", BindingFlags.NonPublic | BindingFlags.Static);
                        var convertNullableEnum = convertNullableEnumGen.MakeGenericMethod(enumType);

                        needsCategoryList = true;
                        conversionStaticMethod = convertNullableEnum;
                    }
                    else
                    {
                        needsCategoryList = false;
                        conversionStaticMethod =
                            typeof(Value)
                                .GetMethods(BindingFlags.Static | BindingFlags.Public)
                                .Where(mtd => mtd.Name == "op_Implicit" && mtd.ReturnType == typeof(T))
                                .Single();
                    }
                }

                il.Emit(OpCodes.Ldarg_0);                        // Value

                if (needsCategoryList)
                {
                    il.Emit(OpCodes.Ldarg_1);                    // Value CategoryEnumMapType
                }

                il.Emit(OpCodes.Call, conversionStaticMethod);   // T
                il.Emit(OpCodes.Ret);                            // --empty--
            }

            Delegate = (Func<Value, CategoryEnumMapType, T>)dyn.CreateDelegate(typeof(Func<Value, CategoryEnumMapType, T>));
        }

        internal static T Cast(Value value, CategoryEnumMapType enumMapConfig) => Delegate(value, enumMapConfig);
    }
}
