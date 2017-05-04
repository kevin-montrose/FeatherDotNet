using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet.Impl
{
    static class CollectionLengthLookup
    {
        static readonly Dictionary<Type, Func<object, int>> LengthGetterLookup = new Dictionary<Type, Func<object, int>>();

        public static int GetLength(Type elementType, object collection)
        {
            Func<object, int> getter;

            // assuming this is a low contention lock
            lock (LengthGetterLookup)
            {
                if (!LengthGetterLookup.TryGetValue(elementType, out getter))
                {
                    LengthGetterLookup[elementType] = getter = CreateLengthGetter(elementType);
                }
            }

            return getter(collection);
        }

        static Func<object, int> CreateLengthGetter(Type elementType)
        {
            var typedCollection = typeof(ICollection<>).MakeGenericType(elementType);
            var count = typedCollection.GetProperty("Count").GetMethod;

            var dyn = new System.Reflection.Emit.DynamicMethod($"{nameof(CollectionLengthLookup)}_{nameof(elementType.Name)}", typeof(int), new[] { typeof(object) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            il.Emit(System.Reflection.Emit.OpCodes.Ldarg_0);                        // object
            il.Emit(System.Reflection.Emit.OpCodes.Castclass, typedCollection);     // ICollection<elementType>
            il.Emit(System.Reflection.Emit.OpCodes.Callvirt, count);                // int
            il.Emit(System.Reflection.Emit.OpCodes.Ret);                            // --empty--

            return (Func<object, int>)dyn.CreateDelegate(typeof(Func<object, int>));
        }
    }
}
