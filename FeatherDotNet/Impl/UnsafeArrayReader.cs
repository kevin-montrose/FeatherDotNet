using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet.Impl
{
    static class UnsafeArrayReader<T>
    {
        static readonly Action<MemoryMappedViewAccessor, long, T[], int, int> Delegate;

        static UnsafeArrayReader()
        {
            var dyn = new System.Reflection.Emit.DynamicMethod("UnsafeArrayReader_" + typeof(T).Name, null, new[] { typeof(MemoryMappedViewAccessor), typeof(long), typeof(T[]), typeof(int), typeof(int) });
            var il = dyn.GetILGenerator();

            var readArrayGen = typeof(MemoryMappedViewAccessor).GetMethod("ReadArray");
            var readArray = readArrayGen.MakeGenericMethod(typeof(T));

            il.Emit(System.Reflection.Emit.OpCodes.Ldarg_0);            // MemoryMappedViewAccessor
            il.Emit(System.Reflection.Emit.OpCodes.Ldarg_1);            // MemoryMappedViewAccessor long
            il.Emit(System.Reflection.Emit.OpCodes.Ldarg_2);            // MemoryMappedViewAccessor long T[]
            il.Emit(System.Reflection.Emit.OpCodes.Ldarg_3);            // MemoryMappedViewAccessor long T[] int
            il.Emit(System.Reflection.Emit.OpCodes.Ldarg_S, (byte)4);   // MemoryMappedViewAccessor long T[] int int
            il.Emit(System.Reflection.Emit.OpCodes.Call, readArray);    // int
            il.Emit(System.Reflection.Emit.OpCodes.Pop);                // --empty--
            il.Emit(System.Reflection.Emit.OpCodes.Ret);                // --empty--

            Delegate = (Action<MemoryMappedViewAccessor, long, T[], int, int>)dyn.CreateDelegate(typeof(Action<MemoryMappedViewAccessor, long, T[], int, int>));
        }

        public static void ReadArray(MemoryMappedViewAccessor view, long position, T[] arr, int index, int length) => Delegate(view, position, arr, index, length);
    }
}
