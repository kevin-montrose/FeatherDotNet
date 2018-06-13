using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet.Impl
{
    static class SyntheticEnum
    {
        struct Key: IEquatable<Key>
        {
            public Type[] Enums;

            public Key(Type[] enums)
            {
                Enums = enums;
            }

            public bool Equals(Key other)
            {
                return other.Enums.SequenceEqual(Enums);
            }

            public override bool Equals(object obj)
            {
                if (!(obj is Key)) return false;

                return Equals((Key)obj);
            }

            public override int GetHashCode()
            {
                var ret = 0;
                foreach(var t in Enums)
                {
                    ret *= 17;
                    ret += t.GetHashCode();
                }

                return ret;
            }
        }

        static readonly AssemblyBuilder Assembly;
        static readonly ModuleBuilder Module;
        static readonly Dictionary<Key, Type> SyntheticEnumLookup;

        static SyntheticEnum()
        {
            Assembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("FeatherDotNet_SynthethicEnumLookup_DynamicAssembly"), AssemblyBuilderAccess.Run);
            Module = Assembly.DefineDynamicModule("DynamicModule");
            SyntheticEnumLookup = new Dictionary<Key, Type>();
        }

        public static Type Lookup(IEnumerable<Type> enumTypes)
        {
            var inOrder = enumTypes.OrderBy(t => t.AssemblyQualifiedName).ThenBy(t => t.FullName).ThenBy(t => t.GUID).ToArray();
            var key = new Key(inOrder);

            // assumed low contention
            lock (SyntheticEnumLookup)
            {
                Type syntheticEnum;
                if (SyntheticEnumLookup.TryGetValue(key, out syntheticEnum)) return syntheticEnum;

                SyntheticEnumLookup[key] = syntheticEnum = CreateSyntheticEnum(inOrder);

                return syntheticEnum;
            }
        }

        public static bool IsSynthetic(Type enumType) => enumType.Assembly.Equals(Assembly);

        static Type CreateSyntheticEnum(Type[] realEnums)
        {
            var enumName = "SytheticEnumFor_" + string.Join("-", realEnums.Select(t => t.FullName));

            var builder = Module.DefineEnum(enumName, TypeAttributes.Public, typeof(long));

            var distinctNames = realEnums.SelectMany(e => Enum.GetNames(e)).Distinct(StringComparer.InvariantCultureIgnoreCase).OrderBy(_ => _).ToArray();

            for(var i = 0; i < distinctNames.Length; i++)
            {
                builder.DefineLiteral(distinctNames[i], (long)(i + 1));
            }

            return builder.CreateTypeInfo().AsType();
        }
    }
}