using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet.Tests
{
    [TestClass]
    public class PublicInterfaceTests
    {
        [TestMethod]
        public void OnlyPublicInMainNamespace()
        {
            var mainNamespace = nameof(FeatherDotNet);

            var asm = Assembly.GetAssembly(typeof(FeatherReader));
            var allTypes = asm.GetTypes().Where(t => t.GetCustomAttribute<System.Runtime.CompilerServices.CompilerGeneratedAttribute>() == null).ToList();

            foreach(var type in allTypes)
            {
                var ns = type.Namespace;
                if (ns != mainNamespace) continue;
                
                if (type.IsPublic) continue;

                Assert.Fail($"Type {type.FullName} declared in main namespace {mainNamespace} is not public.");
            }
        }

        [TestMethod]
        public void OnlyNonPublicInNonMainNamespace()
        {
            var mainNamespace = nameof(FeatherDotNet);

            var asm = Assembly.GetAssembly(typeof(FeatherReader));
            var allTypes = asm.GetTypes().Where(t => t.GetCustomAttribute<System.Runtime.CompilerServices.CompilerGeneratedAttribute>() == null).ToList();

            foreach (var type in allTypes)
            {
                var ns = type.Namespace;
                if (ns == mainNamespace) continue;

                if (!type.IsPublic) continue;

                Assert.Fail($"Type {type.FullName} declared in non-main namespace {mainNamespace} is public.");
            }
        }
    }
}
