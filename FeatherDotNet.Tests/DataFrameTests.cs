using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FeatherDotNet.Tests
{
    [TestClass]
    public class DataFrameTests
    {
        enum TestCategorySameNamesEnum
        {
            A = 99,
            B = 101,
            C = 202
        }

        enum TestCategorySameValuesEnum
        {
            Fizz = 0,
            Buzz = 1,
            Bazz = 2
        }

        [TestMethod]
        public void UntypedBasis0()
        {
            using (var dataframe = FeatherReader.ReadFromFile(@".\examples\r-feather-test.feather", BasisType.Zero))
            {

                Assert.AreEqual(5, dataframe.RowCount);
                Assert.AreEqual(8, dataframe.ColumnCount);

                // Column "Boolean" as bool
                {
                    // by index
                    Assert.AreEqual(true, dataframe[0, 0]);
                    Assert.AreEqual(true, dataframe[1, 0]);
                    Assert.AreEqual(true, dataframe[2, 0]);
                    Assert.AreEqual(false, dataframe[3, 0]);
                    Assert.AreEqual(false, dataframe[4, 0]);

                    // by name
                    Assert.AreEqual(true, dataframe[0, "Boolean"]);
                    Assert.AreEqual(true, dataframe[1, "Boolean"]);
                    Assert.AreEqual(true, dataframe[2, "Boolean"]);
                    Assert.AreEqual(false, dataframe[3, "Boolean"]);
                    Assert.AreEqual(false, dataframe[4, "Boolean"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[0].Cast<bool>().ToList();
                        Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Boolean"].Cast<bool>().ToList();
                        Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var boolColumn = dataframe.Columns[0];
                        var foreachRes = new List<bool>();
                        foreach (var val in boolColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var boolColumn = dataframe.Columns["Boolean"];
                        var foreachRes = new List<bool>();
                        foreach (var val in boolColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Boolean" as bool?
                {
                    // by index
                    Assert.AreEqual((bool?)true, dataframe[0, 0]);
                    Assert.AreEqual((bool?)true, dataframe[1, 0]);
                    Assert.AreEqual((bool?)true, dataframe[2, 0]);
                    Assert.AreEqual((bool?)false, dataframe[3, 0]);
                    Assert.AreEqual((bool?)false, dataframe[4, 0]);

                    // by name
                    Assert.AreEqual((bool?)true, dataframe[0, "Boolean"]);
                    Assert.AreEqual((bool?)true, dataframe[1, "Boolean"]);
                    Assert.AreEqual((bool?)true, dataframe[2, "Boolean"]);
                    Assert.AreEqual((bool?)false, dataframe[3, "Boolean"]);
                    Assert.AreEqual((bool?)false, dataframe[4, "Boolean"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[0].Cast<bool?>().ToList();
                        Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Boolean"].Cast<bool?>().ToList();
                        Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var boolColumn = dataframe.Columns[0];
                        var foreachRes = new List<bool?>();
                        foreach (var val in boolColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var boolColumn = dataframe.Columns["Boolean"];
                        var foreachRes = new List<bool?>();
                        foreach (var val in boolColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Integer" as int
                {
                    // by index
                    Assert.AreEqual(-1, dataframe[0, 1]);
                    Assert.AreEqual(0, dataframe[1, 1]);
                    Assert.AreEqual(1, dataframe[2, 1]);
                    Assert.AreEqual(2, dataframe[3, 1]);
                    Assert.AreEqual(3, dataframe[4, 1]);

                    // by name
                    Assert.AreEqual(-1, dataframe[0, "Integer"]);
                    Assert.AreEqual(0, dataframe[1, "Integer"]);
                    Assert.AreEqual(1, dataframe[2, "Integer"]);
                    Assert.AreEqual(2, dataframe[3, "Integer"]);
                    Assert.AreEqual(3, dataframe[4, "Integer"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[1].Cast<int>().ToList();
                        Assert.IsTrue(new int[] { -1, 0, 1, 2, 3 }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Integer"].Cast<int>().ToList();
                        Assert.IsTrue(new int[] { -1, 0, 1, 2, 3 }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var integerColumn = dataframe.Columns[1];
                        var foreachRes = new List<int>();
                        foreach (var val in integerColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int[] { -1, 0, 1, 2, 3 }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var integerColumn = dataframe["Integer"];
                        var foreachRes = new List<int>();
                        foreach (var val in integerColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int[] { -1, 0, 1, 2, 3 }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Integer" as int?
                {
                    // by index
                    Assert.AreEqual((int?)-1, dataframe[0, 1]);
                    Assert.AreEqual((int?)0, dataframe[1, 1]);
                    Assert.AreEqual((int?)1, dataframe[2, 1]);
                    Assert.AreEqual((int?)2, dataframe[3, 1]);
                    Assert.AreEqual((int?)3, dataframe[4, 1]);

                    // by name
                    Assert.AreEqual((int?)-1, dataframe[0, "Integer"]);
                    Assert.AreEqual((int?)0, dataframe[1, "Integer"]);
                    Assert.AreEqual((int?)1, dataframe[2, "Integer"]);
                    Assert.AreEqual((int?)2, dataframe[3, "Integer"]);
                    Assert.AreEqual((int?)3, dataframe[4, "Integer"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[1].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3 }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Integer"].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3 }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var integerColumn = dataframe.Columns[1];
                        var foreachRes = new List<int?>();
                        foreach (var val in integerColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3 }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var integerColumn = dataframe["Integer"];
                        var foreachRes = new List<int?>();
                        foreach (var val in integerColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3 }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Double" as double
                {
                    // by index
                    Assert.AreEqual(-1.5, dataframe[0, 2]);
                    Assert.AreEqual(-0.5, dataframe[1, 2]);
                    Assert.AreEqual(0.5, dataframe[2, 2]);
                    Assert.AreEqual(1.5, dataframe[3, 2]);
                    Assert.AreEqual(2.5, dataframe[4, 2]);

                    // by name
                    Assert.AreEqual(-1.5, dataframe[0, "Double"]);
                    Assert.AreEqual(-0.5, dataframe[1, "Double"]);
                    Assert.AreEqual(0.5, dataframe[2, "Double"]);
                    Assert.AreEqual(1.5, dataframe[3, "Double"]);
                    Assert.AreEqual(2.5, dataframe[4, "Double"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[2].Cast<double>().ToList();
                        Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Double"].Cast<double>().ToList();
                        Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var doubleColumn = dataframe.Columns[2];
                        var foreachRes = new List<double>();
                        foreach (var val in doubleColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var doubleColumn = dataframe["Double"];
                        var foreachRes = new List<double>();
                        foreach (var val in doubleColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Double" as double?
                {
                    // by index
                    Assert.AreEqual((double?)-1.5, dataframe[0, 2]);
                    Assert.AreEqual((double?)-0.5, dataframe[1, 2]);
                    Assert.AreEqual((double?)0.5, dataframe[2, 2]);
                    Assert.AreEqual((double?)1.5, dataframe[3, 2]);
                    Assert.AreEqual((double?)2.5, dataframe[4, 2]);

                    // by name
                    Assert.AreEqual((double?)-1.5, dataframe[0, "Double"]);
                    Assert.AreEqual((double?)-0.5, dataframe[1, "Double"]);
                    Assert.AreEqual((double?)0.5, dataframe[2, "Double"]);
                    Assert.AreEqual((double?)1.5, dataframe[3, "Double"]);
                    Assert.AreEqual((double?)2.5, dataframe[4, "Double"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[2].Cast<double?>().ToList();
                        Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Double"].Cast<double?>().ToList();
                        Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var doubleColumn = dataframe.Columns[2];
                        var foreachRes = new List<double?>();
                        foreach (var val in doubleColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var doubleColumn = dataframe["Double"];
                        var foreachRes = new List<double?>();
                        foreach (var val in doubleColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Category" as int
                {
                    // by index
                    Assert.AreEqual(2, dataframe[0, 3]);
                    Assert.AreEqual(2, dataframe[1, 3]);
                    Assert.AreEqual(0, dataframe[2, 3]);
                    Assert.AreEqual(0, dataframe[3, 3]);
                    Assert.AreEqual(1, dataframe[4, 3]);

                    // by name
                    Assert.AreEqual(2, dataframe[0, "Category"]);
                    Assert.AreEqual(2, dataframe[1, "Category"]);
                    Assert.AreEqual(0, dataframe[2, "Category"]);
                    Assert.AreEqual(0, dataframe[3, "Category"]);
                    Assert.AreEqual(1, dataframe[4, "Category"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<int>().ToList();
                        Assert.IsTrue(new int[] { 2, 2, 0, 0, 1 }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<int>().ToList();
                        Assert.IsTrue(new int[] { 2, 2, 0, 0, 1 }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var categoryColumn = dataframe.Columns[3];
                        var foreachRes = new List<int>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int[] { 2, 2, 0, 0, 1 }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var categoryColumn = dataframe["Category"];
                        var foreachRes = new List<int>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int[] { 2, 2, 0, 0, 1 }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Category" as int?
                {
                    // by index
                    Assert.AreEqual((int?)2, dataframe[0, 3]);
                    Assert.AreEqual((int?)2, dataframe[1, 3]);
                    Assert.AreEqual((int?)0, dataframe[2, 3]);
                    Assert.AreEqual((int?)0, dataframe[3, 3]);
                    Assert.AreEqual((int?)1, dataframe[4, 3]);

                    // by name
                    Assert.AreEqual((int?)2, dataframe[0, "Category"]);
                    Assert.AreEqual((int?)2, dataframe[1, "Category"]);
                    Assert.AreEqual((int?)0, dataframe[2, "Category"]);
                    Assert.AreEqual((int?)0, dataframe[3, "Category"]);
                    Assert.AreEqual((int?)1, dataframe[4, "Category"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { 2, 2, 0, 0, 1 }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { 2, 2, 0, 0, 1 }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var categoryColumn = dataframe.Columns[3];
                        var foreachRes = new List<int?>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { 2, 2, 0, 0, 1 }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var categoryColumn = dataframe["Category"];
                        var foreachRes = new List<int?>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { 2, 2, 0, 0, 1 }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Category" as string
                {
                    // by index
                    Assert.AreEqual("A", dataframe[0, 3]);
                    Assert.AreEqual("A", dataframe[1, 3]);
                    Assert.AreEqual("B", dataframe[2, 3]);
                    Assert.AreEqual("B", dataframe[3, 3]);
                    Assert.AreEqual("C", dataframe[4, 3]);

                    // by name
                    Assert.AreEqual("A", dataframe[0, "Category"]);
                    Assert.AreEqual("A", dataframe[1, "Category"]);
                    Assert.AreEqual("B", dataframe[2, "Category"]);
                    Assert.AreEqual("B", dataframe[3, "Category"]);
                    Assert.AreEqual("C", dataframe[4, "Category"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var categoryColumn = dataframe.Columns[3];
                        var foreachRes = new List<string>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var categoryColumn = dataframe["Category"];
                        var foreachRes = new List<string>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Category" as enum with same name
                {
                    // We can't implicitly convert to an enum, so only the LINQ-y Cast method can be used

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<TestCategorySameNamesEnum>().ToList();
                        Assert.IsTrue(new TestCategorySameNamesEnum[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<TestCategorySameNamesEnum>().ToList();
                        Assert.IsTrue(new TestCategorySameNamesEnum[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(linq));
                    }
                }

                // Column "Category" as nullable enum with same name
                {
                    // We can't implicitly convert to an enum, so only the LINQ-y Cast method can be used

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<TestCategorySameNamesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<TestCategorySameNamesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(linq));
                    }
                }

                // Column "Category" as enum with same values
                {
                    // We can't implicitly convert to an enum, so only the LINQ-y Cast method can be used

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<TestCategorySameValuesEnum>().ToList();
                        Assert.IsTrue(new TestCategorySameValuesEnum[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<TestCategorySameValuesEnum>().ToList();
                        Assert.IsTrue(new TestCategorySameValuesEnum[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(linq));
                    }
                }

                // Column "Category" as nullable enum with same values
                {
                    // We can't implicitly convert to an enum, so only the LINQ-y Cast method can be used

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<TestCategorySameValuesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<TestCategorySameValuesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(linq));
                    }
                }

                // Column "Timestamp" as DateTime
                {
                    // by index
                    Assert.AreEqual(new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[0, 4]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 4]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[2, 4]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[3, 4]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[4, 4]);

                    // by name
                    Assert.AreEqual(new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[0, "Timestamp"]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Timestamp"]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[2, "Timestamp"]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[3, "Timestamp"]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[4, "Timestamp"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<DateTime>().ToList();
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Timestamp"].Cast<DateTime>().ToList();
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timestampColumn = dataframe.Columns[4];
                        var foreachRes = new List<DateTime>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timestampColumn = dataframe["Timestamp"];
                        var foreachRes = new List<DateTime>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Timestamp" as DateTime?
                {
                    // by index
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[0, 4]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 4]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[2, 4]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[3, 4]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[4, 4]);

                    // by name
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[0, "Timestamp"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Timestamp"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[2, "Timestamp"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[3, "Timestamp"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[4, "Timestamp"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Timestamp"].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timestampColumn = dataframe.Columns[4];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timestampColumn = dataframe["Timestamp"];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Timestamp" as DateTimeOffset
                {
                    // by index
                    Assert.AreEqual((DateTimeOffset)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[0, 4]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 4]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[2, 4]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[3, 4]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[4, 4]);

                    // by name
                    Assert.AreEqual((DateTimeOffset)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[0, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[2, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[3, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[4, "Timestamp"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<DateTimeOffset>().ToList();
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Timestamp"].Cast<DateTimeOffset>().ToList();
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timestampColumn = dataframe.Columns[4];
                        var foreachRes = new List<DateTimeOffset>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timestampColumn = dataframe["Timestamp"];
                        var foreachRes = new List<DateTimeOffset>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Timestamp" as DateTimeOffset?
                {
                    // by index
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[0, 4]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 4]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[2, 4]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[3, 4]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[4, 4]);

                    // by name
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[0, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[2, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[3, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[4, "Timestamp"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Timestamp"].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timestampColumn = dataframe.Columns[4];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timestampColumn = dataframe["Timestamp"];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Time" as TimeSpan
                {
                    // by index
                    Assert.AreEqual(new TimeSpan(0, 0, 0), dataframe[0, 5]);
                    Assert.AreEqual(new TimeSpan(1, 1, 1), dataframe[1, 5]);
                    Assert.AreEqual(new TimeSpan(2, 2, 2), dataframe[2, 5]);
                    Assert.AreEqual(new TimeSpan(3, 3, 3), dataframe[3, 5]);
                    Assert.AreEqual(new TimeSpan(4, 4, 4), dataframe[4, 5]);

                    // by name
                    Assert.AreEqual(new TimeSpan(0, 0, 0), dataframe[0, "Time"]);
                    Assert.AreEqual(new TimeSpan(1, 1, 1), dataframe[1, "Time"]);
                    Assert.AreEqual(new TimeSpan(2, 2, 2), dataframe[2, "Time"]);
                    Assert.AreEqual(new TimeSpan(3, 3, 3), dataframe[3, "Time"]);
                    Assert.AreEqual(new TimeSpan(4, 4, 4), dataframe[4, "Time"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[5].Cast<TimeSpan>().ToList();
                        Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0) , new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Time"].Cast<TimeSpan>().ToList();
                        Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timeColumn = dataframe.Columns[5];
                        var foreachRes = new List<TimeSpan>();
                        foreach (var val in timeColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timeColumn = dataframe["Time"];
                        var foreachRes = new List<TimeSpan>();
                        foreach (var val in timeColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Time" as TimeSpan?
                {
                    // by index
                    Assert.AreEqual((TimeSpan?)new TimeSpan(0, 0, 0), dataframe[0, 5]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(1, 1, 1), dataframe[1, 5]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(2, 2, 2), dataframe[2, 5]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(3, 3, 3), dataframe[3, 5]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(4, 4, 4), dataframe[4, 5]);

                    // by name
                    Assert.AreEqual((TimeSpan?)new TimeSpan(0, 0, 0), dataframe[0, "Time"]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(1, 1, 1), dataframe[1, "Time"]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(2, 2, 2), dataframe[2, "Time"]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(3, 3, 3), dataframe[3, "Time"]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(4, 4, 4), dataframe[4, "Time"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[5].Cast<TimeSpan?>().ToList();
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Time"].Cast<TimeSpan?>().ToList();
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timeColumn = dataframe.Columns[5];
                        var foreachRes = new List<TimeSpan?>();
                        foreach (var val in timeColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timeColumn = dataframe["Time"];
                        var foreachRes = new List<TimeSpan?>();
                        foreach (var val in timeColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Date" as DateTime
                {
                    // by index
                    Assert.AreEqual(new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[0, 6]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 6]);
                    Assert.AreEqual(new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 6]);
                    Assert.AreEqual(new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[3, 6]);
                    Assert.AreEqual(new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[4, 6]);

                    // by name
                    Assert.AreEqual(new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[0, "Date"]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Date"]);
                    Assert.AreEqual(new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Date"]);
                    Assert.AreEqual(new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[3, "Date"]);
                    Assert.AreEqual(new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[4, "Date"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[6].Cast<DateTime>().ToList();
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Date"].Cast<DateTime>().ToList();
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var dateColumn = dataframe.Columns[6];
                        var foreachRes = new List<DateTime>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var dateColumn = dataframe["Date"];
                        var foreachRes = new List<DateTime>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Date" as DateTime?
                {
                    // by index
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[0, 6]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 6]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 6]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[3, 6]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[4, 6]);

                    // by name
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[0, "Date"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Date"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Date"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[3, "Date"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[4, "Date"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[6].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Date"].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var dateColumn = dataframe.Columns[6];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var dateColumn = dataframe["Date"];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Date" as DateTimeOffset
                {
                    // by index
                    Assert.AreEqual((DateTimeOffset)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[0, 6]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 6]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 6]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[3, 6]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[4, 6]);

                    // by name
                    Assert.AreEqual((DateTimeOffset)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[0, "Date"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Date"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Date"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[3, "Date"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[4, "Date"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[6].Cast<DateTimeOffset>().ToList();
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Date"].Cast<DateTimeOffset>().ToList();
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var dateColumn = dataframe.Columns[6];
                        var foreachRes = new List<DateTimeOffset>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var dateColumn = dataframe["Date"];
                        var foreachRes = new List<DateTimeOffset>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Date" as DateTimeOffset?
                {
                    // by index
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[0, 6]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 6]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 6]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[3, 6]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[4, 6]);

                    // by name
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[0, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[3, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[4, "Date"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[6].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Date"].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var dateColumn = dataframe.Columns[6];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var dateColumn = dataframe["Date"];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "String" as string
                {
                    // by index
                    Assert.AreEqual("A", dataframe[0, 7]);
                    Assert.AreEqual("", dataframe[1, 7]);
                    Assert.AreEqual("aaaaaaaa", dataframe[2, 7]);
                    Assert.AreEqual("bbbbbbbbbbbbb", dataframe[3, 7]);
                    Assert.AreEqual("CC", dataframe[4, 7]);

                    // by name
                    Assert.AreEqual("A", dataframe[0, "String"]);
                    Assert.AreEqual("", dataframe[1, "String"]);
                    Assert.AreEqual("aaaaaaaa", dataframe[2, "String"]);
                    Assert.AreEqual("bbbbbbbbbbbbb", dataframe[3, "String"]);
                    Assert.AreEqual("CC", dataframe[4, "String"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[7].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["String"].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var stringColumn = dataframe.Columns[7];
                        var foreachRes = new List<string>();
                        foreach (var val in stringColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var stringColumn = dataframe["String"];
                        var foreachRes = new List<string>();
                        foreach (var val in stringColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(foreachRes));
                    }
                }
            }
        }

        [TestMethod]
        public void UntypedBasis1()
        {
            using (var dataframe = FeatherReader.ReadFromFile(@".\examples\r-feather-test.feather", BasisType.One))
            {

                Assert.AreEqual(5, dataframe.RowCount);
                Assert.AreEqual(8, dataframe.ColumnCount);

                // Column "Boolean" as bool
                {
                    // by index
                    Assert.AreEqual(true, dataframe[1, 1]);
                    Assert.AreEqual(true, dataframe[2, 1]);
                    Assert.AreEqual(true, dataframe[3, 1]);
                    Assert.AreEqual(false, dataframe[4, 1]);
                    Assert.AreEqual(false, dataframe[5, 1]);

                    // by name
                    Assert.AreEqual(true, dataframe[1, "Boolean"]);
                    Assert.AreEqual(true, dataframe[2, "Boolean"]);
                    Assert.AreEqual(true, dataframe[3, "Boolean"]);
                    Assert.AreEqual(false, dataframe[4, "Boolean"]);
                    Assert.AreEqual(false, dataframe[5, "Boolean"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[1].Cast<bool>().ToList();
                        Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Boolean"].Cast<bool>().ToList();
                        Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var boolColumn = dataframe.Columns[1];
                        var foreachRes = new List<bool>();
                        foreach (var val in boolColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var boolColumn = dataframe.Columns["Boolean"];
                        var foreachRes = new List<bool>();
                        foreach (var val in boolColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Boolean" as bool?
                {
                    // by index
                    Assert.AreEqual((bool?)true, dataframe[1, 1]);
                    Assert.AreEqual((bool?)true, dataframe[2, 1]);
                    Assert.AreEqual((bool?)true, dataframe[3, 1]);
                    Assert.AreEqual((bool?)false, dataframe[4, 1]);
                    Assert.AreEqual((bool?)false, dataframe[5, 1]);

                    // by name
                    Assert.AreEqual((bool?)true, dataframe[1, "Boolean"]);
                    Assert.AreEqual((bool?)true, dataframe[2, "Boolean"]);
                    Assert.AreEqual((bool?)true, dataframe[3, "Boolean"]);
                    Assert.AreEqual((bool?)false, dataframe[4, "Boolean"]);
                    Assert.AreEqual((bool?)false, dataframe[5, "Boolean"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[1].Cast<bool?>().ToList();
                        Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Boolean"].Cast<bool?>().ToList();
                        Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var boolColumn = dataframe.Columns[1];
                        var foreachRes = new List<bool?>();
                        foreach (var val in boolColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var boolColumn = dataframe.Columns["Boolean"];
                        var foreachRes = new List<bool?>();
                        foreach (var val in boolColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Integer" as int
                {
                    // by index
                    Assert.AreEqual(-1, dataframe[1, 2]);
                    Assert.AreEqual(0, dataframe[2, 2]);
                    Assert.AreEqual(1, dataframe[3, 2]);
                    Assert.AreEqual(2, dataframe[4, 2]);
                    Assert.AreEqual(3, dataframe[5, 2]);

                    // by name
                    Assert.AreEqual(-1, dataframe[1, "Integer"]);
                    Assert.AreEqual(0, dataframe[2, "Integer"]);
                    Assert.AreEqual(1, dataframe[3, "Integer"]);
                    Assert.AreEqual(2, dataframe[4, "Integer"]);
                    Assert.AreEqual(3, dataframe[5, "Integer"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[2].Cast<int>().ToList();
                        Assert.IsTrue(new int[] { -1, 0, 1, 2, 3 }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Integer"].Cast<int>().ToList();
                        Assert.IsTrue(new int[] { -1, 0, 1, 2, 3 }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var integerColumn = dataframe.Columns[2];
                        var foreachRes = new List<int>();
                        foreach (var val in integerColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int[] { -1, 0, 1, 2, 3 }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var integerColumn = dataframe["Integer"];
                        var foreachRes = new List<int>();
                        foreach (var val in integerColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int[] { -1, 0, 1, 2, 3 }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Integer" as int?
                {
                    // by index
                    Assert.AreEqual((int?)-1, dataframe[1, 2]);
                    Assert.AreEqual((int?)0, dataframe[2, 2]);
                    Assert.AreEqual((int?)1, dataframe[3, 2]);
                    Assert.AreEqual((int?)2, dataframe[4, 2]);
                    Assert.AreEqual((int?)3, dataframe[5, 2]);

                    // by name
                    Assert.AreEqual((int?)-1, dataframe[1, "Integer"]);
                    Assert.AreEqual((int?)0, dataframe[2, "Integer"]);
                    Assert.AreEqual((int?)1, dataframe[3, "Integer"]);
                    Assert.AreEqual((int?)2, dataframe[4, "Integer"]);
                    Assert.AreEqual((int?)3, dataframe[5, "Integer"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[2].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3 }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Integer"].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3 }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var integerColumn = dataframe.Columns[2];
                        var foreachRes = new List<int?>();
                        foreach (var val in integerColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3 }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var integerColumn = dataframe["Integer"];
                        var foreachRes = new List<int?>();
                        foreach (var val in integerColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3 }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Double" as double
                {
                    // by index
                    Assert.AreEqual(-1.5, dataframe[1, 3]);
                    Assert.AreEqual(-0.5, dataframe[2, 3]);
                    Assert.AreEqual(0.5, dataframe[3, 3]);
                    Assert.AreEqual(1.5, dataframe[4, 3]);
                    Assert.AreEqual(2.5, dataframe[5, 3]);

                    // by name
                    Assert.AreEqual(-1.5, dataframe[1, "Double"]);
                    Assert.AreEqual(-0.5, dataframe[2, "Double"]);
                    Assert.AreEqual(0.5, dataframe[3, "Double"]);
                    Assert.AreEqual(1.5, dataframe[4, "Double"]);
                    Assert.AreEqual(2.5, dataframe[5, "Double"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<double>().ToList();
                        Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Double"].Cast<double>().ToList();
                        Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var doubleColumn = dataframe.Columns[3];
                        var foreachRes = new List<double>();
                        foreach (var val in doubleColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var doubleColumn = dataframe["Double"];
                        var foreachRes = new List<double>();
                        foreach (var val in doubleColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Double" as double?
                {
                    // by index
                    Assert.AreEqual((double?)-1.5, dataframe[1, 3]);
                    Assert.AreEqual((double?)-0.5, dataframe[2, 3]);
                    Assert.AreEqual((double?)0.5, dataframe[3, 3]);
                    Assert.AreEqual((double?)1.5, dataframe[4, 3]);
                    Assert.AreEqual((double?)2.5, dataframe[5, 3]);

                    // by name
                    Assert.AreEqual((double?)-1.5, dataframe[1, "Double"]);
                    Assert.AreEqual((double?)-0.5, dataframe[2, "Double"]);
                    Assert.AreEqual((double?)0.5, dataframe[3, "Double"]);
                    Assert.AreEqual((double?)1.5, dataframe[4, "Double"]);
                    Assert.AreEqual((double?)2.5, dataframe[5, "Double"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<double?>().ToList();
                        Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Double"].Cast<double?>().ToList();
                        Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var doubleColumn = dataframe.Columns[3];
                        var foreachRes = new List<double?>();
                        foreach (var val in doubleColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var doubleColumn = dataframe["Double"];
                        var foreachRes = new List<double?>();
                        foreach (var val in doubleColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Category" as int
                {
                    // by index
                    Assert.AreEqual(2, dataframe[1, 4]);
                    Assert.AreEqual(2, dataframe[2, 4]);
                    Assert.AreEqual(0, dataframe[3, 4]);
                    Assert.AreEqual(0, dataframe[4, 4]);
                    Assert.AreEqual(1, dataframe[5, 4]);

                    // by name
                    Assert.AreEqual(2, dataframe[1, "Category"]);
                    Assert.AreEqual(2, dataframe[2, "Category"]);
                    Assert.AreEqual(0, dataframe[3, "Category"]);
                    Assert.AreEqual(0, dataframe[4, "Category"]);
                    Assert.AreEqual(1, dataframe[5, "Category"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<int>().ToList();
                        Assert.IsTrue(new int[] { 2, 2, 0, 0, 1 }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<int>().ToList();
                        Assert.IsTrue(new int[] { 2, 2, 0, 0, 1 }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var categoryColumn = dataframe.Columns[4];
                        var foreachRes = new List<int>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int[] { 2, 2, 0, 0, 1 }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var categoryColumn = dataframe["Category"];
                        var foreachRes = new List<int>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int[] { 2, 2, 0, 0, 1 }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Category" as int?
                {
                    // by index
                    Assert.AreEqual((int?)2, dataframe[1, 4]);
                    Assert.AreEqual((int?)2, dataframe[2, 4]);
                    Assert.AreEqual((int?)0, dataframe[3, 4]);
                    Assert.AreEqual((int?)0, dataframe[4, 4]);
                    Assert.AreEqual((int?)1, dataframe[5, 4]);

                    // by name
                    Assert.AreEqual((int?)2, dataframe[1, "Category"]);
                    Assert.AreEqual((int?)2, dataframe[2, "Category"]);
                    Assert.AreEqual((int?)0, dataframe[3, "Category"]);
                    Assert.AreEqual((int?)0, dataframe[4, "Category"]);
                    Assert.AreEqual((int?)1, dataframe[5, "Category"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { 2, 2, 0, 0, 1 }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { 2, 2, 0, 0, 1 }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var categoryColumn = dataframe.Columns[4];
                        var foreachRes = new List<int?>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { 2, 2, 0, 0, 1 }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var categoryColumn = dataframe["Category"];
                        var foreachRes = new List<int?>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { 2, 2, 0, 0, 1 }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Category" as string
                {
                    // by index
                    Assert.AreEqual("A", dataframe[1, 4]);
                    Assert.AreEqual("A", dataframe[2, 4]);
                    Assert.AreEqual("B", dataframe[3, 4]);
                    Assert.AreEqual("B", dataframe[4, 4]);
                    Assert.AreEqual("C", dataframe[5, 4]);

                    // by name
                    Assert.AreEqual("A", dataframe[1, "Category"]);
                    Assert.AreEqual("A", dataframe[2, "Category"]);
                    Assert.AreEqual("B", dataframe[3, "Category"]);
                    Assert.AreEqual("B", dataframe[4, "Category"]);
                    Assert.AreEqual("C", dataframe[5, "Category"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var categoryColumn = dataframe.Columns[4];
                        var foreachRes = new List<string>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var categoryColumn = dataframe["Category"];
                        var foreachRes = new List<string>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Category" as enum with same name
                {
                    // We can't implicitly convert to an enum, so only the LINQ-y Cast method can be used

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<TestCategorySameNamesEnum>().ToList();
                        Assert.IsTrue(new TestCategorySameNamesEnum[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<TestCategorySameNamesEnum>().ToList();
                        Assert.IsTrue(new TestCategorySameNamesEnum[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(linq));
                    }
                }

                // Column "Category" as nullable enum with same name
                {
                    // We can't implicitly convert to an enum, so only the LINQ-y Cast method can be used

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<TestCategorySameNamesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<TestCategorySameNamesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(linq));
                    }
                }

                // Column "Category" as enum with same values
                {
                    // We can't implicitly convert to an enum, so only the LINQ-y Cast method can be used

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<TestCategorySameValuesEnum>().ToList();
                        Assert.IsTrue(new TestCategorySameValuesEnum[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<TestCategorySameValuesEnum>().ToList();
                        Assert.IsTrue(new TestCategorySameValuesEnum[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(linq));
                    }
                }

                // Column "Category" as nullable enum with same values
                {
                    // We can't implicitly convert to an enum, so only the LINQ-y Cast method can be used

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<TestCategorySameValuesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<TestCategorySameValuesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(linq));
                    }
                }

                // Column "Timestamp" as DateTime
                {
                    // by index
                    Assert.AreEqual(new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[1, 5]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 5]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[3, 5]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[4, 5]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[5, 5]);

                    // by name
                    Assert.AreEqual(new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[1, "Timestamp"]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Timestamp"]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[3, "Timestamp"]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[4, "Timestamp"]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[5, "Timestamp"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[5].Cast<DateTime>().ToList();
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Timestamp"].Cast<DateTime>().ToList();
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timestampColumn = dataframe.Columns[5];
                        var foreachRes = new List<DateTime>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timestampColumn = dataframe["Timestamp"];
                        var foreachRes = new List<DateTime>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Timestamp" as DateTime?
                {
                    // by index
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[1, 5]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 5]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[3, 5]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[4, 5]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[5, 5]);

                    // by name
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[1, "Timestamp"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Timestamp"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[3, "Timestamp"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[4, "Timestamp"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[5, "Timestamp"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[5].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Timestamp"].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timestampColumn = dataframe.Columns[5];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timestampColumn = dataframe["Timestamp"];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Timestamp" as DateTimeOffset
                {
                    // by index
                    Assert.AreEqual((DateTimeOffset)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[1, 5]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 5]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[3, 5]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[4, 5]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[5, 5]);

                    // by name
                    Assert.AreEqual((DateTimeOffset)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[1, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[3, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[4, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[5, "Timestamp"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[5].Cast<DateTimeOffset>().ToList();
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Timestamp"].Cast<DateTimeOffset>().ToList();
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timestampColumn = dataframe.Columns[5];
                        var foreachRes = new List<DateTimeOffset>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timestampColumn = dataframe["Timestamp"];
                        var foreachRes = new List<DateTimeOffset>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Timestamp" as DateTimeOffset?
                {
                    // by index
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[1, 5]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 5]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[3, 5]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[4, 5]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[5, 5]);

                    // by name
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[1, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), dataframe[3, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[4, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc), dataframe[5, "Timestamp"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[5].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Timestamp"].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timestampColumn = dataframe.Columns[5];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timestampColumn = dataframe["Timestamp"];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Time" as TimeSpan
                {
                    // by index
                    Assert.AreEqual(new TimeSpan(0, 0, 0), dataframe[1, 6]);
                    Assert.AreEqual(new TimeSpan(1, 1, 1), dataframe[2, 6]);
                    Assert.AreEqual(new TimeSpan(2, 2, 2), dataframe[3, 6]);
                    Assert.AreEqual(new TimeSpan(3, 3, 3), dataframe[4, 6]);
                    Assert.AreEqual(new TimeSpan(4, 4, 4), dataframe[5, 6]);

                    // by name
                    Assert.AreEqual(new TimeSpan(0, 0, 0), dataframe[1, "Time"]);
                    Assert.AreEqual(new TimeSpan(1, 1, 1), dataframe[2, "Time"]);
                    Assert.AreEqual(new TimeSpan(2, 2, 2), dataframe[3, "Time"]);
                    Assert.AreEqual(new TimeSpan(3, 3, 3), dataframe[4, "Time"]);
                    Assert.AreEqual(new TimeSpan(4, 4, 4), dataframe[5, "Time"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[6].Cast<TimeSpan>().ToList();
                        Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Time"].Cast<TimeSpan>().ToList();
                        Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timeColumn = dataframe.Columns[6];
                        var foreachRes = new List<TimeSpan>();
                        foreach (var val in timeColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timeColumn = dataframe["Time"];
                        var foreachRes = new List<TimeSpan>();
                        foreach (var val in timeColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Time" as TimeSpan?
                {
                    // by index
                    Assert.AreEqual((TimeSpan?)new TimeSpan(0, 0, 0), dataframe[1, 6]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(1, 1, 1), dataframe[2, 6]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(2, 2, 2), dataframe[3, 6]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(3, 3, 3), dataframe[4, 6]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(4, 4, 4), dataframe[5, 6]);

                    // by name
                    Assert.AreEqual((TimeSpan?)new TimeSpan(0, 0, 0), dataframe[1, "Time"]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(1, 1, 1), dataframe[2, "Time"]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(2, 2, 2), dataframe[3, "Time"]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(3, 3, 3), dataframe[4, "Time"]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(4, 4, 4), dataframe[5, "Time"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[6].Cast<TimeSpan?>().ToList();
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Time"].Cast<TimeSpan?>().ToList();
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timeColumn = dataframe.Columns[6];
                        var foreachRes = new List<TimeSpan?>();
                        foreach (var val in timeColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timeColumn = dataframe["Time"];
                        var foreachRes = new List<TimeSpan?>();
                        foreach (var val in timeColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Date" as DateTime
                {
                    // by index
                    Assert.AreEqual(new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 7]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 7]);
                    Assert.AreEqual(new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[3, 7]);
                    Assert.AreEqual(new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[4, 7]);
                    Assert.AreEqual(new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[5, 7]);

                    // by name
                    Assert.AreEqual(new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Date"]);
                    Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Date"]);
                    Assert.AreEqual(new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[3, "Date"]);
                    Assert.AreEqual(new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[4, "Date"]);
                    Assert.AreEqual(new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[5, "Date"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[7].Cast<DateTime>().ToList();
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Date"].Cast<DateTime>().ToList();
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var dateColumn = dataframe.Columns[7];
                        var foreachRes = new List<DateTime>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var dateColumn = dataframe["Date"];
                        var foreachRes = new List<DateTime>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Date" as DateTime?
                {
                    // by index
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 7]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 7]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[3, 7]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[4, 7]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[5, 7]);

                    // by name
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Date"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Date"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[3, "Date"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[4, "Date"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[5, "Date"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[7].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Date"].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var dateColumn = dataframe.Columns[7];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var dateColumn = dataframe["Date"];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Date" as DateTimeOffset
                {
                    // by index
                    Assert.AreEqual((DateTimeOffset)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 7]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 7]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[3, 7]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[4, 7]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[5, 7]);

                    // by name
                    Assert.AreEqual((DateTimeOffset)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Date"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Date"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[3, "Date"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[4, "Date"]);
                    Assert.AreEqual((DateTimeOffset)new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[5, "Date"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[7].Cast<DateTimeOffset>().ToList();
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Date"].Cast<DateTimeOffset>().ToList();
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var dateColumn = dataframe.Columns[7];
                        var foreachRes = new List<DateTimeOffset>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var dateColumn = dataframe["Date"];
                        var foreachRes = new List<DateTimeOffset>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Date" as DateTimeOffset?
                {
                    // by index
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 7]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 7]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[3, 7]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[4, 7]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[5, 7]);

                    // by name
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), dataframe[3, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[4, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc), dataframe[5, "Date"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[7].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Date"].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var dateColumn = dataframe.Columns[7];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var dateColumn = dataframe["Date"];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(foreachRes));
                    }
                }

                // Column "String" as string
                {
                    // by index
                    Assert.AreEqual("A", dataframe[1, 8]);
                    Assert.AreEqual("", dataframe[2, 8]);
                    Assert.AreEqual("aaaaaaaa", dataframe[3, 8]);
                    Assert.AreEqual("bbbbbbbbbbbbb", dataframe[4, 8]);
                    Assert.AreEqual("CC", dataframe[5, 8]);

                    // by name
                    Assert.AreEqual("A", dataframe[1, "String"]);
                    Assert.AreEqual("", dataframe[2, "String"]);
                    Assert.AreEqual("aaaaaaaa", dataframe[3, "String"]);
                    Assert.AreEqual("bbbbbbbbbbbbb", dataframe[4, "String"]);
                    Assert.AreEqual("CC", dataframe[5, "String"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[8].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["String"].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var stringColumn = dataframe.Columns[8];
                        var foreachRes = new List<string>();
                        foreach (var val in stringColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var stringColumn = dataframe["String"];
                        var foreachRes = new List<string>();
                        foreach (var val in stringColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(foreachRes));
                    }
                }
            }
        }

        [TestMethod]
        public void UntypedBasis0Nullable()
        {
            using (var dataframe = FeatherReader.ReadFromFile(@".\examples\r-feather-test-nullable.feather", BasisType.Zero))
            {

                Assert.AreEqual(5, dataframe.RowCount);
                Assert.AreEqual(8, dataframe.ColumnCount);

                // Column "Boolean" as bool?
                {
                    // by index
                    Assert.AreEqual((bool?)true, dataframe[0, 0]);
                    Assert.AreEqual((bool?)true, dataframe[1, 0]);
                    Assert.AreEqual((bool?)null, dataframe[2, 0]);
                    Assert.AreEqual((bool?)false, dataframe[3, 0]);
                    Assert.AreEqual((bool?)null, dataframe[4, 0]);

                    // by name
                    Assert.AreEqual((bool?)true, dataframe[0, "Boolean"]);
                    Assert.AreEqual((bool?)true, dataframe[1, "Boolean"]);
                    Assert.AreEqual((bool?)null, dataframe[2, "Boolean"]);
                    Assert.AreEqual((bool?)false, dataframe[3, "Boolean"]);
                    Assert.AreEqual((bool?)null, dataframe[4, "Boolean"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[0].Cast<bool?>().ToList();
                        Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Boolean"].Cast<bool?>().ToList();
                        Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var boolColumn = dataframe.Columns[0];
                        var foreachRes = new List<bool?>();
                        foreach (var val in boolColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var boolColumn = dataframe.Columns["Boolean"];
                        var foreachRes = new List<bool?>();
                        foreach (var val in boolColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(foreachRes));
                    }
                }
                
                // Column "Integer" as int?
                {
                    // by index
                    Assert.AreEqual((int?)-1, dataframe[0, 1]);
                    Assert.AreEqual((int?)0, dataframe[1, 1]);
                    Assert.AreEqual((int?)null, dataframe[2, 1]);
                    Assert.AreEqual((int?)2, dataframe[3, 1]);
                    Assert.AreEqual((int?)null, dataframe[4, 1]);

                    // by name
                    Assert.AreEqual((int?)-1, dataframe[0, "Integer"]);
                    Assert.AreEqual((int?)0, dataframe[1, "Integer"]);
                    Assert.AreEqual((int?)null, dataframe[2, "Integer"]);
                    Assert.AreEqual((int?)2, dataframe[3, "Integer"]);
                    Assert.AreEqual((int?)null, dataframe[4, "Integer"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[1].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { -1, 0, null, 2, null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Integer"].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { -1, 0, null, 2, null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var integerColumn = dataframe.Columns[1];
                        var foreachRes = new List<int?>();
                        foreach (var val in integerColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { -1, 0, null, 2, null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var integerColumn = dataframe["Integer"];
                        var foreachRes = new List<int?>();
                        foreach (var val in integerColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { -1, 0, null, 2, null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Double" as double?
                {
                    // by index
                    Assert.AreEqual((double?)-1.5, dataframe[0, 2]);
                    Assert.AreEqual((double?)-0.5, dataframe[1, 2]);
                    Assert.AreEqual((double?)null, dataframe[2, 2]);
                    Assert.AreEqual((double?)1.5, dataframe[3, 2]);
                    Assert.AreEqual((double?)null, dataframe[4, 2]);

                    // by name
                    Assert.AreEqual((double?)-1.5, dataframe[0, "Double"]);
                    Assert.AreEqual((double?)-0.5, dataframe[1, "Double"]);
                    Assert.AreEqual((double?)null, dataframe[2, "Double"]);
                    Assert.AreEqual((double?)1.5, dataframe[3, "Double"]);
                    Assert.AreEqual((double?)null, dataframe[4, "Double"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[2].Cast<double?>().ToList();
                        Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Double"].Cast<double?>().ToList();
                        Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var doubleColumn = dataframe.Columns[2];
                        var foreachRes = new List<double?>();
                        foreach (var val in doubleColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var doubleColumn = dataframe["Double"];
                        var foreachRes = new List<double?>();
                        foreach (var val in doubleColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(foreachRes));
                    }
                }
                
                // Column "Category" as int?
                {
                    // by index
                    Assert.AreEqual((int?)2, dataframe[0, 3]);
                    Assert.AreEqual((int?)2, dataframe[1, 3]);
                    Assert.AreEqual((int?)null, dataframe[2, 3]);
                    Assert.AreEqual((int?)0, dataframe[3, 3]);
                    Assert.AreEqual((int?)null, dataframe[4, 3]);

                    // by name
                    Assert.AreEqual((int?)2, dataframe[0, "Category"]);
                    Assert.AreEqual((int?)2, dataframe[1, "Category"]);
                    Assert.AreEqual((int?)null, dataframe[2, "Category"]);
                    Assert.AreEqual((int?)0, dataframe[3, "Category"]);
                    Assert.AreEqual((int?)null, dataframe[4, "Category"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { 2, 2, null, 0, null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { 2, 2, null, 0, null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var categoryColumn = dataframe.Columns[3];
                        var foreachRes = new List<int?>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { 2, 2, null, 0, null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var categoryColumn = dataframe["Category"];
                        var foreachRes = new List<int?>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { 2, 2, null, 0, null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Category" as string
                {
                    // by index
                    Assert.AreEqual("A", dataframe[0, 3]);
                    Assert.AreEqual("A", dataframe[1, 3]);
                    Assert.AreEqual(default(string), dataframe[2, 3]);
                    Assert.AreEqual("B", dataframe[3, 3]);
                    Assert.AreEqual(default(string), dataframe[4, 3]);

                    // by name
                    Assert.AreEqual("A", dataframe[0, "Category"]);
                    Assert.AreEqual("A", dataframe[1, "Category"]);
                    Assert.AreEqual(default(string), dataframe[2, "Category"]);
                    Assert.AreEqual("B", dataframe[3, "Category"]);
                    Assert.AreEqual(default(string), dataframe[4, "Category"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var categoryColumn = dataframe.Columns[3];
                        var foreachRes = new List<string>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var categoryColumn = dataframe["Category"];
                        var foreachRes = new List<string>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(foreachRes));
                    }
                }
                
                // Column "Category" as nullable enum with same name
                {
                    // We can't implicitly convert to an enum, so only the LINQ-y Cast method can be used

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<TestCategorySameNamesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, null, TestCategorySameNamesEnum.B, null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<TestCategorySameNamesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, null, TestCategorySameNamesEnum.B, null }.SequenceEqual(linq));
                    }
                }
                
                // Column "Category" as nullable enum with same values
                {
                    // We can't implicitly convert to an enum, so only the LINQ-y Cast method can be used

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<TestCategorySameValuesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, null, TestCategorySameValuesEnum.Fizz, null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<TestCategorySameValuesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, null, TestCategorySameValuesEnum.Fizz, null }.SequenceEqual(linq));
                    }
                }
                
                // Column "Timestamp" as DateTime?
                {
                    // by index
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[0, 4]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 4]);
                    Assert.AreEqual((DateTime?)null, dataframe[2, 4]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[3, 4]);
                    Assert.AreEqual((DateTime?)null, dataframe[4, 4]);

                    // by name
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[0, "Timestamp"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Timestamp"]);
                    Assert.AreEqual((DateTime?)null, dataframe[2, "Timestamp"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[3, "Timestamp"]);
                    Assert.AreEqual((DateTime?)null, dataframe[4, "Timestamp"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Timestamp"].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timestampColumn = dataframe.Columns[4];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timestampColumn = dataframe["Timestamp"];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }
                }
                
                // Column "Timestamp" as DateTimeOffset?
                {
                    // by index
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[0, 4]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 4]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[2, 4]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[3, 4]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[4, 4]);

                    // by name
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[0, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[2, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[3, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[4, "Timestamp"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Timestamp"].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timestampColumn = dataframe.Columns[4];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timestampColumn = dataframe["Timestamp"];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }
                }
                
                // Column "Time" as TimeSpan?
                {
                    // by index
                    Assert.AreEqual((TimeSpan?)new TimeSpan(0, 0, 0), dataframe[0, 5]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(1, 1, 1), dataframe[1, 5]);
                    Assert.AreEqual((TimeSpan?)null, dataframe[2, 5]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(3, 3, 3), dataframe[3, 5]);
                    Assert.AreEqual((TimeSpan?)null, dataframe[4, 5]);

                    // by name
                    Assert.AreEqual((TimeSpan?)new TimeSpan(0, 0, 0), dataframe[0, "Time"]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(1, 1, 1), dataframe[1, "Time"]);
                    Assert.AreEqual((TimeSpan?)null, dataframe[2, "Time"]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(3, 3, 3), dataframe[3, "Time"]);
                    Assert.AreEqual((TimeSpan?)null, dataframe[4, "Time"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[5].Cast<TimeSpan?>().ToList();
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Time"].Cast<TimeSpan?>().ToList();
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timeColumn = dataframe.Columns[5];
                        var foreachRes = new List<TimeSpan?>();
                        foreach (var val in timeColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timeColumn = dataframe["Time"];
                        var foreachRes = new List<TimeSpan?>();
                        foreach (var val in timeColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(foreachRes));
                    }
                }
                
                // Column "Date" as DateTime?
                {
                    // by index
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[0, 6]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 6]);
                    Assert.AreEqual((DateTime?)null, dataframe[2, 6]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[3, 6]);
                    Assert.AreEqual((DateTime?)null, dataframe[4, 6]);

                    // by name
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[0, "Date"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Date"]);
                    Assert.AreEqual((DateTime?)null, dataframe[2, "Date"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[3, "Date"]);
                    Assert.AreEqual((DateTime?)null, dataframe[4, "Date"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[6].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Date"].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var dateColumn = dataframe.Columns[6];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var dateColumn = dataframe["Date"];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }
                }
                
                // Column "Date" as DateTimeOffset?
                {
                    // by index
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[0, 6]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 6]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[2, 6]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[3, 6]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[4, 6]);

                    // by name
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[0, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[2, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[3, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[4, "Date"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[6].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Date"].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var dateColumn = dataframe.Columns[6];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var dateColumn = dataframe["Date"];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "String" as string
                {
                    // by index
                    Assert.AreEqual("A", dataframe[0, 7]);
                    Assert.AreEqual("", dataframe[1, 7]);
                    Assert.AreEqual(default(string), dataframe[2, 7]);
                    Assert.AreEqual("bbbbbbbbbbbbb", dataframe[3, 7]);
                    Assert.AreEqual(default(string), dataframe[4, 7]);

                    // by name
                    Assert.AreEqual("A", dataframe[0, "String"]);
                    Assert.AreEqual("", dataframe[1, "String"]);
                    Assert.AreEqual(default(string), dataframe[2, "String"]);
                    Assert.AreEqual("bbbbbbbbbbbbb", dataframe[3, "String"]);
                    Assert.AreEqual(default(string), dataframe[4, "String"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[7].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["String"].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var stringColumn = dataframe.Columns[7];
                        var foreachRes = new List<string>();
                        foreach (var val in stringColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var stringColumn = dataframe["String"];
                        var foreachRes = new List<string>();
                        foreach (var val in stringColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(foreachRes));
                    }
                }
            }
        }

        [TestMethod]
        public void UntypedBasis1Nullable()
        {
            using (var dataframe = FeatherReader.ReadFromFile(@".\examples\r-feather-test-nullable.feather", BasisType.One))
            {

                Assert.AreEqual(5, dataframe.RowCount);
                Assert.AreEqual(8, dataframe.ColumnCount);

                // Column "Boolean" as bool?
                {
                    // by index
                    Assert.AreEqual((bool?)true, dataframe[1, 1]);
                    Assert.AreEqual((bool?)true, dataframe[2, 1]);
                    Assert.AreEqual((bool?)null, dataframe[3, 1]);
                    Assert.AreEqual((bool?)false, dataframe[4, 1]);
                    Assert.AreEqual((bool?)null, dataframe[5, 1]);

                    // by name
                    Assert.AreEqual((bool?)true, dataframe[1, "Boolean"]);
                    Assert.AreEqual((bool?)true, dataframe[2, "Boolean"]);
                    Assert.AreEqual((bool?)null, dataframe[3, "Boolean"]);
                    Assert.AreEqual((bool?)false, dataframe[4, "Boolean"]);
                    Assert.AreEqual((bool?)null, dataframe[5, "Boolean"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[1].Cast<bool?>().ToList();
                        Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Boolean"].Cast<bool?>().ToList();
                        Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var boolColumn = dataframe.Columns[1];
                        var foreachRes = new List<bool?>();
                        foreach (var val in boolColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var boolColumn = dataframe.Columns["Boolean"];
                        var foreachRes = new List<bool?>();
                        foreach (var val in boolColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Integer" as int?
                {
                    // by index
                    Assert.AreEqual((int?)-1, dataframe[1, 2]);
                    Assert.AreEqual((int?)0, dataframe[2, 2]);
                    Assert.AreEqual((int?)null, dataframe[3, 2]);
                    Assert.AreEqual((int?)2, dataframe[4, 2]);
                    Assert.AreEqual((int?)null, dataframe[5, 2]);

                    // by name
                    Assert.AreEqual((int?)-1, dataframe[1, "Integer"]);
                    Assert.AreEqual((int?)0, dataframe[2, "Integer"]);
                    Assert.AreEqual((int?)null, dataframe[3, "Integer"]);
                    Assert.AreEqual((int?)2, dataframe[4, "Integer"]);
                    Assert.AreEqual((int?)null, dataframe[5, "Integer"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[2].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { -1, 0, null, 2, null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Integer"].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { -1, 0, null, 2, null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var integerColumn = dataframe.Columns[2];
                        var foreachRes = new List<int?>();
                        foreach (var val in integerColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { -1, 0, null, 2, null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var integerColumn = dataframe["Integer"];
                        var foreachRes = new List<int?>();
                        foreach (var val in integerColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { -1, 0, null, 2, null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Double" as double?
                {
                    // by index
                    Assert.AreEqual((double?)-1.5, dataframe[1, 3]);
                    Assert.AreEqual((double?)-0.5, dataframe[2, 3]);
                    Assert.AreEqual((double?)null, dataframe[3, 3]);
                    Assert.AreEqual((double?)1.5, dataframe[4, 3]);
                    Assert.AreEqual((double?)null, dataframe[5, 3]);

                    // by name
                    Assert.AreEqual((double?)-1.5, dataframe[1, "Double"]);
                    Assert.AreEqual((double?)-0.5, dataframe[2, "Double"]);
                    Assert.AreEqual((double?)null, dataframe[3, "Double"]);
                    Assert.AreEqual((double?)1.5, dataframe[4, "Double"]);
                    Assert.AreEqual((double?)null, dataframe[5, "Double"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[3].Cast<double?>().ToList();
                        Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Double"].Cast<double?>().ToList();
                        Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var doubleColumn = dataframe.Columns[3];
                        var foreachRes = new List<double?>();
                        foreach (var val in doubleColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var doubleColumn = dataframe["Double"];
                        var foreachRes = new List<double?>();
                        foreach (var val in doubleColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Category" as int?
                {
                    // by index
                    Assert.AreEqual((int?)2, dataframe[1, 4]);
                    Assert.AreEqual((int?)2, dataframe[2, 4]);
                    Assert.AreEqual((int?)null, dataframe[3, 4]);
                    Assert.AreEqual((int?)0, dataframe[4, 4]);
                    Assert.AreEqual((int?)null, dataframe[5, 4]);

                    // by name
                    Assert.AreEqual((int?)2, dataframe[1, "Category"]);
                    Assert.AreEqual((int?)2, dataframe[2, "Category"]);
                    Assert.AreEqual((int?)null, dataframe[3, "Category"]);
                    Assert.AreEqual((int?)0, dataframe[4, "Category"]);
                    Assert.AreEqual((int?)null, dataframe[5, "Category"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { 2, 2, null, 0, null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<int?>().ToList();
                        Assert.IsTrue(new int?[] { 2, 2, null, 0, null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var categoryColumn = dataframe.Columns[4];
                        var foreachRes = new List<int?>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { 2, 2, null, 0, null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var categoryColumn = dataframe["Category"];
                        var foreachRes = new List<int?>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new int?[] { 2, 2, null, 0, null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Category" as string
                {
                    // by index
                    Assert.AreEqual("A", dataframe[1, 4]);
                    Assert.AreEqual("A", dataframe[2, 4]);
                    Assert.AreEqual(default(string), dataframe[3, 4]);
                    Assert.AreEqual("B", dataframe[4, 4]);
                    Assert.AreEqual(default(string), dataframe[5, 4]);

                    // by name
                    Assert.AreEqual("A", dataframe[1, "Category"]);
                    Assert.AreEqual("A", dataframe[2, "Category"]);
                    Assert.AreEqual(default(string), dataframe[3, "Category"]);
                    Assert.AreEqual("B", dataframe[4, "Category"]);
                    Assert.AreEqual(default(string), dataframe[5, "Category"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var categoryColumn = dataframe.Columns[4];
                        var foreachRes = new List<string>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var categoryColumn = dataframe["Category"];
                        var foreachRes = new List<string>();
                        foreach (var val in categoryColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Category" as nullable enum with same name
                {
                    // We can't implicitly convert to an enum, so only the LINQ-y Cast method can be used

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<TestCategorySameNamesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, null, TestCategorySameNamesEnum.B, null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<TestCategorySameNamesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, null, TestCategorySameNamesEnum.B, null }.SequenceEqual(linq));
                    }
                }

                // Column "Category" as nullable enum with same values
                {
                    // We can't implicitly convert to an enum, so only the LINQ-y Cast method can be used

                    // via linq by index
                    {
                        var linq = dataframe.Columns[4].Cast<TestCategorySameValuesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, null, TestCategorySameValuesEnum.Fizz, null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Category"].Cast<TestCategorySameValuesEnum?>().ToList();
                        Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, null, TestCategorySameValuesEnum.Fizz, null }.SequenceEqual(linq));
                    }
                }

                // Column "Timestamp" as DateTime?
                {
                    // by index
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[1, 5]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 5]);
                    Assert.AreEqual((DateTime?)null, dataframe[3, 5]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[4, 5]);
                    Assert.AreEqual((DateTime?)null, dataframe[5, 5]);

                    // by name
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[1, "Timestamp"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Timestamp"]);
                    Assert.AreEqual((DateTime?)null, dataframe[3, "Timestamp"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[4, "Timestamp"]);
                    Assert.AreEqual((DateTime?)null, dataframe[5, "Timestamp"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[5].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Timestamp"].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timestampColumn = dataframe.Columns[5];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timestampColumn = dataframe["Timestamp"];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Timestamp" as DateTimeOffset?
                {
                    // by index
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[1, 5]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 5]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[3, 5]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[4, 5]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[5, 5]);

                    // by name
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), dataframe[1, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[3, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), dataframe[4, "Timestamp"]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[5, "Timestamp"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[5].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Timestamp"].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timestampColumn = dataframe.Columns[5];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timestampColumn = dataframe["Timestamp"];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in timestampColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Time" as TimeSpan?
                {
                    // by index
                    Assert.AreEqual((TimeSpan?)new TimeSpan(0, 0, 0), dataframe[1, 6]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(1, 1, 1), dataframe[2, 6]);
                    Assert.AreEqual((TimeSpan?)null, dataframe[3, 6]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(3, 3, 3), dataframe[4, 6]);
                    Assert.AreEqual((TimeSpan?)null, dataframe[5, 6]);

                    // by name
                    Assert.AreEqual((TimeSpan?)new TimeSpan(0, 0, 0), dataframe[1, "Time"]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(1, 1, 1), dataframe[2, "Time"]);
                    Assert.AreEqual((TimeSpan?)null, dataframe[3, "Time"]);
                    Assert.AreEqual((TimeSpan?)new TimeSpan(3, 3, 3), dataframe[4, "Time"]);
                    Assert.AreEqual((TimeSpan?)null, dataframe[5, "Time"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[6].Cast<TimeSpan?>().ToList();
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Time"].Cast<TimeSpan?>().ToList();
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var timeColumn = dataframe.Columns[6];
                        var foreachRes = new List<TimeSpan?>();
                        foreach (var val in timeColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var timeColumn = dataframe["Time"];
                        var foreachRes = new List<TimeSpan?>();
                        foreach (var val in timeColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Date" as DateTime?
                {
                    // by index
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 7]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 7]);
                    Assert.AreEqual((DateTime?)null, dataframe[3, 7]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[4, 7]);
                    Assert.AreEqual((DateTime?)null, dataframe[5, 7]);

                    // by name
                    Assert.AreEqual((DateTime?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Date"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Date"]);
                    Assert.AreEqual((DateTime?)null, dataframe[3, "Date"]);
                    Assert.AreEqual((DateTime?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[4, "Date"]);
                    Assert.AreEqual((DateTime?)null, dataframe[5, "Date"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[7].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Date"].Cast<DateTime?>().ToList();
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var dateColumn = dataframe.Columns[7];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var dateColumn = dataframe["Date"];
                        var foreachRes = new List<DateTime?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "Date" as DateTimeOffset?
                {
                    // by index
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[1, 7]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[2, 7]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[3, 7]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[4, 7]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[5, 7]);

                    // by name
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), dataframe[1, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), dataframe[2, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[3, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), dataframe[4, "Date"]);
                    Assert.AreEqual((DateTimeOffset?)null, dataframe[5, "Date"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[7].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["Date"].Cast<DateTimeOffset?>().ToList();
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var dateColumn = dataframe.Columns[7];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var dateColumn = dataframe["Date"];
                        var foreachRes = new List<DateTimeOffset?>();
                        foreach (var val in dateColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(foreachRes));
                    }
                }

                // Column "String" as string
                {
                    // by index
                    Assert.AreEqual("A", dataframe[1, 8]);
                    Assert.AreEqual("", dataframe[2, 8]);
                    Assert.AreEqual(default(string), dataframe[3, 8]);
                    Assert.AreEqual("bbbbbbbbbbbbb", dataframe[4, 8]);
                    Assert.AreEqual(default(string), dataframe[5, 8]);

                    // by name
                    Assert.AreEqual("A", dataframe[1, "String"]);
                    Assert.AreEqual("", dataframe[2, "String"]);
                    Assert.AreEqual(default(string), dataframe[3, "String"]);
                    Assert.AreEqual("bbbbbbbbbbbbb", dataframe[4, "String"]);
                    Assert.AreEqual(default(string), dataframe[5, "String"]);

                    // via linq by index
                    {
                        var linq = dataframe.Columns[8].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(linq));
                    }

                    // via linq by name
                    {
                        var linq = dataframe["String"].Cast<string>().ToList();
                        Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(linq));
                    }

                    // via foreach by index
                    {
                        var stringColumn = dataframe.Columns[8];
                        var foreachRes = new List<string>();
                        foreach (var val in stringColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(foreachRes));
                    }

                    // via foreach by name
                    {
                        var stringColumn = dataframe["String"];
                        var foreachRes = new List<string>();
                        foreach (var val in stringColumn)
                        {
                            foreachRes.Add(val);
                        }
                        Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(foreachRes));
                    }
                }
            }
        }
    }
}
