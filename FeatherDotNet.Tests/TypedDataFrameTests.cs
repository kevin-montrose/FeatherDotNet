using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FeatherDotNet.Tests
{
    [TestClass]
    public class TypedDataFrameTests
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
        public void TypedBasis0()
        {
            using (var untyped = FeatherReader.ReadFromFile(@".\examples\r-feather-test.feather", BasisType.Zero))
            {
                {
                    var typed = untyped.Map<bool, int, double, string, DateTimeOffset, TimeSpan, DateTime, string>();

                    Assert.AreEqual(5, typed.RowCount);
                    Assert.AreEqual(8, typed.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 0; i < typed.RowCount; i++)
                    {
                        var row = typed.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(typed.Column1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(typed.Column2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(typed.Column3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(typed.Column4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(typed.Column5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(typed.Column6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(typed.Column7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(typed.Column8));
                }

                {
                    var typed = untyped.Map<bool, int, double, TestCategorySameNamesEnum, DateTimeOffset, TimeSpan, DateTime, string>();

                    Assert.AreEqual(5, typed.RowCount);
                    Assert.AreEqual(8, typed.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<TestCategorySameNamesEnum>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 0; i < typed.RowCount; i++)
                    {
                        var row = typed.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum[]  { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(typed.Column1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(typed.Column2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(typed.Column3));
                    Assert.IsTrue(new TestCategorySameNamesEnum[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(typed.Column4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(typed.Column5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(typed.Column6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(typed.Column7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(typed.Column8));
                }

                {
                    var typed = untyped.Map<bool, int, double, TestCategorySameValuesEnum, DateTimeOffset, TimeSpan, DateTime, string>();

                    Assert.AreEqual(5, typed.RowCount);
                    Assert.AreEqual(8, typed.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<TestCategorySameValuesEnum>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 0; i < typed.RowCount; i++)
                    {
                        var row = typed.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(typed.Column1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(typed.Column2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(typed.Column3));
                    Assert.IsTrue(new TestCategorySameValuesEnum[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(typed.Column4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(typed.Column5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(typed.Column6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(typed.Column7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(typed.Column8));
                }

                {
                    var typedNull = untyped.Map<bool?, int?, double?, string, DateTimeOffset?, TimeSpan?, DateTime?, string>();

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < typedNull.RowCount; i++)
                    {
                        var row = typedNull.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(typedNull.Column1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(typedNull.Column2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(typedNull.Column3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(typedNull.Column4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(typedNull.Column5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(typedNull.Column6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(typedNull.Column7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(typedNull.Column8));
                }

                {
                    var typedNull = untyped.Map<bool?, int?, double?, TestCategorySameNamesEnum?, DateTimeOffset?, TimeSpan?, DateTime?, string>();

                    Assert.AreEqual(5, typedNull.RowCount);
                    Assert.AreEqual(8, typedNull.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameNamesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < typedNull.RowCount; i++)
                    {
                        var row = typedNull.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(typedNull.Column1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(typedNull.Column2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(typedNull.Column3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(typedNull.Column4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(typedNull.Column5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(typedNull.Column6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(typedNull.Column7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(typedNull.Column8));
                }

                {
                    var typedNull = untyped.Map<bool?, int?, double?, TestCategorySameValuesEnum?, DateTimeOffset?, TimeSpan?, DateTime?, string>();

                    Assert.AreEqual(5, typedNull.RowCount);
                    Assert.AreEqual(8, typedNull.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameValuesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < typedNull.RowCount; i++)
                    {
                        var row = typedNull.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(typedNull.Column1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(typedNull.Column2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(typedNull.Column3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(typedNull.Column4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(typedNull.Column5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(typedNull.Column6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(typedNull.Column7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(typedNull.Column8));
                }
            }
        }

        [TestMethod]
        public void TypedBasis1()
        {
            using (var untyped = FeatherReader.ReadFromFile(@".\examples\r-feather-test.feather", BasisType.One))
            {
                {
                    var typed = untyped.Map<bool, int, double, string, DateTimeOffset, TimeSpan, DateTime, string>();

                    Assert.AreEqual(5, typed.RowCount);
                    Assert.AreEqual(8, typed.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= typed.RowCount; i++)
                    {
                        var row = typed.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(typed.Column1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(typed.Column2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(typed.Column3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(typed.Column4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(typed.Column5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(typed.Column6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(typed.Column7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(typed.Column8));
                }

                {
                    var typed = untyped.Map<bool, int, double, TestCategorySameNamesEnum, DateTimeOffset, TimeSpan, DateTime, string>();

                    Assert.AreEqual(5, typed.RowCount);
                    Assert.AreEqual(8, typed.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<TestCategorySameNamesEnum>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= typed.RowCount; i++)
                    {
                        var row = typed.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(typed.Column1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(typed.Column2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(typed.Column3));
                    Assert.IsTrue(new TestCategorySameNamesEnum[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(typed.Column4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(typed.Column5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(typed.Column6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(typed.Column7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(typed.Column8));
                }

                {
                    var typed = untyped.Map<bool, int, double, TestCategorySameValuesEnum, DateTimeOffset, TimeSpan, DateTime, string>();

                    Assert.AreEqual(5, typed.RowCount);
                    Assert.AreEqual(8, typed.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<TestCategorySameValuesEnum>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= typed.RowCount; i++)
                    {
                        var row = typed.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(typed.Column1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(typed.Column2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(typed.Column3));
                    Assert.IsTrue(new TestCategorySameValuesEnum[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(typed.Column4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(typed.Column5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(typed.Column6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(typed.Column7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(typed.Column8));
                }

                {
                    var typedNull = untyped.Map<bool?, int?, double?, string, DateTimeOffset?, TimeSpan?, DateTime?, string>();

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= typedNull.RowCount; i++)
                    {
                        var row = typedNull.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(typedNull.Column1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(typedNull.Column2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(typedNull.Column3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(typedNull.Column4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(typedNull.Column5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(typedNull.Column6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(typedNull.Column7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(typedNull.Column8));
                }

                {
                    var typedNull = untyped.Map<bool?, int?, double?, TestCategorySameNamesEnum?, DateTimeOffset?, TimeSpan?, DateTime?, string>();

                    Assert.AreEqual(5, typedNull.RowCount);
                    Assert.AreEqual(8, typedNull.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameNamesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= typedNull.RowCount; i++)
                    {
                        var row = typedNull.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(typedNull.Column1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(typedNull.Column2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(typedNull.Column3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(typedNull.Column4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(typedNull.Column5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(typedNull.Column6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(typedNull.Column7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(typedNull.Column8));
                }

                {
                    var typedNull = untyped.Map<bool?, int?, double?, TestCategorySameValuesEnum?, DateTimeOffset?, TimeSpan?, DateTime?, string>();

                    Assert.AreEqual(5, typedNull.RowCount);
                    Assert.AreEqual(8, typedNull.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameValuesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= typedNull.RowCount; i++)
                    {
                        var row = typedNull.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(typedNull.Column1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(typedNull.Column2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(typedNull.Column3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(typedNull.Column4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(typedNull.Column5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(typedNull.Column6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(typedNull.Column7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(typedNull.Column8));
                }
            }
        }

        [TestMethod]
        public void TypedBasis0Nullables()
        {
            using (var untyped = FeatherReader.ReadFromFile(@".\examples\r-feather-test-nullable.feather", BasisType.Zero))
            {
                {
                    var typedNull = untyped.Map<bool?, int?, double?, string, DateTimeOffset?, TimeSpan?, DateTime?, string>();

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < typedNull.RowCount; i++)
                    {
                        var row = typedNull.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(typedNull.Column1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(typedNull.Column2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(typedNull.Column3));
                    Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(typedNull.Column4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(typedNull.Column5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(typedNull.Column6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(typedNull.Column7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(typedNull.Column8));
                }

                {
                    var typedNull = untyped.Map<bool?, int?, double?, TestCategorySameNamesEnum?, DateTimeOffset?, TimeSpan?, DateTime?, string>();

                    Assert.AreEqual(5, typedNull.RowCount);
                    Assert.AreEqual(8, typedNull.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameNamesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < typedNull.RowCount; i++)
                    {
                        var row = typedNull.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, null, TestCategorySameNamesEnum.B, null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(typedNull.Column1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(typedNull.Column2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(typedNull.Column3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, null, TestCategorySameNamesEnum.B, null }.SequenceEqual(typedNull.Column4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(typedNull.Column5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(typedNull.Column6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(typedNull.Column7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(typedNull.Column8));
                }

                {
                    var typedNull = untyped.Map<bool?, int?, double?, TestCategorySameValuesEnum?, DateTimeOffset?, TimeSpan?, DateTime?, string>();

                    Assert.AreEqual(5, typedNull.RowCount);
                    Assert.AreEqual(8, typedNull.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameValuesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < typedNull.RowCount; i++)
                    {
                        var row = typedNull.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, null, TestCategorySameValuesEnum.Fizz, null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(typedNull.Column1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(typedNull.Column2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(typedNull.Column3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, null, TestCategorySameValuesEnum.Fizz, null }.SequenceEqual(typedNull.Column4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(typedNull.Column5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(typedNull.Column6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(typedNull.Column7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(typedNull.Column8));
                }
            }
        }

        [TestMethod]
        public void TypedBasis1Nullables()
        {
            using (var untyped = FeatherReader.ReadFromFile(@".\examples\r-feather-test-nullable.feather", BasisType.One))
            {
                {
                    var typedNull = untyped.Map<bool?, int?, double?, string, DateTimeOffset?, TimeSpan?, DateTime?, string>();

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= typedNull.RowCount; i++)
                    {
                        var row = typedNull.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(typedNull.Column1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(typedNull.Column2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(typedNull.Column3));
                    Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(typedNull.Column4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(typedNull.Column5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(typedNull.Column6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(typedNull.Column7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(typedNull.Column8));
                }

                {
                    var typedNull = untyped.Map<bool?, int?, double?, TestCategorySameNamesEnum?, DateTimeOffset?, TimeSpan?, DateTime?, string>();

                    Assert.AreEqual(5, typedNull.RowCount);
                    Assert.AreEqual(8, typedNull.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameNamesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= typedNull.RowCount; i++)
                    {
                        var row = typedNull.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, null, TestCategorySameNamesEnum.B, null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(typedNull.Column1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(typedNull.Column2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(typedNull.Column3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, null, TestCategorySameNamesEnum.B, null }.SequenceEqual(typedNull.Column4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(typedNull.Column5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(typedNull.Column6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(typedNull.Column7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(typedNull.Column8));
                }

                {
                    var typedNull = untyped.Map<bool?, int?, double?, TestCategorySameValuesEnum?, DateTimeOffset?, TimeSpan?, DateTime?, string>();

                    Assert.AreEqual(5, typedNull.RowCount);
                    Assert.AreEqual(8, typedNull.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameValuesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= typedNull.RowCount; i++)
                    {
                        var row = typedNull.Rows[i];
                        t1.Add(row.Column1);
                        t2.Add(row.Column2);
                        t3.Add(row.Column3);
                        t4.Add(row.Column4);
                        t5.Add(row.Column5);
                        t6.Add(row.Column6);
                        t7.Add(row.Column7);
                        t8.Add(row.Column8);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, null, TestCategorySameValuesEnum.Fizz, null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(typedNull.Column1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(typedNull.Column2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(typedNull.Column3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, null, TestCategorySameValuesEnum.Fizz, null }.SequenceEqual(typedNull.Column4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(typedNull.Column5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(typedNull.Column6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(typedNull.Column7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(typedNull.Column8));
                }
            }
        }
    }
}
