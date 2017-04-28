using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FeatherDotNet.Tests
{
    [TestClass]
    public class ProxyDataFrameTests
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

#pragma warning disable 0649
        class ProxyClass1
        {
            public bool Boolean { get; set; }
            public int Integer;
            public double Double { get; set; }
            public string Category;
            public DateTimeOffset Timestamp { get; set; }
            public TimeSpan Time;
            public DateTime Date { get; set; }
            public string @String { get; set; }
        }

        class ProxyClass1Nullable
        {
            public bool? Boolean { get; set; }
            public int? Integer;
            public double? Double { get; set; }
            public string Category;
            public DateTimeOffset? Timestamp { get; set; }
            public TimeSpan? Time;
            public DateTime? Date { get; set; }
            public string @String { get; set; }
        }

        struct ProxyStruct1
        {
            public bool Boolean { get; set; }
            public int Integer;
            public double Double { get; set; }
            public string Category;
            public DateTimeOffset Timestamp { get; set; }
            public TimeSpan Time;
            public DateTime Date { get; set; }
            public string @String { get; set; }
        }

        struct ProxyStruct1Nullable
        {
            public bool? Boolean { get; set; }
            public int? Integer;
            public double? Double { get; set; }
            public string Category;
            public DateTimeOffset? Timestamp { get; set; }
            public TimeSpan? Time;
            public DateTime? Date { get; set; }
            public string @String { get; set; }
        }

        class ProxyClass2
        {
            public bool Boolean { get; set; }
            public int Integer;
            public double Double { get; set; }
            public TestCategorySameNamesEnum Category;
            public DateTimeOffset Timestamp { get; set; }
            public TimeSpan Time;
            public DateTime Date { get; set; }
            public string @String { get; set; }
        }

        class ProxyClass2Nullable
        {
            public bool? Boolean { get; set; }
            public int? Integer;
            public double? Double { get; set; }
            public TestCategorySameNamesEnum? Category;
            public DateTimeOffset? Timestamp { get; set; }
            public TimeSpan? Time;
            public DateTime? Date { get; set; }
            public string @String { get; set; }
        }

        struct ProxyStruct2
        {
            public bool Boolean { get; set; }
            public int Integer;
            public double Double { get; set; }
            public TestCategorySameNamesEnum Category;
            public DateTimeOffset Timestamp { get; set; }
            public TimeSpan Time;
            public DateTime Date { get; set; }
            public string @String { get; set; }
        }

        struct ProxyStruct2Nullable
        {
            public bool? Boolean { get; set; }
            public int? Integer;
            public double? Double { get; set; }
            public TestCategorySameNamesEnum? Category;
            public DateTimeOffset? Timestamp { get; set; }
            public TimeSpan? Time;
            public DateTime? Date { get; set; }
            public string @String { get; set; }
        }

        class ProxyClass3
        {
            public bool Boolean { get; set; }
            public int Integer;
            public double Double { get; set; }
            public TestCategorySameValuesEnum Category;
            public DateTimeOffset Timestamp { get; set; }
            public TimeSpan Time;
            public DateTime Date { get; set; }
            public string @String { get; set; }
        }

        class ProxyClass3Nullable
        {
            public bool? Boolean { get; set; }
            public int? Integer;
            public double? Double { get; set; }
            public TestCategorySameValuesEnum? Category;
            public DateTimeOffset? Timestamp { get; set; }
            public TimeSpan? Time;
            public DateTime? Date { get; set; }
            public string @String { get; set; }
        }

        struct ProxyStruct3
        {
            public bool Boolean { get; set; }
            public int Integer;
            public double Double { get; set; }
            public TestCategorySameValuesEnum Category;
            public DateTimeOffset Timestamp { get; set; }
            public TimeSpan Time;
            public DateTime Date { get; set; }
            public string @String { get; set; }
        }

        struct ProxyStruct3Nullable
        {
            public bool? Boolean { get; set; }
            public int? Integer;
            public double? Double { get; set; }
            public TestCategorySameValuesEnum? Category;
            public DateTimeOffset? Timestamp { get; set; }
            public TimeSpan? Time;
            public DateTime? Date { get; set; }
            public string @String { get; set; }
        }
#pragma warning restore 0649

        [TestMethod]
        public void ProxiedClassBasis0()
        {
            using (var untyped = FeatherReader.ReadFromFile(@".\examples\r-feather-test.feather", BasisType.Zero))
            {
                {
                    var proxy = untyped.Proxy<ProxyClass1>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass1Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass2>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<TestCategorySameNamesEnum>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass2Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameNamesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass3>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<TestCategorySameValuesEnum>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass3Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameValuesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }
            }
        }

        [TestMethod]
        public void ProxiedClassBasis1()
        {
            using (var untyped = FeatherReader.ReadFromFile(@".\examples\r-feather-test.feather", BasisType.One))
            {
                {
                    var proxy = untyped.Proxy<ProxyClass1>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass1Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass2>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<TestCategorySameNamesEnum>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass2Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameNamesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass3>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<TestCategorySameValuesEnum>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass3Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameValuesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }
            }
        }

        [TestMethod]
        public void ProxiedClassBasis0Nullable()
        {
            using (var untyped = FeatherReader.ReadFromFile(@".\examples\r-feather-test-nullable.feather", BasisType.Zero))
            {
                {
                    var proxy = untyped.Proxy<ProxyClass1Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass2Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameNamesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, null, TestCategorySameNamesEnum.B, null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass3Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameValuesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, null, TestCategorySameValuesEnum.Fizz, null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));
                }
            }
        }

        [TestMethod]
        public void ProxiedClassBasis1Nullable()
        {
            using (var untyped = FeatherReader.ReadFromFile(@".\examples\r-feather-test-nullable.feather", BasisType.One))
            {
                {
                    var proxy = untyped.Proxy<ProxyClass1Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass2Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameNamesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, null, TestCategorySameNamesEnum.B, null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyClass3Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameValuesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, null, TestCategorySameValuesEnum.Fizz, null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));
                }
            }
        }

        [TestMethod]
        public void ProxiedStructBasis0()
        {
            using (var untyped = FeatherReader.ReadFromFile(@".\examples\r-feather-test.feather", BasisType.Zero))
            {
                {
                    var proxy = untyped.Proxy<ProxyStruct1>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct1Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct2>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<TestCategorySameNamesEnum>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct2Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameNamesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct3>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<TestCategorySameValuesEnum>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct3Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameValuesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }
            }
        }

        [TestMethod]
        public void ProxiedStructBasis1()
        {
            using (var untyped = FeatherReader.ReadFromFile(@".\examples\r-feather-test.feather", BasisType.One))
            {
                {
                    var proxy = untyped.Proxy<ProxyStruct1>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct1Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", "B", "B", "C" }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct2>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<TestCategorySameNamesEnum>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct2Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameNamesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.B, TestCategorySameNamesEnum.C }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct3>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool>();
                    var t2 = new List<int>();
                    var t3 = new List<double>();
                    var t4 = new List<TestCategorySameValuesEnum>();
                    var t5 = new List<DateTimeOffset>();
                    var t6 = new List<TimeSpan>();
                    var t7 = new List<DateTime>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct3Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameValuesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, true, false, false }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, 1, 2, 3, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, 0.5, 1.5, 2.5 }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Fizz, TestCategorySameValuesEnum.Buzz }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 1, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 3, 0, DateTimeKind.Utc) }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), new TimeSpan(2, 2, 2), new TimeSpan(3, 3, 3), new TimeSpan(4, 4, 4) }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 4, 0, 0, 0, DateTimeKind.Utc) }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", "aaaaaaaa", "bbbbbbbbbbbbb", "CC" }.SequenceEqual(t8));
                }
            }
        }

        [TestMethod]
        public void ProxiedStructBasis0Nullable()
        {
            using (var untyped = FeatherReader.ReadFromFile(@".\examples\r-feather-test-nullable.feather", BasisType.Zero))
            {
                {
                    var proxy = untyped.Proxy<ProxyStruct1Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct2Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameNamesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, null, TestCategorySameNamesEnum.B, null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct3Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameValuesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 0; i < proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, null, TestCategorySameValuesEnum.Fizz, null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));
                }
            }
        }

        [TestMethod]
        public void ProxiedStructBasis1Nullable()
        {
            using (var untyped = FeatherReader.ReadFromFile(@".\examples\r-feather-test-nullable.feather", BasisType.One))
            {
                {
                    var proxy = untyped.Proxy<ProxyStruct1Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<string>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new string[] { "A", "A", null, "B", null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct2Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameNamesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameNamesEnum?[] { TestCategorySameNamesEnum.A, TestCategorySameNamesEnum.A, null, TestCategorySameNamesEnum.B, null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));
                }

                {
                    var proxy = untyped.Proxy<ProxyStruct3Nullable>();

                    Assert.AreEqual(5, proxy.RowCount);
                    Assert.AreEqual(8, proxy.ColumnCount);

                    var t1 = new List<bool?>();
                    var t2 = new List<int?>();
                    var t3 = new List<double?>();
                    var t4 = new List<TestCategorySameValuesEnum?>();
                    var t5 = new List<DateTimeOffset?>();
                    var t6 = new List<TimeSpan?>();
                    var t7 = new List<DateTime?>();
                    var t8 = new List<string>();
                    for (var i = 1; i <= proxy.RowCount; i++)
                    {
                        var row = proxy.Rows[i];
                        t1.Add(row.Boolean);
                        t2.Add(row.Integer);
                        t3.Add(row.Double);
                        t4.Add(row.Category);
                        t5.Add(row.Timestamp);
                        t6.Add(row.Time);
                        t7.Add(row.Date);
                        t8.Add(row.String);
                    }

                    Assert.IsTrue(new bool?[] { true, true, null, false, null }.SequenceEqual(t1));
                    Assert.IsTrue(new int?[] { -1, 0, null, 2, null, }.SequenceEqual(t2));
                    Assert.IsTrue(new double?[] { -1.5, -0.5, null, 1.5, null }.SequenceEqual(t3));
                    Assert.IsTrue(new TestCategorySameValuesEnum?[] { TestCategorySameValuesEnum.Bazz, TestCategorySameValuesEnum.Bazz, null, TestCategorySameValuesEnum.Fizz, null }.SequenceEqual(t4));
                    Assert.IsTrue(new DateTimeOffset?[] { new DateTime(1969, 12, 31, 23, 59, 59, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 2, 0, DateTimeKind.Utc), null }.SequenceEqual(t5));
                    Assert.IsTrue(new TimeSpan?[] { new TimeSpan(0, 0, 0), new TimeSpan(1, 1, 1), null, new TimeSpan(3, 3, 3), null }.SequenceEqual(t6));
                    Assert.IsTrue(new DateTime?[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Utc), null }.SequenceEqual(t7));
                    Assert.IsTrue(new string[] { "A", "", null, "bbbbbbbbbbbbb", null }.SequenceEqual(t8));
                }
            }
        }
    }
}