using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FeatherDotNet.Tests
{
    [TestClass]
    public class WriteTests
    {
        [TestMethod]
        public void StreamPrimitiveFixedSizedArrayWrite()
        {
            var col1 = new double[] { -1, 0, 1 };
            var col2 = new float[] { -2, 0.5f, 2 };
            var col3 = new long[] { long.MinValue, 0, long.MaxValue };
            var col4 = new ulong[] { ulong.MinValue, 1234, ulong.MaxValue };
            var col5 = new int[] { int.MinValue, 0, int.MaxValue };
            var col6 = new uint[] { uint.MinValue, 4321, uint.MaxValue };
            var col7 = new short[] { short.MinValue, 0, short.MaxValue };
            var col8 = new ushort[] { ushort.MinValue, 8642, ushort.MaxValue };
            var col9 = new byte[] { byte.MinValue, 12, byte.MaxValue };
            var col10 = new sbyte[] { sbyte.MinValue, 0, sbyte.MaxValue };
            var col11 = new bool[] { true, false, true };
            var col12 = new DateTime[] { new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc) };
            var col13 = new DateTimeOffset[] { new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc) };
            var col14 = new TimeSpan[] { new TimeSpan(-1000, -23, -59, -48, -37), TimeSpan.Zero, new TimeSpan(1000, 23, 59, 48, 37) };

            byte[] bytes;
            using (var memory = new MemoryStream())
            {

                using (var writer = new FeatherWriter(memory, WriteMode.Eager))
                {
                    writer.AddColumn("Double", col1);
                    writer.AddColumn("Float", col2);
                    writer.AddColumn("Long", col3);
                    writer.AddColumn("ULong", col4);
                    writer.AddColumn("Int", col5);
                    writer.AddColumn("UInt", col6);
                    writer.AddColumn("Short", col7);
                    writer.AddColumn("UShort", col8);
                    writer.AddColumn("Byte", col9);
                    writer.AddColumn("SByte", col10);
                    writer.AddColumn("Bool", col11);
                    writer.AddColumn("DateTime", col12);
                    writer.AddColumn("DateTimeOffset", col13);
                    writer.AddColumn("TimeSpan", col14);
                }

                bytes = memory.ToArray();
            }

            using (var offDisk = FeatherReader.ReadFromBytes(bytes, BasisType.Zero))
            {
                Assert.AreEqual(3, offDisk.RowCount);
                Assert.AreEqual(14, offDisk.ColumnCount);

                Assert.AreEqual("Double", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(double), offDisk.Columns[0].Type);
                Assert.AreEqual("Float", offDisk.Columns[1].Name);
                Assert.AreEqual(typeof(float), offDisk.Columns[1].Type);
                Assert.AreEqual("Long", offDisk.Columns[2].Name);
                Assert.AreEqual(typeof(long), offDisk.Columns[2].Type);
                Assert.AreEqual("ULong", offDisk.Columns[3].Name);
                Assert.AreEqual(typeof(ulong), offDisk.Columns[3].Type);
                Assert.AreEqual("Int", offDisk.Columns[4].Name);
                Assert.AreEqual(typeof(int), offDisk.Columns[4].Type);
                Assert.AreEqual("UInt", offDisk.Columns[5].Name);
                Assert.AreEqual(typeof(uint), offDisk.Columns[5].Type);
                Assert.AreEqual("Short", offDisk.Columns[6].Name);
                Assert.AreEqual(typeof(short), offDisk.Columns[6].Type);
                Assert.AreEqual("UShort", offDisk.Columns[7].Name);
                Assert.AreEqual(typeof(ushort), offDisk.Columns[7].Type);
                Assert.AreEqual("Byte", offDisk.Columns[8].Name);
                Assert.AreEqual(typeof(byte), offDisk.Columns[8].Type);
                Assert.AreEqual("SByte", offDisk.Columns[9].Name);
                Assert.AreEqual(typeof(sbyte), offDisk.Columns[9].Type);
                Assert.AreEqual("Bool", offDisk.Columns[10].Name);
                Assert.AreEqual(typeof(bool), offDisk.Columns[10].Type);
                Assert.AreEqual("DateTime", offDisk.Columns[11].Name);
                Assert.AreEqual(typeof(DateTime), offDisk.Columns[11].Type);
                Assert.AreEqual("DateTimeOffset", offDisk.Columns[12].Name);
                Assert.AreEqual(typeof(DateTime), offDisk.Columns[12].Type);    // DateTimeOffset is deserialized as a DateTime (since we UTC everything anyway)
                Assert.AreEqual("TimeSpan", offDisk.Columns[13].Name);
                Assert.AreEqual(typeof(TimeSpan), offDisk.Columns[13].Type);

                Assert.IsTrue(col1.SequenceEqual(offDisk.Columns[0].Cast<double>()));
                Assert.IsTrue(col2.SequenceEqual(offDisk.Columns[1].Cast<float>()));
                Assert.IsTrue(col3.SequenceEqual(offDisk.Columns[2].Cast<long>()));
                Assert.IsTrue(col4.SequenceEqual(offDisk.Columns[3].Cast<ulong>()));
                Assert.IsTrue(col5.SequenceEqual(offDisk.Columns[4].Cast<int>()));
                Assert.IsTrue(col6.SequenceEqual(offDisk.Columns[5].Cast<uint>()));
                Assert.IsTrue(col7.SequenceEqual(offDisk.Columns[6].Cast<short>()));
                Assert.IsTrue(col8.SequenceEqual(offDisk.Columns[7].Cast<ushort>()));
                Assert.IsTrue(col9.SequenceEqual(offDisk.Columns[8].Cast<byte>()));
                Assert.IsTrue(col10.SequenceEqual(offDisk.Columns[9].Cast<sbyte>()));
                Assert.IsTrue(col11.SequenceEqual(offDisk.Columns[10].Cast<bool>()));
                Assert.IsTrue(col12.SequenceEqual(offDisk.Columns[11].Cast<DateTime>()));
                Assert.IsTrue(col13.SequenceEqual(offDisk.Columns[12].Cast<DateTimeOffset>()));
                Assert.IsTrue(col14.SequenceEqual(offDisk.Columns[13].Cast<TimeSpan>()));
            }
        }

        [TestMethod]
        public void PrimitiveFixedSizedArrayWrite()
        {
            var col1 = new double[] { -1, 0, 1 };
            var col2 = new float[] { -2, 0.5f, 2 };
            var col3 = new long[] { long.MinValue, 0, long.MaxValue };
            var col4 = new ulong[] { ulong.MinValue, 1234, ulong.MaxValue };
            var col5 = new int[] { int.MinValue, 0, int.MaxValue };
            var col6 = new uint[] { uint.MinValue, 4321, uint.MaxValue };
            var col7 = new short[] { short.MinValue, 0, short.MaxValue };
            var col8 = new ushort[] { ushort.MinValue, 8642, ushort.MaxValue };
            var col9 = new byte[] { byte.MinValue, 12, byte.MaxValue };
            var col10 = new sbyte[] { sbyte.MinValue, 0, sbyte.MaxValue };
            var col11 = new bool[] { true, false, true };
            var col12 = new DateTime[] { new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc) };
            var col13 = new DateTimeOffset[] { new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc) };
            var col14 = new TimeSpan[] { new TimeSpan(-1000, -23, -59, -48, -37), TimeSpan.Zero, new TimeSpan(1000, 23, 59, 48, 37) };

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Double", col1);
                writer.AddColumn("Float", col2);
                writer.AddColumn("Long", col3);
                writer.AddColumn("ULong", col4);
                writer.AddColumn("Int", col5);
                writer.AddColumn("UInt", col6);
                writer.AddColumn("Short", col7);
                writer.AddColumn("UShort", col8);
                writer.AddColumn("Byte", col9);
                writer.AddColumn("SByte", col10);
                writer.AddColumn("Bool", col11);
                writer.AddColumn("DateTime", col12);
                writer.AddColumn("DateTimeOffset", col13);
                writer.AddColumn("TimeSpan", col14);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(3, offDisk.RowCount);
                Assert.AreEqual(14, offDisk.ColumnCount);

                Assert.AreEqual("Double", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(double), offDisk.Columns[0].Type);
                Assert.AreEqual("Float", offDisk.Columns[1].Name);
                Assert.AreEqual(typeof(float), offDisk.Columns[1].Type);
                Assert.AreEqual("Long", offDisk.Columns[2].Name);
                Assert.AreEqual(typeof(long), offDisk.Columns[2].Type);
                Assert.AreEqual("ULong", offDisk.Columns[3].Name);
                Assert.AreEqual(typeof(ulong), offDisk.Columns[3].Type);
                Assert.AreEqual("Int", offDisk.Columns[4].Name);
                Assert.AreEqual(typeof(int), offDisk.Columns[4].Type);
                Assert.AreEqual("UInt", offDisk.Columns[5].Name);
                Assert.AreEqual(typeof(uint), offDisk.Columns[5].Type);
                Assert.AreEqual("Short", offDisk.Columns[6].Name);
                Assert.AreEqual(typeof(short), offDisk.Columns[6].Type);
                Assert.AreEqual("UShort", offDisk.Columns[7].Name);
                Assert.AreEqual(typeof(ushort), offDisk.Columns[7].Type);
                Assert.AreEqual("Byte", offDisk.Columns[8].Name);
                Assert.AreEqual(typeof(byte), offDisk.Columns[8].Type);
                Assert.AreEqual("SByte", offDisk.Columns[9].Name);
                Assert.AreEqual(typeof(sbyte), offDisk.Columns[9].Type);
                Assert.AreEqual("Bool", offDisk.Columns[10].Name);
                Assert.AreEqual(typeof(bool), offDisk.Columns[10].Type);
                Assert.AreEqual("DateTime", offDisk.Columns[11].Name);
                Assert.AreEqual(typeof(DateTime), offDisk.Columns[11].Type);
                Assert.AreEqual("DateTimeOffset", offDisk.Columns[12].Name);
                Assert.AreEqual(typeof(DateTime), offDisk.Columns[12].Type);    // DateTimeOffset is deserialized as a DateTime (since we UTC everything anyway)
                Assert.AreEqual("TimeSpan", offDisk.Columns[13].Name);
                Assert.AreEqual(typeof(TimeSpan), offDisk.Columns[13].Type);

                Assert.IsTrue(col1.SequenceEqual(offDisk.Columns[0].Cast<double>()));
                Assert.IsTrue(col2.SequenceEqual(offDisk.Columns[1].Cast<float>()));
                Assert.IsTrue(col3.SequenceEqual(offDisk.Columns[2].Cast<long>()));
                Assert.IsTrue(col4.SequenceEqual(offDisk.Columns[3].Cast<ulong>()));
                Assert.IsTrue(col5.SequenceEqual(offDisk.Columns[4].Cast<int>()));
                Assert.IsTrue(col6.SequenceEqual(offDisk.Columns[5].Cast<uint>()));
                Assert.IsTrue(col7.SequenceEqual(offDisk.Columns[6].Cast<short>()));
                Assert.IsTrue(col8.SequenceEqual(offDisk.Columns[7].Cast<ushort>()));
                Assert.IsTrue(col9.SequenceEqual(offDisk.Columns[8].Cast<byte>()));
                Assert.IsTrue(col10.SequenceEqual(offDisk.Columns[9].Cast<sbyte>()));
                Assert.IsTrue(col11.SequenceEqual(offDisk.Columns[10].Cast<bool>()));
                Assert.IsTrue(col12.SequenceEqual(offDisk.Columns[11].Cast<DateTime>()));
                Assert.IsTrue(col13.SequenceEqual(offDisk.Columns[12].Cast<DateTimeOffset>()));
                Assert.IsTrue(col14.SequenceEqual(offDisk.Columns[13].Cast<TimeSpan>()));
            }
        }

        [TestMethod]
        public void StreamPrimitiveFixedSizedArrayNullableWrite()
        {
            var col1 = new double?[] { null, -1, null, 0, null, 1 };
            var col2 = new float?[] { null, -2, null, 0.5f, null, 2 };
            var col3 = new long?[] { null, long.MinValue, null, 0, null, long.MaxValue };
            var col4 = new ulong?[] { null, ulong.MinValue, null, 1234, null, long.MaxValue };
            var col5 = new int?[] { null, int.MinValue, null, 0, null, int.MaxValue };
            var col6 = new uint?[] { null, uint.MinValue, null, 4321, null, uint.MaxValue };
            var col7 = new short?[] { null, short.MinValue, null, 0, null, short.MaxValue };
            var col8 = new ushort?[] { null, ushort.MinValue, null, 8642, null, ushort.MaxValue };
            var col9 = new byte?[] { null, byte.MinValue, null, 12, null, byte.MaxValue };
            var col10 = new sbyte?[] { null, sbyte.MinValue, null, 0, null, sbyte.MaxValue };
            var col11 = new bool?[] { null, true, null, false, null, true };
            var col12 = new DateTime?[] { null, new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc) };
            var col13 = new DateTimeOffset?[] { null, new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc) };
            var col14 = new TimeSpan?[] { null, new TimeSpan(-1000, -23, -59, -48, -37), null, TimeSpan.Zero, null, new TimeSpan(1000, 23, 59, 48, 37) };

            byte[] bytes;
            using (var mem = new MemoryStream())
            {
                using (var writer = new FeatherWriter(mem, WriteMode.Eager))
                {
                    writer.AddColumn("Double", col1);
                    writer.AddColumn("Float", col2);
                    writer.AddColumn("Long", col3);
                    writer.AddColumn("ULong", col4);
                    writer.AddColumn("Int", col5);
                    writer.AddColumn("UInt", col6);
                    writer.AddColumn("Short", col7);
                    writer.AddColumn("UShort", col8);
                    writer.AddColumn("Byte", col9);
                    writer.AddColumn("SByte", col10);
                    writer.AddColumn("Bool", col11);
                    writer.AddColumn("DateTime", col12);
                    writer.AddColumn("DateTimeOffset", col13);
                    writer.AddColumn("TimeSpan", col14);
                }

                bytes = mem.ToArray();
            }

            using (var offDisk = FeatherReader.ReadFromBytes(bytes, BasisType.Zero))
            {
                Assert.AreEqual(6, offDisk.RowCount);
                Assert.AreEqual(14, offDisk.ColumnCount);

                Assert.AreEqual("Double", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(double?), offDisk.Columns[0].Type);
                Assert.AreEqual("Float", offDisk.Columns[1].Name);
                Assert.AreEqual(typeof(float?), offDisk.Columns[1].Type);
                Assert.AreEqual("Long", offDisk.Columns[2].Name);
                Assert.AreEqual(typeof(long?), offDisk.Columns[2].Type);
                Assert.AreEqual("ULong", offDisk.Columns[3].Name);
                Assert.AreEqual(typeof(ulong?), offDisk.Columns[3].Type);
                Assert.AreEqual("Int", offDisk.Columns[4].Name);
                Assert.AreEqual(typeof(int?), offDisk.Columns[4].Type);
                Assert.AreEqual("UInt", offDisk.Columns[5].Name);
                Assert.AreEqual(typeof(uint?), offDisk.Columns[5].Type);
                Assert.AreEqual("Short", offDisk.Columns[6].Name);
                Assert.AreEqual(typeof(short?), offDisk.Columns[6].Type);
                Assert.AreEqual("UShort", offDisk.Columns[7].Name);
                Assert.AreEqual(typeof(ushort?), offDisk.Columns[7].Type);
                Assert.AreEqual("Byte", offDisk.Columns[8].Name);
                Assert.AreEqual(typeof(byte?), offDisk.Columns[8].Type);
                Assert.AreEqual("SByte", offDisk.Columns[9].Name);
                Assert.AreEqual(typeof(sbyte?), offDisk.Columns[9].Type);
                Assert.AreEqual("Bool", offDisk.Columns[10].Name);
                Assert.AreEqual(typeof(bool?), offDisk.Columns[10].Type);
                Assert.AreEqual("DateTime", offDisk.Columns[11].Name);
                Assert.AreEqual(typeof(DateTime?), offDisk.Columns[11].Type);
                Assert.AreEqual("DateTimeOffset", offDisk.Columns[12].Name);
                Assert.AreEqual(typeof(DateTime?), offDisk.Columns[12].Type);    // DateTimeOffset is deserialized as a DateTime (since we UTC everything anyway)
                Assert.AreEqual("TimeSpan", offDisk.Columns[13].Name);
                Assert.AreEqual(typeof(TimeSpan?), offDisk.Columns[13].Type);

                Assert.IsTrue(col1.SequenceEqual(offDisk.Columns[0].Cast<double?>()));
                Assert.IsTrue(col2.SequenceEqual(offDisk.Columns[1].Cast<float?>()));
                Assert.IsTrue(col3.SequenceEqual(offDisk.Columns[2].Cast<long?>()));
                Assert.IsTrue(col4.SequenceEqual(offDisk.Columns[3].Cast<ulong?>()));
                Assert.IsTrue(col5.SequenceEqual(offDisk.Columns[4].Cast<int?>()));
                Assert.IsTrue(col6.SequenceEqual(offDisk.Columns[5].Cast<uint?>()));
                Assert.IsTrue(col7.SequenceEqual(offDisk.Columns[6].Cast<short?>()));
                Assert.IsTrue(col8.SequenceEqual(offDisk.Columns[7].Cast<ushort?>()));
                Assert.IsTrue(col9.SequenceEqual(offDisk.Columns[8].Cast<byte?>()));
                Assert.IsTrue(col10.SequenceEqual(offDisk.Columns[9].Cast<sbyte?>()));
                Assert.IsTrue(col11.SequenceEqual(offDisk.Columns[10].Cast<bool?>()));
                Assert.IsTrue(col12.SequenceEqual(offDisk.Columns[11].Cast<DateTime?>()));
                Assert.IsTrue(col13.SequenceEqual(offDisk.Columns[12].Cast<DateTimeOffset?>()));
                Assert.IsTrue(col14.SequenceEqual(offDisk.Columns[13].Cast<TimeSpan?>()));
            }
        }

        [TestMethod]
        public void PrimitiveFixedSizedArrayNullableWrite()
        {
            var col1 = new double?[] { null, -1, null, 0, null, 1 };
            var col2 = new float?[] { null, -2, null, 0.5f, null, 2 };
            var col3 = new long?[] { null, long.MinValue, null, 0, null, long.MaxValue };
            var col4 = new ulong?[] { null, ulong.MinValue, null, 1234, null, long.MaxValue };
            var col5 = new int?[] { null, int.MinValue, null, 0, null, int.MaxValue };
            var col6 = new uint?[] { null, uint.MinValue, null, 4321, null, uint.MaxValue };
            var col7 = new short?[] { null, short.MinValue, null, 0, null, short.MaxValue };
            var col8 = new ushort?[] { null, ushort.MinValue, null, 8642, null, ushort.MaxValue };
            var col9 = new byte?[] { null, byte.MinValue, null, 12, null, byte.MaxValue };
            var col10 = new sbyte?[] { null, sbyte.MinValue, null, 0, null, sbyte.MaxValue };
            var col11 = new bool?[] { null, true, null, false, null, true };
            var col12 = new DateTime?[] { null, new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc) };
            var col13 = new DateTimeOffset?[] { null, new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc) };
            var col14 = new TimeSpan?[] { null, new TimeSpan(-1000, -23, -59, -48, -37), null, TimeSpan.Zero, null, new TimeSpan(1000, 23, 59, 48, 37) };

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Double", col1);
                writer.AddColumn("Float", col2);
                writer.AddColumn("Long", col3);
                writer.AddColumn("ULong", col4);
                writer.AddColumn("Int", col5);
                writer.AddColumn("UInt", col6);
                writer.AddColumn("Short", col7);
                writer.AddColumn("UShort", col8);
                writer.AddColumn("Byte", col9);
                writer.AddColumn("SByte", col10);
                writer.AddColumn("Bool", col11);
                writer.AddColumn("DateTime", col12);
                writer.AddColumn("DateTimeOffset", col13);
                writer.AddColumn("TimeSpan", col14);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(6, offDisk.RowCount);
                Assert.AreEqual(14, offDisk.ColumnCount);

                Assert.AreEqual("Double", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(double?), offDisk.Columns[0].Type);
                Assert.AreEqual("Float", offDisk.Columns[1].Name);
                Assert.AreEqual(typeof(float?), offDisk.Columns[1].Type);
                Assert.AreEqual("Long", offDisk.Columns[2].Name);
                Assert.AreEqual(typeof(long?), offDisk.Columns[2].Type);
                Assert.AreEqual("ULong", offDisk.Columns[3].Name);
                Assert.AreEqual(typeof(ulong?), offDisk.Columns[3].Type);
                Assert.AreEqual("Int", offDisk.Columns[4].Name);
                Assert.AreEqual(typeof(int?), offDisk.Columns[4].Type);
                Assert.AreEqual("UInt", offDisk.Columns[5].Name);
                Assert.AreEqual(typeof(uint?), offDisk.Columns[5].Type);
                Assert.AreEqual("Short", offDisk.Columns[6].Name);
                Assert.AreEqual(typeof(short?), offDisk.Columns[6].Type);
                Assert.AreEqual("UShort", offDisk.Columns[7].Name);
                Assert.AreEqual(typeof(ushort?), offDisk.Columns[7].Type);
                Assert.AreEqual("Byte", offDisk.Columns[8].Name);
                Assert.AreEqual(typeof(byte?), offDisk.Columns[8].Type);
                Assert.AreEqual("SByte", offDisk.Columns[9].Name);
                Assert.AreEqual(typeof(sbyte?), offDisk.Columns[9].Type);
                Assert.AreEqual("Bool", offDisk.Columns[10].Name);
                Assert.AreEqual(typeof(bool?), offDisk.Columns[10].Type);
                Assert.AreEqual("DateTime", offDisk.Columns[11].Name);
                Assert.AreEqual(typeof(DateTime?), offDisk.Columns[11].Type);
                Assert.AreEqual("DateTimeOffset", offDisk.Columns[12].Name);
                Assert.AreEqual(typeof(DateTime?), offDisk.Columns[12].Type);    // DateTimeOffset is deserialized as a DateTime (since we UTC everything anyway)
                Assert.AreEqual("TimeSpan", offDisk.Columns[13].Name);
                Assert.AreEqual(typeof(TimeSpan?), offDisk.Columns[13].Type);

                Assert.IsTrue(col1.SequenceEqual(offDisk.Columns[0].Cast<double?>()));
                Assert.IsTrue(col2.SequenceEqual(offDisk.Columns[1].Cast<float?>()));
                Assert.IsTrue(col3.SequenceEqual(offDisk.Columns[2].Cast<long?>()));
                Assert.IsTrue(col4.SequenceEqual(offDisk.Columns[3].Cast<ulong?>()));
                Assert.IsTrue(col5.SequenceEqual(offDisk.Columns[4].Cast<int?>()));
                Assert.IsTrue(col6.SequenceEqual(offDisk.Columns[5].Cast<uint?>()));
                Assert.IsTrue(col7.SequenceEqual(offDisk.Columns[6].Cast<short?>()));
                Assert.IsTrue(col8.SequenceEqual(offDisk.Columns[7].Cast<ushort?>()));
                Assert.IsTrue(col9.SequenceEqual(offDisk.Columns[8].Cast<byte?>()));
                Assert.IsTrue(col10.SequenceEqual(offDisk.Columns[9].Cast<sbyte?>()));
                Assert.IsTrue(col11.SequenceEqual(offDisk.Columns[10].Cast<bool?>()));
                Assert.IsTrue(col12.SequenceEqual(offDisk.Columns[11].Cast<DateTime?>()));
                Assert.IsTrue(col13.SequenceEqual(offDisk.Columns[12].Cast<DateTimeOffset?>()));
                Assert.IsTrue(col14.SequenceEqual(offDisk.Columns[13].Cast<TimeSpan?>()));
            }
        }

        [TestMethod]
        public void PrimitiveFixedSizedCollectionsWrite()
        {
            var col1 = new List<double> { -1, 0, 1 };
            var col2 = new List<float> { -2, 0.5f, 2 };
            var col3 = new List<long> { long.MinValue, 0, long.MaxValue };
            var col4 = new List<ulong> { ulong.MinValue, 1234, ulong.MaxValue };
            var col5 = new List<int> { int.MinValue, 0, int.MaxValue };
            var col6 = new List<uint> { uint.MinValue, 4321, uint.MaxValue };
            var col7 = new List<short> { short.MinValue, 0, short.MaxValue };
            var col8 = new List<ushort> { ushort.MinValue, 8642, ushort.MaxValue };
            var col9 = new List<byte> { byte.MinValue, 12, byte.MaxValue };
            var col10 = new List<sbyte> { sbyte.MinValue, 0, sbyte.MaxValue };
            var col11 = new List<bool> { true, false, true };
            var col12 = new List<DateTime> { new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc) };
            var col13 = new List<DateTimeOffset> { new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc) };
            var col14 = new List<TimeSpan> { new TimeSpan(-1000, -23, -59, -48, -37), TimeSpan.Zero, new TimeSpan(1000, 23, 59, 48, 37) };

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Double", col1);
                writer.AddColumn("Float", col2);
                writer.AddColumn("Long", col3);
                writer.AddColumn("ULong", col4);
                writer.AddColumn("Int", col5);
                writer.AddColumn("UInt", col6);
                writer.AddColumn("Short", col7);
                writer.AddColumn("UShort", col8);
                writer.AddColumn("Byte", col9);
                writer.AddColumn("SByte", col10);
                writer.AddColumn("Bool", col11);
                writer.AddColumn("DateTime", col12);
                writer.AddColumn("DateTimeOffset", col13);
                writer.AddColumn("TimeSpan", col14);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(3, offDisk.RowCount);
                Assert.AreEqual(14, offDisk.ColumnCount);

                Assert.AreEqual("Double", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(double), offDisk.Columns[0].Type);
                Assert.AreEqual("Float", offDisk.Columns[1].Name);
                Assert.AreEqual(typeof(float), offDisk.Columns[1].Type);
                Assert.AreEqual("Long", offDisk.Columns[2].Name);
                Assert.AreEqual(typeof(long), offDisk.Columns[2].Type);
                Assert.AreEqual("ULong", offDisk.Columns[3].Name);
                Assert.AreEqual(typeof(ulong), offDisk.Columns[3].Type);
                Assert.AreEqual("Int", offDisk.Columns[4].Name);
                Assert.AreEqual(typeof(int), offDisk.Columns[4].Type);
                Assert.AreEqual("UInt", offDisk.Columns[5].Name);
                Assert.AreEqual(typeof(uint), offDisk.Columns[5].Type);
                Assert.AreEqual("Short", offDisk.Columns[6].Name);
                Assert.AreEqual(typeof(short), offDisk.Columns[6].Type);
                Assert.AreEqual("UShort", offDisk.Columns[7].Name);
                Assert.AreEqual(typeof(ushort), offDisk.Columns[7].Type);
                Assert.AreEqual("Byte", offDisk.Columns[8].Name);
                Assert.AreEqual(typeof(byte), offDisk.Columns[8].Type);
                Assert.AreEqual("SByte", offDisk.Columns[9].Name);
                Assert.AreEqual(typeof(sbyte), offDisk.Columns[9].Type);
                Assert.AreEqual("Bool", offDisk.Columns[10].Name);
                Assert.AreEqual(typeof(bool), offDisk.Columns[10].Type);
                Assert.AreEqual("DateTime", offDisk.Columns[11].Name);
                Assert.AreEqual(typeof(DateTime), offDisk.Columns[11].Type);
                Assert.AreEqual("DateTimeOffset", offDisk.Columns[12].Name);
                Assert.AreEqual(typeof(DateTime), offDisk.Columns[12].Type);    // DateTimeOffset is deserialized as a DateTime (since we UTC everything anyway)
                Assert.AreEqual("TimeSpan", offDisk.Columns[13].Name);
                Assert.AreEqual(typeof(TimeSpan), offDisk.Columns[13].Type);

                Assert.IsTrue(col1.SequenceEqual(offDisk.Columns[0].Cast<double>()));
                Assert.IsTrue(col2.SequenceEqual(offDisk.Columns[1].Cast<float>()));
                Assert.IsTrue(col3.SequenceEqual(offDisk.Columns[2].Cast<long>()));
                Assert.IsTrue(col4.SequenceEqual(offDisk.Columns[3].Cast<ulong>()));
                Assert.IsTrue(col5.SequenceEqual(offDisk.Columns[4].Cast<int>()));
                Assert.IsTrue(col6.SequenceEqual(offDisk.Columns[5].Cast<uint>()));
                Assert.IsTrue(col7.SequenceEqual(offDisk.Columns[6].Cast<short>()));
                Assert.IsTrue(col8.SequenceEqual(offDisk.Columns[7].Cast<ushort>()));
                Assert.IsTrue(col9.SequenceEqual(offDisk.Columns[8].Cast<byte>()));
                Assert.IsTrue(col10.SequenceEqual(offDisk.Columns[9].Cast<sbyte>()));
                Assert.IsTrue(col11.SequenceEqual(offDisk.Columns[10].Cast<bool>()));
                Assert.IsTrue(col12.SequenceEqual(offDisk.Columns[11].Cast<DateTime>()));
                Assert.IsTrue(col13.SequenceEqual(offDisk.Columns[12].Cast<DateTimeOffset>()));
                Assert.IsTrue(col14.SequenceEqual(offDisk.Columns[13].Cast<TimeSpan>()));
            }
        }

        [TestMethod]
        public void PrimitiveFixedSizedCollectionNullableWrite()
        {
            var col1 = new List<double?> { null, -1, null, 0, null, 1 };
            var col2 = new List<float?> { null, -2, null, 0.5f, null, 2 };
            var col3 = new List<long?> { null, long.MinValue, null, 0, null, long.MaxValue };
            var col4 = new List<ulong?> { null, ulong.MinValue, null, 1234, null, long.MaxValue };
            var col5 = new List<int?> { null, int.MinValue, null, 0, null, int.MaxValue };
            var col6 = new List<uint?> { null, uint.MinValue, null, 4321, null, uint.MaxValue };
            var col7 = new List<short?> { null, short.MinValue, null, 0, null, short.MaxValue };
            var col8 = new List<ushort?> { null, ushort.MinValue, null, 8642, null, ushort.MaxValue };
            var col9 = new List<byte?> { null, byte.MinValue, null, 12, null, byte.MaxValue };
            var col10 = new List<sbyte?> { null, sbyte.MinValue, null, 0, null, sbyte.MaxValue };
            var col11 = new List<bool?> { null, true, null, false, null, true };
            var col12 = new List<DateTime?> { null, new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc) };
            var col13 = new List<DateTimeOffset?> { null, new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc) };
            var col14 = new List<TimeSpan?> { null, new TimeSpan(-1000, -23, -59, -48, -37), null, TimeSpan.Zero, null, new TimeSpan(1000, 23, 59, 48, 37) };

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Double", col1);
                writer.AddColumn("Float", col2);
                writer.AddColumn("Long", col3);
                writer.AddColumn("ULong", col4);
                writer.AddColumn("Int", col5);
                writer.AddColumn("UInt", col6);
                writer.AddColumn("Short", col7);
                writer.AddColumn("UShort", col8);
                writer.AddColumn("Byte", col9);
                writer.AddColumn("SByte", col10);
                writer.AddColumn("Bool", col11);
                writer.AddColumn("DateTime", col12);
                writer.AddColumn("DateTimeOffset", col13);
                writer.AddColumn("TimeSpan", col14);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(6, offDisk.RowCount);
                Assert.AreEqual(14, offDisk.ColumnCount);

                Assert.AreEqual("Double", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(double?), offDisk.Columns[0].Type);
                Assert.AreEqual("Float", offDisk.Columns[1].Name);
                Assert.AreEqual(typeof(float?), offDisk.Columns[1].Type);
                Assert.AreEqual("Long", offDisk.Columns[2].Name);
                Assert.AreEqual(typeof(long?), offDisk.Columns[2].Type);
                Assert.AreEqual("ULong", offDisk.Columns[3].Name);
                Assert.AreEqual(typeof(ulong?), offDisk.Columns[3].Type);
                Assert.AreEqual("Int", offDisk.Columns[4].Name);
                Assert.AreEqual(typeof(int?), offDisk.Columns[4].Type);
                Assert.AreEqual("UInt", offDisk.Columns[5].Name);
                Assert.AreEqual(typeof(uint?), offDisk.Columns[5].Type);
                Assert.AreEqual("Short", offDisk.Columns[6].Name);
                Assert.AreEqual(typeof(short?), offDisk.Columns[6].Type);
                Assert.AreEqual("UShort", offDisk.Columns[7].Name);
                Assert.AreEqual(typeof(ushort?), offDisk.Columns[7].Type);
                Assert.AreEqual("Byte", offDisk.Columns[8].Name);
                Assert.AreEqual(typeof(byte?), offDisk.Columns[8].Type);
                Assert.AreEqual("SByte", offDisk.Columns[9].Name);
                Assert.AreEqual(typeof(sbyte?), offDisk.Columns[9].Type);
                Assert.AreEqual("Bool", offDisk.Columns[10].Name);
                Assert.AreEqual(typeof(bool?), offDisk.Columns[10].Type);
                Assert.AreEqual("DateTime", offDisk.Columns[11].Name);
                Assert.AreEqual(typeof(DateTime?), offDisk.Columns[11].Type);
                Assert.AreEqual("DateTimeOffset", offDisk.Columns[12].Name);
                Assert.AreEqual(typeof(DateTime?), offDisk.Columns[12].Type);    // DateTimeOffset is deserialized as a DateTime (since we UTC everything anyway)
                Assert.AreEqual("TimeSpan", offDisk.Columns[13].Name);
                Assert.AreEqual(typeof(TimeSpan?), offDisk.Columns[13].Type);

                Assert.IsTrue(col1.SequenceEqual(offDisk.Columns[0].Cast<double?>()));
                Assert.IsTrue(col2.SequenceEqual(offDisk.Columns[1].Cast<float?>()));
                Assert.IsTrue(col3.SequenceEqual(offDisk.Columns[2].Cast<long?>()));
                Assert.IsTrue(col4.SequenceEqual(offDisk.Columns[3].Cast<ulong?>()));
                Assert.IsTrue(col5.SequenceEqual(offDisk.Columns[4].Cast<int?>()));
                Assert.IsTrue(col6.SequenceEqual(offDisk.Columns[5].Cast<uint?>()));
                Assert.IsTrue(col7.SequenceEqual(offDisk.Columns[6].Cast<short?>()));
                Assert.IsTrue(col8.SequenceEqual(offDisk.Columns[7].Cast<ushort?>()));
                Assert.IsTrue(col9.SequenceEqual(offDisk.Columns[8].Cast<byte?>()));
                Assert.IsTrue(col10.SequenceEqual(offDisk.Columns[9].Cast<sbyte?>()));
                Assert.IsTrue(col11.SequenceEqual(offDisk.Columns[10].Cast<bool?>()));
                Assert.IsTrue(col12.SequenceEqual(offDisk.Columns[11].Cast<DateTime?>()));
                Assert.IsTrue(col13.SequenceEqual(offDisk.Columns[12].Cast<DateTimeOffset?>()));
                Assert.IsTrue(col14.SequenceEqual(offDisk.Columns[13].Cast<TimeSpan?>()));
            }
        }

        static IEnumerable<T> _PrimitiveFixedSizedEnumerableWrite<T>(params T[] elems)
        {
            foreach (var e in elems)
            {
                yield return e;
            }
        }

        [TestMethod]
        public void PrimitiveFixedSizedEnumerableWrite()
        {
            var col1 = _PrimitiveFixedSizedEnumerableWrite<double>(-1, 0, 1);
            var col2 = _PrimitiveFixedSizedEnumerableWrite(-2, 0.5f, 2);
            var col3 = _PrimitiveFixedSizedEnumerableWrite(long.MinValue, 0, long.MaxValue);
            var col4 = _PrimitiveFixedSizedEnumerableWrite(ulong.MinValue, 1234UL, ulong.MaxValue);
            var col5 = _PrimitiveFixedSizedEnumerableWrite(int.MinValue, 0, int.MaxValue);
            var col6 = _PrimitiveFixedSizedEnumerableWrite(uint.MinValue, 4321U, uint.MaxValue);
            var col7 = _PrimitiveFixedSizedEnumerableWrite<short>(short.MinValue, 0, short.MaxValue);
            var col8 = _PrimitiveFixedSizedEnumerableWrite<ushort>(ushort.MinValue, 8642, ushort.MaxValue);
            var col9 = _PrimitiveFixedSizedEnumerableWrite<byte>(byte.MinValue, 12, byte.MaxValue);
            var col10 = _PrimitiveFixedSizedEnumerableWrite<sbyte>(sbyte.MinValue, 0, sbyte.MaxValue);
            var col11 = _PrimitiveFixedSizedEnumerableWrite<bool>(true, false, true);
            var col12 = _PrimitiveFixedSizedEnumerableWrite<DateTime>(new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc));
            var col13 = _PrimitiveFixedSizedEnumerableWrite<DateTimeOffset>(new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc));
            var col14 = _PrimitiveFixedSizedEnumerableWrite<TimeSpan>(new TimeSpan(-1000, -23, -59, -48, -37), TimeSpan.Zero, new TimeSpan(1000, 23, 59, 48, 37));

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Double", col1);
                writer.AddColumn("Float", col2);
                writer.AddColumn("Long", col3);
                writer.AddColumn("ULong", col4);
                writer.AddColumn("Int", col5);
                writer.AddColumn("UInt", col6);
                writer.AddColumn("Short", col7);
                writer.AddColumn("UShort", col8);
                writer.AddColumn("Byte", col9);
                writer.AddColumn("SByte", col10);
                writer.AddColumn("Bool", col11);
                writer.AddColumn("DateTime", col12);
                writer.AddColumn("DateTimeOffset", col13);
                writer.AddColumn("TimeSpan", col14);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(3, offDisk.RowCount);
                Assert.AreEqual(14, offDisk.ColumnCount);

                Assert.AreEqual("Double", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(double), offDisk.Columns[0].Type);
                Assert.AreEqual("Float", offDisk.Columns[1].Name);
                Assert.AreEqual(typeof(float), offDisk.Columns[1].Type);
                Assert.AreEqual("Long", offDisk.Columns[2].Name);
                Assert.AreEqual(typeof(long), offDisk.Columns[2].Type);
                Assert.AreEqual("ULong", offDisk.Columns[3].Name);
                Assert.AreEqual(typeof(ulong), offDisk.Columns[3].Type);
                Assert.AreEqual("Int", offDisk.Columns[4].Name);
                Assert.AreEqual(typeof(int), offDisk.Columns[4].Type);
                Assert.AreEqual("UInt", offDisk.Columns[5].Name);
                Assert.AreEqual(typeof(uint), offDisk.Columns[5].Type);
                Assert.AreEqual("Short", offDisk.Columns[6].Name);
                Assert.AreEqual(typeof(short), offDisk.Columns[6].Type);
                Assert.AreEqual("UShort", offDisk.Columns[7].Name);
                Assert.AreEqual(typeof(ushort), offDisk.Columns[7].Type);
                Assert.AreEqual("Byte", offDisk.Columns[8].Name);
                Assert.AreEqual(typeof(byte), offDisk.Columns[8].Type);
                Assert.AreEqual("SByte", offDisk.Columns[9].Name);
                Assert.AreEqual(typeof(sbyte), offDisk.Columns[9].Type);
                Assert.AreEqual("Bool", offDisk.Columns[10].Name);
                Assert.AreEqual(typeof(bool), offDisk.Columns[10].Type);
                Assert.AreEqual("DateTime", offDisk.Columns[11].Name);
                Assert.AreEqual(typeof(DateTime), offDisk.Columns[11].Type);
                Assert.AreEqual("DateTimeOffset", offDisk.Columns[12].Name);
                Assert.AreEqual(typeof(DateTime), offDisk.Columns[12].Type);    // DateTimeOffset is deserialized as a DateTime (since we UTC everything anyway)
                Assert.AreEqual("TimeSpan", offDisk.Columns[13].Name);
                Assert.AreEqual(typeof(TimeSpan), offDisk.Columns[13].Type);

                Assert.IsTrue(col1.SequenceEqual(offDisk.Columns[0].Cast<double>()));
                Assert.IsTrue(col2.SequenceEqual(offDisk.Columns[1].Cast<float>()));
                Assert.IsTrue(col3.SequenceEqual(offDisk.Columns[2].Cast<long>()));
                Assert.IsTrue(col4.SequenceEqual(offDisk.Columns[3].Cast<ulong>()));
                Assert.IsTrue(col5.SequenceEqual(offDisk.Columns[4].Cast<int>()));
                Assert.IsTrue(col6.SequenceEqual(offDisk.Columns[5].Cast<uint>()));
                Assert.IsTrue(col7.SequenceEqual(offDisk.Columns[6].Cast<short>()));
                Assert.IsTrue(col8.SequenceEqual(offDisk.Columns[7].Cast<ushort>()));
                Assert.IsTrue(col9.SequenceEqual(offDisk.Columns[8].Cast<byte>()));
                Assert.IsTrue(col10.SequenceEqual(offDisk.Columns[9].Cast<sbyte>()));
                Assert.IsTrue(col11.SequenceEqual(offDisk.Columns[10].Cast<bool>()));
                Assert.IsTrue(col12.SequenceEqual(offDisk.Columns[11].Cast<DateTime>()));
                Assert.IsTrue(col13.SequenceEqual(offDisk.Columns[12].Cast<DateTimeOffset>()));
                Assert.IsTrue(col14.SequenceEqual(offDisk.Columns[13].Cast<TimeSpan>()));
            }
        }

        [TestMethod]
        public void PrimitiveFixedSizedEnumerableNullableWrite()
        {
            var col1 = _PrimitiveFixedSizedEnumerableWrite<double?>(null, -1, null, 0, null, 1);
            var col2 = _PrimitiveFixedSizedEnumerableWrite<float?>(null, -2, null, 0.5f, null, 2);
            var col3 = _PrimitiveFixedSizedEnumerableWrite<long?>(null, long.MinValue, null, 0, null, long.MaxValue);
            var col4 = _PrimitiveFixedSizedEnumerableWrite<ulong?>(null, ulong.MinValue, null, 1234, null, long.MaxValue);
            var col5 = _PrimitiveFixedSizedEnumerableWrite<int?>(null, int.MinValue, null, 0, null, int.MaxValue);
            var col6 = _PrimitiveFixedSizedEnumerableWrite<uint?>(null, uint.MinValue, null, 4321, null, uint.MaxValue);
            var col7 = _PrimitiveFixedSizedEnumerableWrite<short?>(null, short.MinValue, null, 0, null, short.MaxValue);
            var col8 = _PrimitiveFixedSizedEnumerableWrite<ushort?>(null, ushort.MinValue, null, 8642, null, ushort.MaxValue);
            var col9 = _PrimitiveFixedSizedEnumerableWrite<byte?>(null, byte.MinValue, null, 12, null, byte.MaxValue);
            var col10 = _PrimitiveFixedSizedEnumerableWrite<sbyte?>(null, sbyte.MinValue, null, 0, null, sbyte.MaxValue);
            var col11 = _PrimitiveFixedSizedEnumerableWrite<bool?>(null, true, null, false, null, true);
            var col12 = _PrimitiveFixedSizedEnumerableWrite<DateTime?>(null, new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc));
            var col13 = _PrimitiveFixedSizedEnumerableWrite<DateTimeOffset?>(null, new DateTime(1234, 5, 6, 7, 8, 9, 20, DateTimeKind.Utc), null, new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, new DateTime(5678, 9, 10, 11, 12, 13, 14, DateTimeKind.Utc));
            var col14 = _PrimitiveFixedSizedEnumerableWrite<TimeSpan?>(null, new TimeSpan(-1000, -23, -59, -48, -37), null, TimeSpan.Zero, null, new TimeSpan(1000, 23, 59, 48, 37));

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Double", col1);
                writer.AddColumn("Float", col2);
                writer.AddColumn("Long", col3);
                writer.AddColumn("ULong", col4);
                writer.AddColumn("Int", col5);
                writer.AddColumn("UInt", col6);
                writer.AddColumn("Short", col7);
                writer.AddColumn("UShort", col8);
                writer.AddColumn("Byte", col9);
                writer.AddColumn("SByte", col10);
                writer.AddColumn("Bool", col11);
                writer.AddColumn("DateTime", col12);
                writer.AddColumn("DateTimeOffset", col13);
                writer.AddColumn("TimeSpan", col14);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(6, offDisk.RowCount);
                Assert.AreEqual(14, offDisk.ColumnCount);

                Assert.AreEqual("Double", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(double?), offDisk.Columns[0].Type);
                Assert.AreEqual("Float", offDisk.Columns[1].Name);
                Assert.AreEqual(typeof(float?), offDisk.Columns[1].Type);
                Assert.AreEqual("Long", offDisk.Columns[2].Name);
                Assert.AreEqual(typeof(long?), offDisk.Columns[2].Type);
                Assert.AreEqual("ULong", offDisk.Columns[3].Name);
                Assert.AreEqual(typeof(ulong?), offDisk.Columns[3].Type);
                Assert.AreEqual("Int", offDisk.Columns[4].Name);
                Assert.AreEqual(typeof(int?), offDisk.Columns[4].Type);
                Assert.AreEqual("UInt", offDisk.Columns[5].Name);
                Assert.AreEqual(typeof(uint?), offDisk.Columns[5].Type);
                Assert.AreEqual("Short", offDisk.Columns[6].Name);
                Assert.AreEqual(typeof(short?), offDisk.Columns[6].Type);
                Assert.AreEqual("UShort", offDisk.Columns[7].Name);
                Assert.AreEqual(typeof(ushort?), offDisk.Columns[7].Type);
                Assert.AreEqual("Byte", offDisk.Columns[8].Name);
                Assert.AreEqual(typeof(byte?), offDisk.Columns[8].Type);
                Assert.AreEqual("SByte", offDisk.Columns[9].Name);
                Assert.AreEqual(typeof(sbyte?), offDisk.Columns[9].Type);
                Assert.AreEqual("Bool", offDisk.Columns[10].Name);
                Assert.AreEqual(typeof(bool?), offDisk.Columns[10].Type);
                Assert.AreEqual("DateTime", offDisk.Columns[11].Name);
                Assert.AreEqual(typeof(DateTime?), offDisk.Columns[11].Type);
                Assert.AreEqual("DateTimeOffset", offDisk.Columns[12].Name);
                Assert.AreEqual(typeof(DateTime?), offDisk.Columns[12].Type);    // DateTimeOffset is deserialized as a DateTime (since we UTC everything anyway)
                Assert.AreEqual("TimeSpan", offDisk.Columns[13].Name);
                Assert.AreEqual(typeof(TimeSpan?), offDisk.Columns[13].Type);

                Assert.IsTrue(col1.SequenceEqual(offDisk.Columns[0].Cast<double?>()));
                Assert.IsTrue(col2.SequenceEqual(offDisk.Columns[1].Cast<float?>()));
                Assert.IsTrue(col3.SequenceEqual(offDisk.Columns[2].Cast<long?>()));
                Assert.IsTrue(col4.SequenceEqual(offDisk.Columns[3].Cast<ulong?>()));
                Assert.IsTrue(col5.SequenceEqual(offDisk.Columns[4].Cast<int?>()));
                Assert.IsTrue(col6.SequenceEqual(offDisk.Columns[5].Cast<uint?>()));
                Assert.IsTrue(col7.SequenceEqual(offDisk.Columns[6].Cast<short?>()));
                Assert.IsTrue(col8.SequenceEqual(offDisk.Columns[7].Cast<ushort?>()));
                Assert.IsTrue(col9.SequenceEqual(offDisk.Columns[8].Cast<byte?>()));
                Assert.IsTrue(col10.SequenceEqual(offDisk.Columns[9].Cast<sbyte?>()));
                Assert.IsTrue(col11.SequenceEqual(offDisk.Columns[10].Cast<bool?>()));
                Assert.IsTrue(col12.SequenceEqual(offDisk.Columns[11].Cast<DateTime?>()));
                Assert.IsTrue(col13.SequenceEqual(offDisk.Columns[12].Cast<DateTimeOffset?>()));
                Assert.IsTrue(col14.SequenceEqual(offDisk.Columns[13].Cast<TimeSpan?>()));
            }
        }

        [TestMethod]
        public void StreamStringArrayWrite()
        {
            var col = new string[] { "hello", "world", "foobar", "" };

            byte[] bytes;
            using (var mem = new MemoryStream())
            {
                using (var writer = new FeatherWriter(mem, WriteMode.Eager))
                {
                    writer.AddColumn("String", col);
                }
                bytes = mem.ToArray();
            }

            using (var offDisk = FeatherReader.ReadFromBytes(bytes, BasisType.Zero))
            {
                var foo = offDisk.Columns[0].Cast<string>().ToList();

                Assert.AreEqual(4, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("String", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(string), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<string>()));
            }
        }

        [TestMethod]
        public void StringArrayWrite()
        {
            var col = new string[] { "hello", "world", "foobar", "" };

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("String", col);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                var foo = offDisk.Columns[0].Cast<string>().ToList();

                Assert.AreEqual(4, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("String", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(string), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<string>()));
            }
        }

        [TestMethod]
        public void StringCollectionWrite()
        {
            var col = new List<string> { "hello", "world", "foobar", "" };

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("String", col);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                var foo = offDisk.Columns[0].Cast<string>().ToList();

                Assert.AreEqual(4, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("String", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(string), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<string>()));
            }
        }

        [TestMethod]
        public void StringIEnumerableWrite()
        {
            var col = _PrimitiveFixedSizedEnumerableWrite("hello", "world", "foobar", "");

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("String", col);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                var foo = offDisk.Columns[0].Cast<string>().ToList();

                Assert.AreEqual(4, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("String", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(string), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<string>()));
            }
        }

        [TestMethod]
        public void StreamStringNullableArrayWrite()
        {
            var col = new string[] { "hello", null, "foobar", "" };

            byte[] bytes;
            using (var mem = new MemoryStream())
            {
                using (var writer = new FeatherWriter(mem, WriteMode.Eager))
                {
                    writer.AddColumn("NullableString", col);
                }
                bytes = mem.ToArray();
            }

            using (var offDisk = FeatherReader.ReadFromBytes(bytes, BasisType.Zero))
            {
                Assert.AreEqual(4, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("NullableString", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(string), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<string>()));
            }
        }

        [TestMethod]
        public void StringNullableArrayWrite()
        {
            var col = new string[] { "hello", null, "foobar", "" };

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("NullableString", col);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(4, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("NullableString", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(string), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<string>()));
            }
        }

        [TestMethod]
        public void StringNullableCollectionWrite()
        {
            var col = new List<string> { "hello", null, "foobar", "" };

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("NullableString", col);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(4, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("NullableString", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(string), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<string>()));
            }
        }

        [TestMethod]
        public void StringNullableIEnumerableWrite()
        {
            var col = _PrimitiveFixedSizedEnumerableWrite("hello", null, "foobar", "");

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("NullableString", col);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(4, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("NullableString", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(string), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<string>()));
            }
        }

        enum _Enum1
        {
            None = 0,

            Foo = 23,

            Bar = 34,

            Hello = 100,

            World = 256,

            Dupe
        }

        [TestMethod]
        public void EnumArrayWrite()
        {
            var col = new _Enum1[] { _Enum1.None, _Enum1.World, _Enum1.Hello, _Enum1.Bar, _Enum1.Foo };

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Enum", col);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(5, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("Enum", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(int), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<_Enum1>()));
            }
        }

        [TestMethod]
        public void NullableEnumArrayWrite()
        {
            var col = new _Enum1?[] { _Enum1.None, null, _Enum1.Hello, null, _Enum1.Foo };

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Enum", col);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(5, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("Enum", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(int?), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<_Enum1?>()));
            }
        }

        [TestMethod]
        public void EnumCollectionWrite()
        {
            var col = new List<_Enum1> { _Enum1.None, _Enum1.World, _Enum1.Hello, _Enum1.Bar, _Enum1.Foo };

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Enum", col);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(5, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("Enum", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(int), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<_Enum1>()));
            }
        }

        [TestMethod]
        public void NullableEnumCollectionWrite()
        {
            var col = new List<_Enum1?> { _Enum1.None, null, _Enum1.Hello, null, _Enum1.Foo };

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Enum", col);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(5, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("Enum", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(int?), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<_Enum1?>()));
            }
        }

        [TestMethod]
        public void EnumIEnumerableWrite()
        {
            var col = _PrimitiveFixedSizedEnumerableWrite<_Enum1>(_Enum1.None, _Enum1.World, _Enum1.Hello, _Enum1.Bar, _Enum1.Foo);

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Enum", col);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(5, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("Enum", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(int), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<_Enum1>()));
            }
        }

        [TestMethod]
        public void NullableEnumIEnumerableWrite()
        {
            var col = _PrimitiveFixedSizedEnumerableWrite<_Enum1?>(_Enum1.None, null, _Enum1.Hello, null, _Enum1.Foo);

            var temp = Path.GetTempFileName();

            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Enum", col);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(5, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("Enum", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(int?), offDisk.Columns[0].Type);

                Assert.IsTrue(col.SequenceEqual(offDisk.Columns[0].Cast<_Enum1?>()));
            }
        }

        static DateTime _DT(int year, int month, int day) => new DateTime(year, month, day, 1, 2, 3, 4, DateTimeKind.Utc);
        static DateTimeOffset _DTO(int year, int month, int day) => new DateTimeOffset(year, month, day, 5, 6, 7, 8, TimeSpan.Zero);
        static TimeSpan _TS(int hour, int min, int sec) => new TimeSpan(hour, min, sec);

        [TestMethod]
        public void UntypedEnumerableWrite()
        {
            var col1Obj = new object[] { 0.1, 0.2f, 3UL, 4L, 5, 6U, (short)7, (ushort)8, (sbyte)9, (byte)10 };
            var col1Real = new double[] { 0.1, 0.2f, 3UL, 4L, 5, 6U, (short)7, (ushort)8, (sbyte)9, (byte)10 };
            var col2Obj = col1Obj.Reverse().ToArray();
            var col2Real = col1Real.Reverse().ToArray();
            var col3Obj = new object[] { 1f, 2, 3, (ushort)4, (uint)5, 6L, 7UL, (short)8, (byte)9, (sbyte)10 };
            var col3Real = new float[] { 1f, 2, 3, (ushort)4, (uint)5, 6L, 7UL, (short)8, (byte)9, (sbyte)10 };
            var col4Obj = col3Obj.Reverse().ToArray();
            var col4Real = col3Real.Reverse().ToArray();
            var col5Obj = new object[] { 1UL, (uint)2, (uint)3, (uint)4, (uint)5, (byte)6, (byte)7, (uint)8, (ushort)9, (byte)10 };
            var col5Real = new ulong[] { 1UL, (uint)2, (uint)3, (uint)4, (uint)5, (byte)6, (byte)7, (uint)8, (ushort)9, (byte)10 };
            var col6Obj = col5Obj.Reverse().ToArray();
            var col6Real = col5Real.Reverse().ToArray();
            var col7Obj = new object[] { 1L, 2, 3, 4, (sbyte)5, 6, 7, (short)-8, (sbyte)-9, -10 };
            var col7Real = new long[] { 1L, 2, 3, 4, (sbyte)5, 6, 7, (short)-8, (sbyte)-9, -10 };
            var col8Obj = col7Obj.Reverse().ToArray();
            var col8Real = col7Real.Reverse().ToArray();
            var col9Obj = new object[] { (sbyte)1, -2, 3, (sbyte)-4, (short)5, 6, 7, 8, 9, 10 };
            var col9Real = new int[] { (sbyte)1, -2, 3, (sbyte)-4, (short)5, 6, 7, 8, 9, 10 };
            var col10Obj = col9Obj.Reverse().ToArray();
            var col10Real = col9Real.Reverse().ToArray();
            var col11Obj = new object[] { (byte)1, (uint)2, (uint)3, (uint)4, (uint)5, (uint)6, (uint)7, (uint)8, (uint)9, (ushort)10 };
            var col11Real = new uint[] { (byte)1, (uint)2, (uint)3, (uint)4, (uint)5, (uint)6, (uint)7, (uint)8, (uint)9, (ushort)10 };
            var col12Obj = col11Obj.Reverse().ToArray();
            var col12Real = col11Real.Reverse().ToArray();
            var col13Obj = new object[] { (sbyte)-1, (short)2, (short)3, (short)4, (short)5, (short)6, (short)7, (short)8, (short)9, (short)10 };
            var col13Real = new short[] { (sbyte)-1, (short)2, (short)3, (short)4, (short)5, (short)6, (short)7, (short)8, (short)9, (short)10 };
            var col14Obj = col13Obj.Reverse().ToArray();
            var col14Real = col13Real.Reverse().ToArray();
            var col15Obj = new object[] { (byte)1, (ushort)2, (ushort)3, (ushort)4, (ushort)5, (ushort)6, (ushort)7, (ushort)8, (ushort)9, (ushort)10 };
            var col15Real = new ushort[] { (byte)1, (ushort)2, (ushort)3, (ushort)4, (ushort)5, (ushort)6, (ushort)7, (ushort)8, (ushort)9, (ushort)10 };
            var col16Obj = col15Obj.Reverse().ToArray();
            var col16Real = col15Real.Reverse().ToArray();
            var col17Obj = new object[] { (sbyte)-1, (sbyte)-2, (sbyte)-3, (sbyte)-4, (sbyte)-5, (sbyte)6, (sbyte)7, (sbyte)8, (sbyte)9, (sbyte)10 };
            var col17Real = new sbyte[] { (sbyte)-1, (sbyte)-2, (sbyte)-3, (sbyte)-4, (sbyte)-5, (sbyte)6, (sbyte)7, (sbyte)8, (sbyte)9, (sbyte)10 };
            var col18Obj = col17Obj.Reverse().ToArray();
            var col18Real = col17Real.Reverse().ToArray();
            var col19Obj = new object[] { (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8, (byte)9, (byte)10 };
            var col19Real = new byte[] { (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8, (byte)9, (byte)10 };
            var col20Obj = col19Obj.Reverse().ToArray();
            var col20Real = col19Real.Reverse().ToArray();
            var col21Obj = new object[] { _DT(1, 2, 3), _DTO(4, 5, 6), _DT(7, 8, 9), _DT(10, 11, 12), _DTO(1970, 2, 3), _DT(1971, 2, 3), _DT(2000, 2, 3), _DT(2001, 2, 3), _DT(2112, 2, 3), _DTO(8765, 2, 3) };
            var col21Real = new DateTime[] { _DT(1, 2, 3), _DTO(4, 5, 6).UtcDateTime, _DT(7, 8, 9), _DT(10, 11, 12), _DTO(1970, 2, 3).UtcDateTime, _DT(1971, 2, 3), _DT(2000, 2, 3), _DT(2001, 2, 3), _DT(2112, 2, 3), _DTO(8765, 2, 3).UtcDateTime };
            var col22Obj = col21Obj.Reverse().ToArray();
            var col22Real = col21Real.Reverse().ToArray();
            var col23Obj = new object[] { _DT(1, 2, 3), _DTO(4, 5, 6), _DT(7, 8, 9), _DT(10, 11, 12), _DTO(1970, 2, 3), _DT(1971, 2, 3), _DT(2000, 2, 3), _DT(2001, 2, 3), _DT(2112, 2, 3), _DTO(8765, 2, 3) };
            var col23Real = new DateTimeOffset[] { _DT(1, 2, 3), _DTO(4, 5, 6), _DT(7, 8, 9), _DT(10, 11, 12), _DTO(1970, 2, 3), _DT(1971, 2, 3), _DT(2000, 2, 3), _DT(2001, 2, 3), _DT(2112, 2, 3), _DTO(8765, 2, 3) };
            var col24Obj = col23Obj.Reverse().ToArray();
            var col24Real = col23Real.Reverse().ToArray();
            var col25Obj = new object[] { _TS(1, 2, 3), _TS(4, 5, 6), _TS(7, 8, 9), _TS(10, 11, 12), _TS(13, 14, 15), _TS(16, 17, 18), _TS(19, 20, 21), _TS(22, 23, 24), _TS(25, 26, 27), _TS(28, 29, 30) };
            var col25Real = new TimeSpan[] { _TS(1, 2, 3), _TS(4, 5, 6), _TS(7, 8, 9), _TS(10, 11, 12), _TS(13, 14, 15), _TS(16, 17, 18), _TS(19, 20, 21), _TS(22, 23, 24), _TS(25, 26, 27), _TS(28, 29, 30) };
            var col26Obj = col25Obj.Reverse().ToArray();
            var col26Real = col25Real.Reverse().ToArray();
            var col27Obj = new object[] { _Enum1.Bar, _Enum1.Dupe, _Enum1.Hello, _Enum1.Dupe, _Enum1.None, _Enum1.Bar, _Enum1.Dupe, _Enum1.Hello, _Enum1.Dupe, _Enum1.None };
            var col27Real = new _Enum1[] { _Enum1.Bar, _Enum1.Dupe, _Enum1.Hello, _Enum1.Dupe, _Enum1.None, _Enum1.Bar, _Enum1.Dupe, _Enum1.Hello, _Enum1.Dupe, _Enum1.None };


            var temp = Path.GetTempFileName();
            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Double1", col1Obj);
                writer.AddColumn("Double2", col2Obj);
                writer.AddColumn("Float1", col3Obj);
                writer.AddColumn("Float2", col4Obj);
                writer.AddColumn("ULong1", col5Obj);
                writer.AddColumn("ULong2", col6Obj);
                writer.AddColumn("Long1", col7Obj);
                writer.AddColumn("Long2", col8Obj);
                writer.AddColumn("Int1", col9Obj);
                writer.AddColumn("Int2", col10Obj);
                writer.AddColumn("UInt1", col11Obj);
                writer.AddColumn("UInt2", col12Obj);
                writer.AddColumn("Short1", col13Obj);
                writer.AddColumn("Short2", col14Obj);
                writer.AddColumn("UShort1", col15Obj);
                writer.AddColumn("UShort2", col16Obj);
                writer.AddColumn("SByte1", col17Obj);
                writer.AddColumn("SByte2", col18Obj);
                writer.AddColumn("Byte1", col19Obj);
                writer.AddColumn("Byte2", col20Obj);
                writer.AddColumn("DateTime1", col21Obj);
                writer.AddColumn("DateTime2", col22Obj);
                writer.AddColumn("DateTime3", col23Obj);
                writer.AddColumn("DateTime4", col24Obj);
                writer.AddColumn("TimeSpan1", col25Obj);
                writer.AddColumn("TimeSpan2", col26Obj);
                writer.AddColumn("Category", col27Obj);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(10, offDisk.RowCount);
                Assert.AreEqual(27, offDisk.ColumnCount);

                Assert.AreEqual("Double1", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(double), offDisk.Columns[0].Type);
                Assert.AreEqual("Double2", offDisk.Columns[1].Name);
                Assert.AreEqual(typeof(double), offDisk.Columns[1].Type);
                Assert.AreEqual("Float1", offDisk.Columns[2].Name);
                Assert.AreEqual(typeof(float), offDisk.Columns[2].Type);
                Assert.AreEqual("Float2", offDisk.Columns[3].Name);
                Assert.AreEqual(typeof(float), offDisk.Columns[3].Type);
                Assert.AreEqual("ULong1", offDisk.Columns[4].Name);
                Assert.AreEqual(typeof(ulong), offDisk.Columns[4].Type);
                Assert.AreEqual("ULong2", offDisk.Columns[5].Name);
                Assert.AreEqual(typeof(ulong), offDisk.Columns[5].Type);
                Assert.AreEqual("Long1", offDisk.Columns[6].Name);
                Assert.AreEqual(typeof(long), offDisk.Columns[6].Type);
                Assert.AreEqual("Long2", offDisk.Columns[7].Name);
                Assert.AreEqual(typeof(long), offDisk.Columns[7].Type);
                Assert.AreEqual("Int1", offDisk.Columns[8].Name);
                Assert.AreEqual(typeof(int), offDisk.Columns[8].Type);
                Assert.AreEqual("Int2", offDisk.Columns[9].Name);
                Assert.AreEqual(typeof(int), offDisk.Columns[9].Type);
                Assert.AreEqual("UInt1", offDisk.Columns[10].Name);
                Assert.AreEqual(typeof(uint), offDisk.Columns[10].Type);
                Assert.AreEqual("UInt2", offDisk.Columns[11].Name);
                Assert.AreEqual(typeof(uint), offDisk.Columns[11].Type);
                Assert.AreEqual("Short1", offDisk.Columns[12].Name);
                Assert.AreEqual(typeof(short), offDisk.Columns[12].Type);
                Assert.AreEqual("Short2", offDisk.Columns[13].Name);
                Assert.AreEqual(typeof(short), offDisk.Columns[13].Type);
                Assert.AreEqual("UShort1", offDisk.Columns[14].Name);
                Assert.AreEqual(typeof(ushort), offDisk.Columns[14].Type);
                Assert.AreEqual("UShort2", offDisk.Columns[15].Name);
                Assert.AreEqual(typeof(ushort), offDisk.Columns[15].Type);
                Assert.AreEqual("SByte1", offDisk.Columns[16].Name);
                Assert.AreEqual(typeof(sbyte), offDisk.Columns[16].Type);
                Assert.AreEqual("SByte2", offDisk.Columns[17].Name);
                Assert.AreEqual(typeof(sbyte), offDisk.Columns[17].Type);
                Assert.AreEqual("Byte1", offDisk.Columns[18].Name);
                Assert.AreEqual(typeof(byte), offDisk.Columns[18].Type);
                Assert.AreEqual("Byte2", offDisk.Columns[19].Name);
                Assert.AreEqual(typeof(byte), offDisk.Columns[19].Type);
                Assert.AreEqual("DateTime1", offDisk.Columns[20].Name);
                Assert.AreEqual(typeof(DateTime), offDisk.Columns[20].Type);
                Assert.AreEqual("DateTime2", offDisk.Columns[21].Name);
                Assert.AreEqual(typeof(DateTime), offDisk.Columns[21].Type);
                Assert.AreEqual("DateTime3", offDisk.Columns[22].Name);
                Assert.AreEqual(typeof(DateTime), offDisk.Columns[22].Type);
                Assert.AreEqual("DateTime4", offDisk.Columns[23].Name);
                Assert.AreEqual(typeof(DateTime), offDisk.Columns[23].Type);
                Assert.AreEqual("TimeSpan1", offDisk.Columns[24].Name);
                Assert.AreEqual(typeof(TimeSpan), offDisk.Columns[24].Type);
                Assert.AreEqual("TimeSpan2", offDisk.Columns[25].Name);
                Assert.AreEqual(typeof(TimeSpan), offDisk.Columns[25].Type);
                Assert.AreEqual("Category", offDisk.Columns[26].Name);
                Assert.AreEqual(typeof(int), offDisk.Columns[26].Type);

                Assert.IsTrue(col1Real.SequenceEqual(offDisk.Columns[0].Cast<double>()));
                Assert.IsTrue(col2Real.SequenceEqual(offDisk.Columns[1].Cast<double>()));
                Assert.IsTrue(col3Real.SequenceEqual(offDisk.Columns[2].Cast<float>()));
                Assert.IsTrue(col4Real.SequenceEqual(offDisk.Columns[3].Cast<float>()));
                Assert.IsTrue(col5Real.SequenceEqual(offDisk.Columns[4].Cast<ulong>()));
                Assert.IsTrue(col6Real.SequenceEqual(offDisk.Columns[5].Cast<ulong>()));
                Assert.IsTrue(col7Real.SequenceEqual(offDisk.Columns[6].Cast<long>()));
                Assert.IsTrue(col8Real.SequenceEqual(offDisk.Columns[7].Cast<long>()));
                Assert.IsTrue(col9Real.SequenceEqual(offDisk.Columns[8].Cast<int>()));
                Assert.IsTrue(col10Real.SequenceEqual(offDisk.Columns[9].Cast<int>()));
                Assert.IsTrue(col11Real.SequenceEqual(offDisk.Columns[10].Cast<uint>()));
                Assert.IsTrue(col12Real.SequenceEqual(offDisk.Columns[11].Cast<uint>()));
                Assert.IsTrue(col13Real.SequenceEqual(offDisk.Columns[12].Cast<short>()));
                Assert.IsTrue(col14Real.SequenceEqual(offDisk.Columns[13].Cast<short>()));
                Assert.IsTrue(col15Real.SequenceEqual(offDisk.Columns[14].Cast<ushort>()));
                Assert.IsTrue(col16Real.SequenceEqual(offDisk.Columns[15].Cast<ushort>()));
                Assert.IsTrue(col17Real.SequenceEqual(offDisk.Columns[16].Cast<sbyte>()));
                Assert.IsTrue(col18Real.SequenceEqual(offDisk.Columns[17].Cast<sbyte>()));
                Assert.IsTrue(col19Real.SequenceEqual(offDisk.Columns[18].Cast<byte>()));
                Assert.IsTrue(col20Real.SequenceEqual(offDisk.Columns[19].Cast<byte>()));
                Assert.IsTrue(col21Real.SequenceEqual(offDisk.Columns[20].Cast<DateTime>()));
                Assert.IsTrue(col22Real.SequenceEqual(offDisk.Columns[21].Cast<DateTime>()));
                Assert.IsTrue(col23Real.SequenceEqual(offDisk.Columns[22].Cast<DateTimeOffset>()));
                Assert.IsTrue(col24Real.SequenceEqual(offDisk.Columns[23].Cast<DateTimeOffset>()));
                Assert.IsTrue(col25Real.SequenceEqual(offDisk.Columns[24].Cast<TimeSpan>()));
                Assert.IsTrue(col26Real.SequenceEqual(offDisk.Columns[25].Cast<TimeSpan>()));
                Assert.IsTrue(col27Real.SequenceEqual(offDisk.Columns[26].Cast<_Enum1>()));
            }
        }

        [TestMethod]
        public void UntypedNullableEnumerableWrite()
        {
            var col1Obj = new object[] { 0.1, (double?)0.2, 3UL, 4L, 5, 6U, (short)7, (ushort)8, (sbyte?)null, (byte)10 };
            var col1Real = new double?[] { 0.1, (double?)0.2, 3UL, 4L, 5, 6U, (short)7, (ushort)8, (sbyte?)null, (byte)10 };
            var col2Obj = col1Obj.Reverse().ToArray();
            var col2Real = col1Real.Reverse().ToArray();
            var col3Obj = new object[] { 1f, default(float?), 3, (ushort)4, (uint?)5, 6L, 7UL, (short?)null, (byte)9, (sbyte)10 };
            var col3Real = new float?[] { 1f, default(float?), 3, (ushort)4, (uint?)5, 6L, 7UL, (short?)null, (byte)9, (sbyte)10 };
            var col4Obj = col3Obj.Reverse().ToArray();
            var col4Real = col3Real.Reverse().ToArray();
            var col5Obj = new object[] { 1UL, (uint)2, (uint)3, (uint)4, (uint?)null, (byte)6, (byte)7, (uint)8, (ushort)9, (byte)10 };
            var col5Real = new ulong?[] { 1UL, (uint)2, (uint)3, (uint)4, (uint?)null, (byte)6, (byte)7, (uint)8, (ushort)9, (byte)10 };
            var col6Obj = col5Obj.Reverse().ToArray();
            var col6Real = col5Real.Reverse().ToArray();
            var col7Obj = new object[] { (long?)1L, 2, 3, 4, (sbyte)5, 6, 7, (short)-8, (sbyte)-9, (sbyte?)null };
            var col7Real = new long?[] { (long?)1L, 2, 3, 4, (sbyte)5, 6, 7, (short)-8, (sbyte)-9, (sbyte?)null };
            var col8Obj = col7Obj.Reverse().ToArray();
            var col8Real = col7Real.Reverse().ToArray();
            var col9Obj = new object[] { (sbyte?)1, -2, (int?)null, (sbyte)-4, (short)5, 6, 7, 8, 9, 10 };
            var col9Real = new int?[] { (sbyte?)1, -2, (int?)null, (sbyte)-4, (short)5, 6, 7, 8, 9, 10 };
            var col10Obj = col9Obj.Reverse().ToArray();
            var col10Real = col9Real.Reverse().ToArray();
            var col11Obj = new object[] { (byte)1, (uint)2, (uint?)3, (uint)4, (uint)5, (uint)6, (uint)7, (uint)8, (uint)9, (ushort?)null };
            var col11Real = new uint?[] { (byte)1, (uint)2, (uint?)3, (uint)4, (uint)5, (uint)6, (uint)7, (uint)8, (uint)9, (ushort?)null };
            var col12Obj = col11Obj.Reverse().ToArray();
            var col12Real = col11Real.Reverse().ToArray();
            var col13Obj = new object[] { (sbyte)-1, (short)2, (short?)3, (short?)4, (short)5, (short)6, (short?)null, (short)8, (short)9, (byte?)null };
            var col13Real = new short?[] { (sbyte)-1, (short)2, (short?)3, (short?)4, (short)5, (short)6, (short?)null, (short)8, (short)9, (byte?)null };
            var col14Obj = col13Obj.Reverse().ToArray();
            var col14Real = col13Real.Reverse().ToArray();
            var col15Obj = new object[] { (byte)1, (ushort?)2, (ushort?)null, (byte?)4, (ushort)5, (ushort)6, (ushort)7, (ushort)8, (byte?)null, (ushort)10 };
            var col15Real = new ushort?[] { (byte)1, (ushort?)2, (ushort?)null, (byte?)4, (ushort)5, (ushort)6, (ushort)7, (ushort)8, (byte?)null, (ushort)10 };
            var col16Obj = col15Obj.Reverse().ToArray();
            var col16Real = col15Real.Reverse().ToArray();
            var col17Obj = new object[] { (sbyte?)-1, (sbyte?)null, (sbyte)-3, (sbyte)-4, (sbyte)-5, (sbyte)6, (sbyte?)7, (sbyte)8, (sbyte)9, (sbyte)10 };
            var col17Real = new sbyte?[] { (sbyte?)-1, (sbyte?)null, (sbyte)-3, (sbyte)-4, (sbyte)-5, (sbyte)6, (sbyte?)7, (sbyte)8, (sbyte)9, (sbyte)10 };
            var col18Obj = col17Obj.Reverse().ToArray();
            var col18Real = col17Real.Reverse().ToArray();
            var col19Obj = new object[] { (byte?)1, (byte?)null, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte?)8, (byte)9, (byte?)null };
            var col19Real = new byte?[] { (byte?)1, (byte?)null, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte?)8, (byte)9, (byte?)null };
            var col20Obj = col19Obj.Reverse().ToArray();
            var col20Real = col19Real.Reverse().ToArray();
            var col21Obj = new object[] { (DateTime?)_DT(1, 2, 3), (DateTimeOffset?)_DTO(4, 5, 6), (DateTime?)null, (DateTimeOffset?)null, _DTO(1970, 2, 3), _DT(1971, 2, 3), _DT(2000, 2, 3), _DT(2001, 2, 3), _DT(2112, 2, 3), _DTO(8765, 2, 3) };
            var col21Real = new DateTime?[] { (DateTime?)_DT(1, 2, 3), (DateTime?)_DTO(4, 5, 6).UtcDateTime, (DateTime?)null, (DateTime?)null, _DTO(1970, 2, 3).UtcDateTime, _DT(1971, 2, 3), _DT(2000, 2, 3), _DT(2001, 2, 3), _DT(2112, 2, 3), _DTO(8765, 2, 3).UtcDateTime };
            var col22Obj = col21Obj.Reverse().ToArray();
            var col22Real = col21Real.Reverse().ToArray();
            var col23Obj = new object[] { (DateTime?)_DT(1, 2, 3), (DateTimeOffset?)_DTO(4, 5, 6), (DateTime?)null, (DateTimeOffset?)null, _DTO(1970, 2, 3), _DT(1971, 2, 3), _DT(2000, 2, 3), _DT(2001, 2, 3), _DT(2112, 2, 3), _DTO(8765, 2, 3) };
            var col23Real = new DateTimeOffset?[] { (DateTime?)_DT(1, 2, 3), (DateTimeOffset?)_DTO(4, 5, 6), (DateTime?)null, (DateTimeOffset?)null, _DTO(1970, 2, 3), _DT(1971, 2, 3), _DT(2000, 2, 3), _DT(2001, 2, 3), _DT(2112, 2, 3), _DTO(8765, 2, 3) };
            var col24Obj = col23Obj.Reverse().ToArray();
            var col24Real = col23Real.Reverse().ToArray();
            var col25Obj = new object[] { (TimeSpan?)_TS(1, 2, 3), (TimeSpan?)null, _TS(7, 8, 9), _TS(10, 11, 12), _TS(13, 14, 15), _TS(16, 17, 18), _TS(19, 20, 21), _TS(22, 23, 24), _TS(25, 26, 27), _TS(28, 29, 30) };
            var col25Real = new TimeSpan?[] { (TimeSpan?)_TS(1, 2, 3), (TimeSpan?)null, _TS(7, 8, 9), _TS(10, 11, 12), _TS(13, 14, 15), _TS(16, 17, 18), _TS(19, 20, 21), _TS(22, 23, 24), _TS(25, 26, 27), _TS(28, 29, 30) };
            var col26Obj = col25Obj.Reverse().ToArray();
            var col26Real = col25Real.Reverse().ToArray();
            var col27Obj = new object[] { (_Enum1?)_Enum1.Bar, (_Enum1?)_Enum1.Dupe, (_Enum1?)_Enum1.Hello, (_Enum1?)null, (_Enum1?)null, (_Enum1?)_Enum1.Bar, (_Enum1?)_Enum1.Dupe, _Enum1.Hello, _Enum1.Dupe, _Enum1.None };
            var col27Real = new _Enum1?[] { (_Enum1?)_Enum1.Bar, (_Enum1?)_Enum1.Dupe, (_Enum1?)_Enum1.Hello, (_Enum1?)null, (_Enum1?)null, (_Enum1?)_Enum1.Bar, (_Enum1?)_Enum1.Dupe, _Enum1.Hello, _Enum1.Dupe, _Enum1.None };


            var temp = Path.GetTempFileName();
            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("Double1", col1Obj);
                writer.AddColumn("Double2", col2Obj);
                writer.AddColumn("Float1", col3Obj);
                writer.AddColumn("Float2", col4Obj);
                writer.AddColumn("ULong1", col5Obj);
                writer.AddColumn("ULong2", col6Obj);
                writer.AddColumn("Long1", col7Obj);
                writer.AddColumn("Long2", col8Obj);
                writer.AddColumn("Int1", col9Obj);
                writer.AddColumn("Int2", col10Obj);
                writer.AddColumn("UInt1", col11Obj);
                writer.AddColumn("UInt2", col12Obj);
                writer.AddColumn("Short1", col13Obj);
                writer.AddColumn("Short2", col14Obj);
                writer.AddColumn("UShort1", col15Obj);
                writer.AddColumn("UShort2", col16Obj);
                writer.AddColumn("SByte1", col17Obj);
                writer.AddColumn("SByte2", col18Obj);
                writer.AddColumn("Byte1", col19Obj);
                writer.AddColumn("Byte2", col20Obj);
                writer.AddColumn("DateTime1", col21Obj);
                writer.AddColumn("DateTime2", col22Obj);
                writer.AddColumn("DateTime3", col23Obj);
                writer.AddColumn("DateTime4", col24Obj);
                writer.AddColumn("TimeSpan1", col25Obj);
                writer.AddColumn("TimeSpan2", col26Obj);
                writer.AddColumn("Category", col27Obj);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(10, offDisk.RowCount);
                Assert.AreEqual(27, offDisk.ColumnCount);

                Assert.AreEqual("Double1", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(double?), offDisk.Columns[0].Type);
                Assert.AreEqual("Double2", offDisk.Columns[1].Name);
                Assert.AreEqual(typeof(double?), offDisk.Columns[1].Type);
                Assert.AreEqual("Float1", offDisk.Columns[2].Name);
                Assert.AreEqual(typeof(float?), offDisk.Columns[2].Type);
                Assert.AreEqual("Float2", offDisk.Columns[3].Name);
                Assert.AreEqual(typeof(float?), offDisk.Columns[3].Type);
                Assert.AreEqual("ULong1", offDisk.Columns[4].Name);
                Assert.AreEqual(typeof(ulong?), offDisk.Columns[4].Type);
                Assert.AreEqual("ULong2", offDisk.Columns[5].Name);
                Assert.AreEqual(typeof(ulong?), offDisk.Columns[5].Type);
                Assert.AreEqual("Long1", offDisk.Columns[6].Name);
                Assert.AreEqual(typeof(long?), offDisk.Columns[6].Type);
                Assert.AreEqual("Long2", offDisk.Columns[7].Name);
                Assert.AreEqual(typeof(long?), offDisk.Columns[7].Type);
                Assert.AreEqual("Int1", offDisk.Columns[8].Name);
                Assert.AreEqual(typeof(int?), offDisk.Columns[8].Type);
                Assert.AreEqual("Int2", offDisk.Columns[9].Name);
                Assert.AreEqual(typeof(int?), offDisk.Columns[9].Type);
                Assert.AreEqual("UInt1", offDisk.Columns[10].Name);
                Assert.AreEqual(typeof(uint?), offDisk.Columns[10].Type);
                Assert.AreEqual("UInt2", offDisk.Columns[11].Name);
                Assert.AreEqual(typeof(uint?), offDisk.Columns[11].Type);
                Assert.AreEqual("Short1", offDisk.Columns[12].Name);
                Assert.AreEqual(typeof(short?), offDisk.Columns[12].Type);
                Assert.AreEqual("Short2", offDisk.Columns[13].Name);
                Assert.AreEqual(typeof(short?), offDisk.Columns[13].Type);
                Assert.AreEqual("UShort1", offDisk.Columns[14].Name);
                Assert.AreEqual(typeof(ushort?), offDisk.Columns[14].Type);
                Assert.AreEqual("UShort2", offDisk.Columns[15].Name);
                Assert.AreEqual(typeof(ushort?), offDisk.Columns[15].Type);
                Assert.AreEqual("SByte1", offDisk.Columns[16].Name);
                Assert.AreEqual(typeof(sbyte?), offDisk.Columns[16].Type);
                Assert.AreEqual("SByte2", offDisk.Columns[17].Name);
                Assert.AreEqual(typeof(sbyte?), offDisk.Columns[17].Type);
                Assert.AreEqual("Byte1", offDisk.Columns[18].Name);
                Assert.AreEqual(typeof(byte?), offDisk.Columns[18].Type);
                Assert.AreEqual("Byte2", offDisk.Columns[19].Name);
                Assert.AreEqual(typeof(byte?), offDisk.Columns[19].Type);
                Assert.AreEqual("DateTime1", offDisk.Columns[20].Name);
                Assert.AreEqual(typeof(DateTime?), offDisk.Columns[20].Type);
                Assert.AreEqual("DateTime2", offDisk.Columns[21].Name);
                Assert.AreEqual(typeof(DateTime?), offDisk.Columns[21].Type);
                Assert.AreEqual("DateTime3", offDisk.Columns[22].Name);
                Assert.AreEqual(typeof(DateTime?), offDisk.Columns[22].Type);
                Assert.AreEqual("DateTime4", offDisk.Columns[23].Name);
                Assert.AreEqual(typeof(DateTime?), offDisk.Columns[23].Type);
                Assert.AreEqual("TimeSpan1", offDisk.Columns[24].Name);
                Assert.AreEqual(typeof(TimeSpan?), offDisk.Columns[24].Type);
                Assert.AreEqual("TimeSpan2", offDisk.Columns[25].Name);
                Assert.AreEqual(typeof(TimeSpan?), offDisk.Columns[25].Type);
                Assert.AreEqual("Category", offDisk.Columns[26].Name);
                Assert.AreEqual(typeof(int?), offDisk.Columns[26].Type);

                Assert.IsTrue(col1Real.SequenceEqual(offDisk.Columns[0].Cast<double?>()));
                Assert.IsTrue(col2Real.SequenceEqual(offDisk.Columns[1].Cast<double?>()));
                Assert.IsTrue(col3Real.SequenceEqual(offDisk.Columns[2].Cast<float?>()));
                Assert.IsTrue(col4Real.SequenceEqual(offDisk.Columns[3].Cast<float?>()));
                Assert.IsTrue(col5Real.SequenceEqual(offDisk.Columns[4].Cast<ulong?>()));
                Assert.IsTrue(col6Real.SequenceEqual(offDisk.Columns[5].Cast<ulong?>()));
                Assert.IsTrue(col7Real.SequenceEqual(offDisk.Columns[6].Cast<long?>()));
                Assert.IsTrue(col8Real.SequenceEqual(offDisk.Columns[7].Cast<long?>()));
                Assert.IsTrue(col9Real.SequenceEqual(offDisk.Columns[8].Cast<int?>()));
                Assert.IsTrue(col10Real.SequenceEqual(offDisk.Columns[9].Cast<int?>()));
                Assert.IsTrue(col11Real.SequenceEqual(offDisk.Columns[10].Cast<uint?>()));
                Assert.IsTrue(col12Real.SequenceEqual(offDisk.Columns[11].Cast<uint?>()));
                Assert.IsTrue(col13Real.SequenceEqual(offDisk.Columns[12].Cast<short?>()));
                Assert.IsTrue(col14Real.SequenceEqual(offDisk.Columns[13].Cast<short?>()));
                Assert.IsTrue(col15Real.SequenceEqual(offDisk.Columns[14].Cast<ushort?>()));
                Assert.IsTrue(col16Real.SequenceEqual(offDisk.Columns[15].Cast<ushort?>()));
                Assert.IsTrue(col17Real.SequenceEqual(offDisk.Columns[16].Cast<sbyte?>()));
                Assert.IsTrue(col18Real.SequenceEqual(offDisk.Columns[17].Cast<sbyte?>()));
                Assert.IsTrue(col19Real.SequenceEqual(offDisk.Columns[18].Cast<byte?>()));
                Assert.IsTrue(col20Real.SequenceEqual(offDisk.Columns[19].Cast<byte?>()));
                Assert.IsTrue(col21Real.SequenceEqual(offDisk.Columns[20].Cast<DateTime?>()));
                Assert.IsTrue(col22Real.SequenceEqual(offDisk.Columns[21].Cast<DateTime?>()));
                Assert.IsTrue(col23Real.SequenceEqual(offDisk.Columns[22].Cast<DateTimeOffset?>()));
                Assert.IsTrue(col24Real.SequenceEqual(offDisk.Columns[23].Cast<DateTimeOffset?>()));
                Assert.IsTrue(col25Real.SequenceEqual(offDisk.Columns[24].Cast<TimeSpan?>()));
                Assert.IsTrue(col26Real.SequenceEqual(offDisk.Columns[25].Cast<TimeSpan?>()));
                Assert.IsTrue(col27Real.SequenceEqual(offDisk.Columns[26].Cast<_Enum1?>()));
            }
        }

        [TestMethod]
        public void UntypedStringWrite()
        {
            var col1Obj = new object[] { "hello", null, -1L, 2UL, -3, 4U, (short)-5, (ushort)6, (sbyte)-7, (byte)8, 1.23, 4.56f, _DT(1, 2, 3), _DTO(4, 5, 6), _TS(7, 9, 9), _Enum1.Bar };
            var col1Real = new string[] { col1Obj[0]?.ToString(), col1Obj[1]?.ToString(), col1Obj[2]?.ToString(), col1Obj[3]?.ToString(), col1Obj[4]?.ToString(), col1Obj[5]?.ToString(), col1Obj[6]?.ToString(), col1Obj[7]?.ToString(), col1Obj[8]?.ToString(), col1Obj[9]?.ToString(), ((IFormattable)col1Obj[10])?.ToString("R", null), ((IFormattable)col1Obj[11])?.ToString("R", null), ((IFormattable)col1Obj[12])?.ToString("u", null), ((IFormattable)col1Obj[13])?.ToString("u", null), ((IFormattable)col1Obj[14])?.ToString("c", null), col1Obj[15]?.ToString() };
            var col2Obj = new object[] { "hello", (DateTime?)null, -1L, (ulong?)2UL, -3, 4U, (short)-5, (ushort)6, (sbyte)-7, (byte)8, 1.23, 4.56f, _DT(1, 2, 3), _DTO(4, 5, 6), _TS(7, 9, 9), _Enum1.Bar };
            var col2Real = new string[] { col2Obj[0]?.ToString(), ((IFormattable)col2Obj[1])?.ToString("u", null), col2Obj[2]?.ToString(), col2Obj[3]?.ToString(), col2Obj[4]?.ToString(), col2Obj[5]?.ToString(), col2Obj[6]?.ToString(), col2Obj[7]?.ToString(), col2Obj[8]?.ToString(), col2Obj[9]?.ToString(), ((IFormattable)col2Obj[10])?.ToString("R", null), ((IFormattable)col2Obj[11])?.ToString("R", null), ((IFormattable)col2Obj[12])?.ToString("u", null), ((IFormattable)col2Obj[13])?.ToString("u", null), ((IFormattable)col2Obj[14])?.ToString("c", null), col2Obj[15]?.ToString() };
            var col3Obj = col1Obj.Reverse().ToArray();
            var col3Real = col1Real.Reverse().ToArray();
            var col4Obj = col2Obj.Reverse().ToArray();
            var col4Real = col2Real.Reverse().ToArray();

            var temp = Path.GetTempFileName();
            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("String1", col1Obj);
                writer.AddColumn("String2", col2Obj);
                writer.AddColumn("String3", col3Obj);
                writer.AddColumn("String4", col4Obj);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(16, offDisk.RowCount);
                Assert.AreEqual(4, offDisk.ColumnCount);

                Assert.AreEqual("String1", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(string), offDisk.Columns[0].Type);
                Assert.AreEqual("String2", offDisk.Columns[1].Name);
                Assert.AreEqual(typeof(string), offDisk.Columns[1].Type);
                Assert.AreEqual("String3", offDisk.Columns[2].Name);
                Assert.AreEqual(typeof(string), offDisk.Columns[2].Type);
                Assert.AreEqual("String4", offDisk.Columns[3].Name);
                Assert.AreEqual(typeof(string), offDisk.Columns[3].Type);

                Assert.IsTrue(col1Real.SequenceEqual(offDisk.Columns[0].Cast<string>()));
                Assert.IsTrue(col2Real.SequenceEqual(offDisk.Columns[1].Cast<string>()));
                Assert.IsTrue(col3Real.SequenceEqual(offDisk.Columns[2].Cast<string>()));
                Assert.IsTrue(col4Real.SequenceEqual(offDisk.Columns[3].Cast<string>()));
            }
        }

        enum _Enum2
        {
            Woops,
            Oops,
            Nope,

            Dupe
        }

        [TestMethod]
        public void HeterogenousEnumWrite()
        {
            var col1Obj = new object[] { _Enum1.Bar, _Enum1.Foo, _Enum1.Hello, _Enum1.None, _Enum1.World, _Enum2.Woops, _Enum2.Oops, _Enum2.Woops, _Enum2.Dupe, _Enum1.Dupe };
            var col1Real = col1Obj.Select(c => c.ToString()).ToArray();

            var temp = Path.GetTempFileName();
            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("SyntheticEnum", col1Obj);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(10, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("SyntheticEnum", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(int), offDisk.Columns[0].Type);

                Assert.IsTrue(col1Real.SequenceEqual(offDisk.Columns[0].Cast<string>()));
            }
        }

        [TestMethod]
        public void HeterogenousNullableEnumWrite()
        {
            var col1Obj = new object[] { (_Enum1?)_Enum1.Bar, _Enum1.Foo, (_Enum1?)null, _Enum1.None, _Enum1.World, (_Enum2?)_Enum2.Woops, (_Enum2?)null, _Enum2.Woops, _Enum2.Dupe, _Enum1.Dupe };
            var col1Real = col1Obj.Select(c => c?.ToString()).ToArray();

            var temp = Path.GetTempFileName();
            using (var writer = new FeatherWriter(temp, WriteMode.Eager))
            {
                writer.AddColumn("SyntheticEnum", col1Obj);
            }

            using (var offDisk = FeatherReader.ReadFromFile(temp, BasisType.Zero))
            {
                Assert.AreEqual(10, offDisk.RowCount);
                Assert.AreEqual(1, offDisk.ColumnCount);

                Assert.AreEqual("SyntheticEnum", offDisk.Columns[0].Name);
                Assert.AreEqual(typeof(int?), offDisk.Columns[0].Type);

                Assert.IsTrue(col1Real.SequenceEqual(offDisk.Columns[0].Cast<string>()));
            }
        }
    }
}
