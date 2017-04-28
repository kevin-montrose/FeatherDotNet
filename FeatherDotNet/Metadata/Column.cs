// automatically generated by the FlatBuffers compiler, do not modify

namespace feather.fbs
{

    using FlatBuffers;
    using System;

    struct Column : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static Column GetRootAsColumn(ByteBuffer _bb) { return GetRootAsColumn(_bb, new Column()); }
  public static Column GetRootAsColumn(ByteBuffer _bb, Column obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p.bb_pos = _i; __p.bb = _bb; }
  public Column __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public string Name { get { int o = __p.__offset(4); return o != 0 ? __p.__string(o + __p.bb_pos) : null; } }
  public ArraySegment<byte>? GetNameBytes() { return __p.__vector_as_arraysegment(4); }
  public PrimitiveArray? Values { get { int o = __p.__offset(6); return o != 0 ? (PrimitiveArray?)(new PrimitiveArray()).__assign(__p.__indirect(o + __p.bb_pos), __p.bb) : null; } }
  public TypeMetadata MetadataType { get { int o = __p.__offset(8); return o != 0 ? (TypeMetadata)__p.bb.Get(o + __p.bb_pos) : TypeMetadata.NONE; } }
  public TTable? Metadata<TTable>() where TTable : struct, IFlatbufferObject { int o = __p.__offset(10); return o != 0 ? (TTable?)__p.__union<TTable>(o) : null; }
  /// This should (probably) be JSON
  public string UserMetadata { get { int o = __p.__offset(12); return o != 0 ? __p.__string(o + __p.bb_pos) : null; } }
  public ArraySegment<byte>? GetUserMetadataBytes() { return __p.__vector_as_arraysegment(12); }

  public static Offset<Column> CreateColumn(FlatBufferBuilder builder,
      StringOffset nameOffset = default(StringOffset),
      Offset<PrimitiveArray> valuesOffset = default(Offset<PrimitiveArray>),
      TypeMetadata metadata_type = TypeMetadata.NONE,
      int metadataOffset = 0,
      StringOffset user_metadataOffset = default(StringOffset)) {
    builder.StartObject(5);
    Column.AddUserMetadata(builder, user_metadataOffset);
    Column.AddMetadata(builder, metadataOffset);
    Column.AddValues(builder, valuesOffset);
    Column.AddName(builder, nameOffset);
    Column.AddMetadataType(builder, metadata_type);
    return Column.EndColumn(builder);
  }

  public static void StartColumn(FlatBufferBuilder builder) { builder.StartObject(5); }
  public static void AddName(FlatBufferBuilder builder, StringOffset nameOffset) { builder.AddOffset(0, nameOffset.Value, 0); }
  public static void AddValues(FlatBufferBuilder builder, Offset<PrimitiveArray> valuesOffset) { builder.AddOffset(1, valuesOffset.Value, 0); }
  public static void AddMetadataType(FlatBufferBuilder builder, TypeMetadata metadataType) { builder.AddByte(2, (byte)metadataType, 0); }
  public static void AddMetadata(FlatBufferBuilder builder, int metadataOffset) { builder.AddOffset(3, metadataOffset, 0); }
  public static void AddUserMetadata(FlatBufferBuilder builder, StringOffset userMetadataOffset) { builder.AddOffset(4, userMetadataOffset.Value, 0); }
  public static Offset<Column> EndColumn(FlatBufferBuilder builder) {
    int o = builder.EndObject();
    return new Offset<Column>(o);
  }
};


}
