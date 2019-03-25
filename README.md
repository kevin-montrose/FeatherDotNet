# Feather.NET

An implementation of the [Feather format](https://github.com/wesm/feather) for .NET.

## Install

[Feather.NET is on Nuget](https://www.nuget.org/packages/FeatherDotNet).  Install through a UI, or use `Install-Package FeatherDotNet`.

## Loading a Dataframe

Use the `FeatherReader` classes `ReadXXX` and `TryReadXXX` methods.

Feather.NET works on memory mapped files, and deserializes lazily.  This means you can operate on very
large datasets (> `int.MaxValue` rows) without paying a large loading time upfront.

## Untyped access

`FeatherReader` returns `DataFrame`s which have indexers that return rows, columns, and values - all of them untyped.
Columns and rows also expose indexers into values.


Columns be `.Cast<T>()`, cast to arrays, or otherwise coerced with `GetRange(...)` and `ToArray(...)` methods.

Rows can be `.Map<T*>()`, cast to `Value[]`, or otherwise coerced with `GetRange(...)` and `ToArray(...)` methods.

## Mapping columns to types

`DataFrame` exposes `.Map<T*>()` and `.TryMap<T*>(...)` methods to map up to 8 columns to specific types.  The resulting `TypedDataFrame<T*>` 
exposes typed columns for easy access.

For more than 8 columns, the individual `Column` references accessed on a `DataFrame` may be mapped as well.

As with other access, types are validated eagerly (the Map calls will fail if the conversion is invalid) but actual values are deserialized
on demand.  In other word, `.Map<string>()` will only work if the underlying type is a string or category, but no actual strings will be created
until values are accessed.

## Mapping rows to types

`DataFrame` exposes `.Proxy<T>()` and `.TryProxy<T>(...)` methods to map rows to particular .NET types.  A mapping may be provided, but if the type
has matching publicly settable member names the mapping can instead be inferred.

As with other access, types are validated eagerly (the Map calls will fail if the conversion is invalid) but actual values are deserialized
on demand.  In other word, `.Proxy<MyType>("foo")` will only work if a `MyType` has a member "foo" (or "Foo", "FOO", etc.), but no `MyTypes` will
be allocated until rows are accessed.

A factory method may be optionally provided, allowing objects to be reused when the dataframe is being accessed.

## Read Examples

See the [test project](https://github.com/kevin-montrose/FeatherDotNet/FeatherDotNet.Tests)

 - [Untyped `DataFrame`s](https://github.com/kevin-montrose/FeatherDotNet/blob/master/FeatherDotNet.Tests/DataFrameTests.cs)
 - [Typed columns as `TypedDataFrame`s](https://github.com/kevin-montrose/FeatherDotNet/blob/master/FeatherDotNet.Tests/TypedDataFrameTests.cs)
 - [Typed rows as `ProxyDataFrameTests`s](https://github.com/kevin-montrose/FeatherDotNet/blob/master/FeatherDotNet.Tests/ProxyDataFrameTests.cs)

## Type Mappings

Integer types can be freely converted provided the underlying type is at least as large as the .NET type (an `Int16` can be mapped to an `int`,
but `Int64` may not be mapped to a `int`) and respects signedness (a `UInt16` can be mapped into `int`, whereas `Int16` can't be mapped into `uint` due to possible loss of sign).

For floating point values, `float` and `double` are supported target types (`decimal` is not).  All integer types can be converted to `float` or `double`.
If the underlying type is a `Double` it cannot be converted to a `float`, but `Single`s can be converted to `double`s.

Underling `UTF8` data can only be mapped to `string`s.

Categories can be mapped to `string`s, `int`s, or `enum`s with the same values or names.  String values are reused if a Category is mapped to strings.

All non-nullable underlying types can be freely converted to their nullable equivalents.

Nullable underlying types cannot be mapped to their non-null equivalent, even if the specific `Value` is non-null.  In other words, type mappings are validated
at the column level rather than the cell level.

## Write Dataframes

Use the `FeatherWriter` class and the `AddColumn` and `AddColumns` methods.  FeatherDotNet can write to either streams or directly to a file.

Ideal performance is seen when columns implement the `ICollection<T>` interface, but `FeatherWriter` will cope with untyped enumerables by dynamically
chosing the widest data type.

Two different write modes are supported: `Eager` and `Lazy`.

`Eager` will immediately write to disk, and won't keep references to any columns added.  This mode is best if the columns need to be available for GC
before writing has completed.

`Lazy` will queue up writes to disk, and execute all of them when the `FeatherWriter` is disposed.  This mode allows `AddColumn` calls to return immediately.

In both modes column data types are validated eagerly, so the benefits of `Lazy` mode can only be fully realized if the added columns implement `ICollection<T>`.

## Write Examples

See the [test project](https://github.com/kevin-montrose/FeatherDotNet/FeatherDotNet.Tests), in particular the [`WriteTests` class](https://github.com/kevin-montrose/FeatherDotNet/blob/master/FeatherDotNet.Tests/WriteTests.cs).

## Not Implemented

Feather.NET is a Work In Progress, the following features are not yet implemented:

 - Binary (ie. `byte[]`) columns
 - Dictionary encodings