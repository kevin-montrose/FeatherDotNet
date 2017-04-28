using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;

namespace FeatherDotNet
{
    public partial class DataFrame
    {
        /// <summary>
        /// Map the columns in this dataframe to a given type.
        /// 
        /// If the column names match (in a case insensitive, culture invariant comparison) the mapping can be automatic.
        /// 
        /// Otherwise, list out the member names in column order to map to.
        /// 
        /// Objects of TProxyType will be created by calling the default constructor.
        /// 
        /// Throws if the types are not compatible.
        /// </summary>
        public ProxyDataFrame<TProxyType> Proxy<TProxyType>(params string[] membersToColumns)
            where TProxyType : new()
        => Proxy(MakeDefaultFactory<TProxyType>(), membersToColumns);

        /// <summary>
        /// Map the columns in this dataframe to a given type.
        /// 
        /// If the column names match (in a case insensitive, culture invariant comparison) the mapping can be automatic.
        /// 
        /// Otherwise, list out the member names in column order to map to.
        /// 
        /// Objects of TProxyType will be created by calling the provided proxy function.
        /// 
        /// Throws if the types are not compatible.
        /// </summary>
        public ProxyDataFrame<TProxyType> Proxy<TProxyType>(Func<TProxyType> factory, params string[] membersToColumns)
        {
            if (factory == null) throw new ArgumentNullException(nameof(factory));

            Dictionary<long, MemberInfo> columnsToMembers;
            string errorMessage;
            if (!TryInferMapping<TProxyType>(membersToColumns, out columnsToMembers, out errorMessage))
            {
                throw new ArgumentException(errorMessage, nameof(membersToColumns));
            }

            return MakeProxy(columnsToMembers, factory);
        }

        /// <summary>
        /// Map the columns in this dataframe to a given type.
        /// 
        /// If the column names match (in a case insensitive, culture invariant comparison) the mapping can be automatic.
        /// 
        /// Otherwise, list out the member names in column order to map to.
        /// 
        /// Objects of TProxyType will be created by calling the default constructor.
        /// 
        /// Return false if the mapping cannot be made, and true otherwise.
        /// </summary>
        public bool TryProxy<TProxyType>(out ProxyDataFrame<TProxyType> dataframe, params string[] membersToColumns)
            where TProxyType : new()
        => TryProxy(MakeDefaultFactory<TProxyType>(), out dataframe, membersToColumns);

        /// <summary>
        /// Map the columns in this dataframe to a given type.
        /// 
        /// If the column names match (in a case insensitive, culture invariant comparison) the mapping can be automatic.
        /// 
        /// Otherwise, list out the member names in column order to map to.
        /// 
        /// Objects of TProxyType will be created by calling the provided proxy function.
        /// 
        /// Return false if the mapping cannot be made, and true otherwise.
        /// </summary>
        public bool TryProxy<TProxyType>(Func<TProxyType> factory, out ProxyDataFrame<TProxyType> dataframe, params string[] membersToColumns)
        {
            if (factory == null)
            {
                dataframe = null;
                return false;
            }

            Dictionary<long, MemberInfo> columnsToMembers;
            string _;
            if (!TryInferMapping<TProxyType>(membersToColumns, out columnsToMembers, out _))
            {
                dataframe = null;
                return false;
            }

            dataframe = MakeProxy(columnsToMembers, factory);
            return true;
        }

        /// <summary>
        /// Maps this dataframe to a dataframe with a single column of the given type.
        /// 
        /// Throws if the mapping cannot be made.
        /// </summary>
        public TypedDataFrame<TCol1> Map<TCol1>()
        {
            if (ColumnCount < 1)
            {
                throw new ArgumentException($"Cannot map dataframe, mapping has 1 column while dataframe has {ColumnCount:N0} columns");
            }

            TypedDataFrame<TCol1> ret;
            if (!TryMap(out ret))
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {AllColumns.ElementAt(0).Type.Name}");
            }

            return ret;
        }

        /// <summary>
        /// Tries to map this dataframe to a dataframe with a single column of the given type.
        /// 
        /// Return true if such a mapping was possible, and false otherwise.
        /// </summary>
        public bool TryMap<TCol1>(out TypedDataFrame<TCol1> dataframe)
        {
            if (ColumnCount < 1)
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[0].CanMapTo(typeof(TCol1)))
            {
                dataframe = null;
                return false;
            }

            dataframe = new TypedDataFrame<TCol1>(this);
            return true;
        }

        /// <summary>
        /// Maps this dataframe to a dataframe with two columns of the given types.
        /// 
        /// Throws if the mapping cannot be made.
        /// </summary>
        public TypedDataFrame<TCol1, TCol2> Map<TCol1, TCol2>()
        {
            if (ColumnCount < 2)
            {
                throw new ArgumentException($"Cannot map dataframe, mapping has 2 columns while dataframe has {ColumnCount:N0} columns");
            }

            TypedDataFrame<TCol1, TCol2> ret;
            if (!TryMap(out ret))
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {AllColumns.ElementAt(0).Type.Name}, {typeof(TCol2).Name} = {AllColumns.ElementAt(1).Type.Name}");
            }

            return ret;
        }

        /// <summary>
        /// Tries to map this dataframe to a dataframe with two columns of the given type.
        /// 
        /// Return true if such a mapping was possible, and false otherwise.
        /// </summary>
        public bool TryMap<TCol1, TCol2>(out TypedDataFrame<TCol1, TCol2> dataframe)
        {
            if (ColumnCount < 2)
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[0].CanMapTo(typeof(TCol1)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[1].CanMapTo(typeof(TCol2)))
            {
                dataframe = null;
                return false;
            }

            dataframe = new TypedDataFrame<TCol1, TCol2>(this);
            return true;
        }

        /// <summary>
        /// Maps this dataframe to a dataframe with three columns of the given types.
        /// 
        /// Throws if the mapping cannot be made.
        /// </summary>
        public TypedDataFrame<TCol1, TCol2, TCol3> Map<TCol1, TCol2, TCol3>()
        {
            if (ColumnCount < 2)
            {
                throw new ArgumentException($"Cannot map dataframe, mapping has 3 columns while dataframe has {ColumnCount:N0} columns");
            }

            TypedDataFrame<TCol1, TCol2, TCol3> ret;
            if (!TryMap(out ret))
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {AllColumns.ElementAt(0).Type.Name}, {typeof(TCol2).Name} = {AllColumns.ElementAt(1).Type.Name}, {typeof(TCol3).Name} = {AllColumns.ElementAt(2).Type.Name}");
            }

            return ret;
        }

        /// <summary>
        /// Tries to map this dataframe to a dataframe with three columns of the given type.
        /// 
        /// Return true if such a mapping was possible, and false otherwise.
        /// </summary>
        public bool TryMap<TCol1, TCol2, TCol3>(out TypedDataFrame<TCol1, TCol2, TCol3> dataframe)
        {
            if (ColumnCount < 3)
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[0].CanMapTo(typeof(TCol1)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[1].CanMapTo(typeof(TCol2)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[2].CanMapTo(typeof(TCol3)))
            {
                dataframe = null;
                return false;
            }

            dataframe = new TypedDataFrame<TCol1, TCol2, TCol3>(this);
            return true;
        }

        /// <summary>
        /// Maps this dataframe to a dataframe with four columns of the given types.
        /// 
        /// Throws if the mapping cannot be made.
        /// </summary>
        public TypedDataFrame<TCol1, TCol2, TCol3, TCol4> Map<TCol1, TCol2, TCol3, TCol4>()
        {
            if (ColumnCount < 4)
            {
                throw new ArgumentException($"Cannot map dataframe, mapping has 4 columns while dataframe has {ColumnCount:N0} columns");
            }

            TypedDataFrame<TCol1, TCol2, TCol3, TCol4> ret;
            if (!TryMap(out ret))
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {AllColumns.ElementAt(0).Type.Name}, {typeof(TCol2).Name} = {AllColumns.ElementAt(1).Type.Name}, {typeof(TCol3).Name} = {AllColumns.ElementAt(2).Type.Name}, {typeof(TCol4).Name} = {AllColumns.ElementAt(3).Type.Name}");
            }

            return ret;
        }

        /// <summary>
        /// Tries to map this dataframe to a dataframe with four columns of the given type.
        /// 
        /// Return true if such a mapping was possible, and false otherwise.
        /// </summary>
        public bool TryMap<TCol1, TCol2, TCol3, TCol4>(out TypedDataFrame<TCol1, TCol2, TCol3, TCol4> dataframe)
        {
            if (ColumnCount < 4)
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[0].CanMapTo(typeof(TCol1)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[1].CanMapTo(typeof(TCol2)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[2].CanMapTo(typeof(TCol3)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[3].CanMapTo(typeof(TCol4)))
            {
                dataframe = null;
                return false;
            }

            dataframe = new TypedDataFrame<TCol1, TCol2, TCol3, TCol4>(this);
            return true;
        }

        /// <summary>
        /// Maps this dataframe to a dataframe with five columns of the given types.
        /// 
        /// Throws if the mapping cannot be made.
        /// </summary>
        public TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5> Map<TCol1, TCol2, TCol3, TCol4, TCol5>()
        {
            if (ColumnCount < 5)
            {
                throw new ArgumentException($"Cannot map dataframe, mapping has 5 columns while dataframe has {ColumnCount:N0} columns");
            }

            TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5> ret;
            if (!TryMap(out ret))
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {AllColumns.ElementAt(0).Type.Name}, {typeof(TCol2).Name} = {AllColumns.ElementAt(1).Type.Name}, {typeof(TCol3).Name} = {AllColumns.ElementAt(2).Type.Name}, {typeof(TCol4).Name} = {AllColumns.ElementAt(3).Type.Name}, {typeof(TCol5).Name} = {AllColumns.ElementAt(4).Type.Name}");
            }

            return ret;
        }

        /// <summary>
        /// Tries to map this dataframe to a dataframe with five columns of the given type.
        /// 
        /// Return true if such a mapping was possible, and false otherwise.
        /// </summary>
        public bool TryMap<TCol1, TCol2, TCol3, TCol4, TCol5>(out TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5> dataframe)
        {
            if (ColumnCount < 5)
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[0].CanMapTo(typeof(TCol1)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[1].CanMapTo(typeof(TCol2)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[2].CanMapTo(typeof(TCol3)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[3].CanMapTo(typeof(TCol4)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[4].CanMapTo(typeof(TCol5)))
            {
                dataframe = null;
                return false;
            }

            dataframe = new TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5>(this);
            return true;
        }

        /// <summary>
        /// Maps this dataframe to a dataframe with six columns of the given types.
        /// 
        /// Throws if the mapping cannot be made.
        /// </summary>
        public TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6> Map<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6>()
        {
            if (ColumnCount < 6)
            {
                throw new ArgumentException($"Cannot map dataframe, mapping has 6 columns while dataframe has {ColumnCount:N0} columns");
            }

            TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6> ret;
            if (!TryMap(out ret))
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {AllColumns.ElementAt(0).Type.Name}, {typeof(TCol2).Name} = {AllColumns.ElementAt(1).Type.Name}, {typeof(TCol3).Name} = {AllColumns.ElementAt(2).Type.Name}, {typeof(TCol4).Name} = {AllColumns.ElementAt(3).Type.Name}, {typeof(TCol5).Name} = {AllColumns.ElementAt(4).Type.Name}, {typeof(TCol6).Name} = {AllColumns.ElementAt(5).Type.Name}");
            }

            return ret;
        }

        /// <summary>
        /// Tries to map this dataframe to a dataframe with six columns of the given type.
        /// 
        /// Return true if such a mapping was possible, and false otherwise.
        /// </summary>
        public bool TryMap<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6>(out TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6> dataframe)
        {
            if (ColumnCount < 6)
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[0].CanMapTo(typeof(TCol1)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[1].CanMapTo(typeof(TCol2)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[2].CanMapTo(typeof(TCol3)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[3].CanMapTo(typeof(TCol4)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[4].CanMapTo(typeof(TCol5)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[5].CanMapTo(typeof(TCol6)))
            {
                dataframe = null;
                return false;
            }

            dataframe = new TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6>(this);
            return true;
        }

        /// <summary>
        /// Maps this dataframe to a dataframe with seven columns of the given types.
        /// 
        /// Throws if the mapping cannot be made.
        /// </summary>
        public TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7> Map<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7>()
        {
            if (ColumnCount < 7)
            {
                throw new ArgumentException($"Cannot map dataframe, mapping has 7 columns while dataframe has {ColumnCount:N0} columns");
            }

            TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7> ret;
            if (!TryMap(out ret))
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {AllColumns.ElementAt(0).Type.Name}, {typeof(TCol2).Name} = {AllColumns.ElementAt(1).Type.Name}, {typeof(TCol3).Name} = {AllColumns.ElementAt(2).Type.Name}, {typeof(TCol4).Name} = {AllColumns.ElementAt(3).Type.Name}, {typeof(TCol5).Name} = {AllColumns.ElementAt(4).Type.Name}, {typeof(TCol6).Name} = {AllColumns.ElementAt(5).Type.Name}, {typeof(TCol7).Name} = {AllColumns.ElementAt(6).Type.Name}");
            }

            return ret;
        }

        /// <summary>
        /// Tries to map this dataframe to a dataframe with seven columns of the given type.
        /// 
        /// Return true if such a mapping was possible, and false otherwise.
        /// </summary>
        public bool TryMap<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7>(out TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7> dataframe)
        {
            if (ColumnCount < 7)
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[0].CanMapTo(typeof(TCol1)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[1].CanMapTo(typeof(TCol2)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[2].CanMapTo(typeof(TCol3)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[3].CanMapTo(typeof(TCol4)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[4].CanMapTo(typeof(TCol5)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[5].CanMapTo(typeof(TCol6)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[6].CanMapTo(typeof(TCol7)))
            {
                dataframe = null;
                return false;
            }

            dataframe = new TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7>(this);
            return true;
        }

        /// <summary>
        /// Maps this dataframe to a dataframe with eight columns of the given types.
        /// 
        /// Throws if the mapping cannot be made.
        /// </summary>
        public TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8> Map<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8>()
        {
            if (ColumnCount < 8)
            {
                throw new ArgumentException($"Cannot map dataframe, mapping has 8 columns while dataframe has {ColumnCount:N0} columns");
            }

            TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8> ret;
            if (!TryMap(out ret))
            {
                throw new ArgumentException($"Cannot map dataframe given mapping: {typeof(TCol1).Name} = {AllColumns.ElementAt(0).Type.Name}, {typeof(TCol2).Name} = {AllColumns.ElementAt(1).Type.Name}, {typeof(TCol3).Name} = {AllColumns.ElementAt(2).Type.Name}, {typeof(TCol4).Name} = {AllColumns.ElementAt(3).Type.Name}, {typeof(TCol5).Name} = {AllColumns.ElementAt(4).Type.Name}, {typeof(TCol6).Name} = {AllColumns.ElementAt(5).Type.Name}, {typeof(TCol7).Name} = {AllColumns.ElementAt(6).Type.Name}, {typeof(TCol8).Name} = {AllColumns.ElementAt(7).Type.Name}");
            }

            return ret;
        }

        /// <summary>
        /// Tries to map this dataframe to a dataframe with eight columns of the given type.
        /// 
        /// Return true if such a mapping was possible, and false otherwise.
        /// </summary>
        public bool TryMap<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8>(out TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8> dataframe)
        {
            if (ColumnCount < 8)
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[0].CanMapTo(typeof(TCol1)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[1].CanMapTo(typeof(TCol2)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[2].CanMapTo(typeof(TCol3)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[3].CanMapTo(typeof(TCol4)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[4].CanMapTo(typeof(TCol5)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[5].CanMapTo(typeof(TCol6)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[6].CanMapTo(typeof(TCol7)))
            {
                dataframe = null;
                return false;
            }

            if (!Metadata.Columns[7].CanMapTo(typeof(TCol8)))
            {
                dataframe = null;
                return false;
            }

            dataframe = new TypedDataFrame<TCol1, TCol2, TCol3, TCol4, TCol5, TCol6, TCol7, TCol8>(this);
            return true;
        }

        Func<TProxyType> MakeDefaultFactory<TProxyType>()
            where TProxyType : new()
        => () => new TProxyType();

        static readonly MethodInfo Row_UnsafeGetTranslated_Generic = typeof(Row).GetMethod("UnsafeGetTranslated", BindingFlags.NonPublic | BindingFlags.Instance);
        Func<Row, TProxyType, TProxyType> MakeMapper<TProxyType>(Dictionary<long, MemberInfo> columnsToMembers)
        {
            var nameBuilder = new StringBuilder();
            nameBuilder.Append("Mapper_");
            nameBuilder.Append(typeof(TProxyType).Name);
            foreach (var kv in columnsToMembers)
            {
                nameBuilder.Append("_");
                nameBuilder.Append(kv.Key);
                nameBuilder.Append("_");
                nameBuilder.Append(kv.Value.Name);
            }
            
            var name = nameBuilder.ToString();
            var dyn = new DynamicMethod(name, typeof(TProxyType), new[] { typeof(Row), typeof(TProxyType) }, restrictedSkipVisibility: true);
            var il = dyn.GetILGenerator();

            var retLocal = il.DeclareLocal(typeof(TProxyType));
            Action loadRetRef =
                () =>
                {
                    if (typeof(TProxyType).IsValueType)
                    {
                        il.Emit(OpCodes.Ldloca, retLocal);              // TProxyType*
                    }
                    else
                    {
                        il.Emit(OpCodes.Ldloc, retLocal);               // TProxyType
                    }
                };
            Action<long> loadColumnValue =
                (translatedColumnIndex) =>
                {
                    var goingToMember = columnsToMembers[translatedColumnIndex];
                    var type = (goingToMember as PropertyInfo)?.PropertyType ?? (goingToMember as FieldInfo)?.FieldType;

                    var unsafeGetTranslated = Row_UnsafeGetTranslated_Generic.MakeGenericMethod(type);
                    il.Emit(OpCodes.Ldarga_S, 0);                       // Row*
                    il.Emit(OpCodes.Ldc_I8, translatedColumnIndex);     // Row* long
                    il.Emit(OpCodes.Call, unsafeGetTranslated);         // whatever-the-appropriate-type-is
                };

            il.Emit(OpCodes.Ldarg_1);                           // TProxyType
            il.Emit(OpCodes.Stloc, retLocal);                   // --empty--
            
            foreach (var kv in columnsToMembers)
            {
                var translatedColumnIndex = kv.Key;
                var member = kv.Value;

                loadRetRef();                                   // TProxyType(*)?
                loadColumnValue(translatedColumnIndex);         // TProxyType(*?) whatever-the-appropriate-type-is

                var asField = member as FieldInfo;
                if (asField != null)
                {
                    il.Emit(OpCodes.Stfld, asField);            // --empty--
                }
                else
                {
                    var setMtd = ((PropertyInfo)member).SetMethod;
                    il.Emit(OpCodes.Call, setMtd);              // --empty--
                }
            }

            il.Emit(OpCodes.Ldloc, retLocal);                   // TProxyType
            il.Emit(OpCodes.Ret);                               // --empty--
            
            var ret = (Func<Row, TProxyType, TProxyType>)dyn.CreateDelegate(typeof(Func<Row, TProxyType, TProxyType>));
            return ret;
        } 

        ProxyDataFrame<TProxyType> MakeProxy<TProxyType>(Dictionary<long, MemberInfo> columnsToMembers, Func<TProxyType> factory)
        {
            var mapper = MakeMapper<TProxyType>(columnsToMembers);

            return new ProxyDataFrame<TProxyType>(this, mapper, factory);
        }

        bool TryInferMapping<TProxyType>(string[] membersInColumnOrder, out Dictionary<long, MemberInfo> translatedColumnIndexToMemberMapping, out string errorMessage)
        {
            Dictionary<long, MemberInfo> suggested;
            if(!TrySuggestMapping<TProxyType>(membersInColumnOrder, out suggested, out errorMessage))
            {
                translatedColumnIndexToMemberMapping = null;
                return false;
            }

            foreach(var kv in suggested)
            {
                var translatedColumnIndex = kv.Key;
                var member = kv.Value;

                var memberType = (member as PropertyInfo)?.PropertyType ?? (member as FieldInfo)?.FieldType;

                var columnDetails = Metadata.Columns[translatedColumnIndex];
                var columnType = columnDetails.Type;
                if (!columnType.CanMapTo(memberType, columnDetails.CategoryLevels))
                {
                    translatedColumnIndexToMemberMapping = null;
                    errorMessage = $"Cannot map column {UntranslateIndex(translatedColumnIndex):N0} \"{columnDetails.Name}\" to member {member.Name} of type {memberType.Name}";
                    return false;
                }
            }

            translatedColumnIndexToMemberMapping = suggested;
            errorMessage = null;
            return true;
        }

        bool TrySuggestMapping<TProxyType>(string[] membersInColumnOrder, out Dictionary<long, MemberInfo> translatedColumnIndexToMemberMapping, out string errorMessage)
        {
            var publicFieldsAndPropertiesList =
                typeof(TProxyType)
                    .GetMembers(BindingFlags.Public | BindingFlags.Instance)
                        .Where(
                            m =>
                            {
                                var asField = m as FieldInfo;
                                if (asField != null) return true;

                                var asProperty = m as PropertyInfo;
                                if (asProperty != null && asProperty.SetMethod != null) return true;

                                return false;
                            }
                        );

            var publicFieldsAndPropertiesLookup = publicFieldsAndPropertiesList.ToDictionary(m => m.Name.ToLowerInvariant(), m => m);

            var ret = new Dictionary<long, MemberInfo>();

            if (membersInColumnOrder != null && membersInColumnOrder.Length > 0)
            {
                if (membersInColumnOrder.Length > ColumnCount)
                {
                    translatedColumnIndexToMemberMapping = null;
                    errorMessage = $"Too many members listed in mapping, there are only {ColumnCount:N0} columns but found {membersInColumnOrder.Length} members in map";
                    return false;
                }

                for (var i = 0; i < membersInColumnOrder.Length; i++)
                {
                    var memberName = membersInColumnOrder[i];
                    MemberInfo pairedMember;
                    if (!publicFieldsAndPropertiesLookup.TryGetValue(memberName.ToLowerInvariant(), out pairedMember))
                    {
                        translatedColumnIndexToMemberMapping = null;
                        errorMessage = $"Could not find public member named {memberName} to map column {TranslateIndex(i):N0} to";
                        return false;
                    }

                    ret[i] = pairedMember;
                }

                translatedColumnIndexToMemberMapping = ret;
                errorMessage = null;
                return true;
            }

            for (var i = 0; i < ColumnCount; i++)
            {
                var columnName = Metadata.Columns[i].Name;
                MemberInfo pairedMember;
                if (publicFieldsAndPropertiesLookup.TryGetValue(columnName.ToLowerInvariant(), out pairedMember))
                {
                    ret[i] = pairedMember;
                }
                else
                {
                    translatedColumnIndexToMemberMapping = null;
                    errorMessage = $"Cannot infer mapping, there aren't public members (fields or properties) with matching names for each column";
                    return false;
                }
            }

            translatedColumnIndexToMemberMapping = ret;
            errorMessage = null;
            return true;
        }
    }
}