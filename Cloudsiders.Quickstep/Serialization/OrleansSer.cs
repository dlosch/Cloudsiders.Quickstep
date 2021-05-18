using System;
using System.CodeDom.Compiler;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Orleans.Concurrency;
using Orleans.Runtime;

// Internal types borrowed :) from Orleans.Xxx for modified BinaryTokenStreamReader2/Writer2
namespace Orleans.Serialization {
    internal enum SerializationTokenType : byte {
        Null = 0,
        Reference = 1, // Followed by uint byte offset from stream start of referred-to object
        Fallback = 2, // .NET-serialized; followed by a ushort length and the serialized bytes
        True = 3,
        False = 4,

        // Type definers

        // Core types
        Boolean = 10, // Only appears in generic type definitions
        Int = 11, // Followed by a 4-byte int
        Short = 12, // Followed by a 2-byte short
        Long = 13, // Followed by an 8-byte long
        Sbyte = 14, // Followed by a signed byte
        Uint = 15, // Followed by a 4-byte uint
        Ushort = 16, // Followed by a 2-byte ushort
        Ulong = 17, // Followed by an 8-byte ulong
        Byte = 18, // Followed by a byte
        Float = 19, // Followed by a 4-byte single-precision float
        Double = 20, // Followed by an 8-byte double-precision float
        Decimal = 21, // Followed by a 16-byte decimal
        String = 22, // Followed by a 4-byte length and the UTF-8 encoding of the string
        Character = 23, // Followed by a 2-byte UTF-16 character
        Guid = 24, // Followed by a 16-byte GUID
        Date = 25, // Followed by a long tick count (in UTC, not local time)
        TimeSpan = 26, // Followed by a long tick count
        IpAddress = 27, // Followed by a 16-byte IPv6 address or 12 bytes of zeroes followed by a 4-byte IPv4 address (IPv4-compatible IPv6 address)
        IpEndPoint = 28, // Followed by a 16-byte IP address and then a 4-byte int port number
        Object = 29, // Followed by nothing

        // Orleans types
        GrainId = 40, // Followed by UniqueKey
        ActivationId = 41, // Followed by UniqueKey
        SiloAddress = 42, // Followed by an IP endpoint and an int (epoch) 
        ActivationAddress = 43, // Followed by a grain id, an activation id, and a silo address, in that order
        CorrelationId = 44, // Followed by a long
        RequestId = 45, // Followed by UniqueKey

        // 47 is no longer used
        Request = 48, // Followed by the integer interface ID, the integer method ID, the integer argument count, and the arguments
        Response = 49, // Followed by either the exception or the result
        StringObjDict = 50, // Followed by the integer count, and a sequence of string/serialized object pairs; optimization for message headers
        ObjList = 51, // Followed by the integer count, and a sequence of serialized objects; optimization for message headers

        // Explicit types
        SpecifiedType = 97, // Followed by the type token, possibly plus generic arguments, or NamedType and the type name
        NamedType = 98, // Followed by the type name as a string
        ExpectedType = 99, // Indicates that the type can be deduced and is what can be deduced
        KeyedSerializer = 100, // Followed by a byte identifying which serializer is being used.

        // Generic types and collections
        Tuple = 200, // Add the count of items to this, followed by that many generic types, then the items
        Array = 210, // Add the number of dimensions to this, followed by the element type, then the dimension sizes as ints, then the elements
        List = 220, // Followed by the generic type, then the element count, then the elements
        Dictionary = 221, // Followed by the generic key type, then the generic value type, then the comparer, then the pair count, 

        // then the elements as a sequence of key, then corresponding value, then key, then value...
        KeyValuePair = 222, // Followed by the generic key type, then the generic value type, then the key, then the value
        Set = 223, // Followed by the generic element type, then the comparer, then the element count, then the elements
        SortedList = 224, // Followed by the generic type, then the comparer, then the element count, then the elements
        SortedSet = 225, // Followed by the generic type, then the comparer, then the element count, then the elements
        Stack = 226, // Followed by the generic type, then the element count, then the elements
        Queue = 227, // Followed by the generic type, then the element count, then the elements
        LinkedList = 228, // Followed by the generic type, then the element count, then the elements
        Nullable = 229, // Followed by the generic type, then either Null or the value

        // Optimized arrays
        ByteArray = 240, // Single-dimension only; followed by the count of elements, then the elements
        ShortArray = 241, // Single-dimension only; followed by the count of elements, then the elements
        IntArray = 242, // Single-dimension only; followed by the count of elements, then the elements
        LongArray = 243, // Single-dimension only; followed by the count of elements, then the elements
        UShortArray = 244, // Single-dimension only; followed by the count of elements, then the elements
        UIntArray = 245, // Single-dimension only; followed by the count of elements, then the elements
        ULongArray = 246, // Single-dimension only; followed by the count of elements, then the elements
        CharArray = 247, // Single-dimension only; followed by the count of elements, then the elements
        FloatArray = 248, // Single-dimension only; followed by the count of elements, then the elements
        DoubleArray = 249, // Single-dimension only; followed by the count of elements, then the elements
        BoolArray = 250, // Single-dimension only; followed by the count of elements, then the elements
        SByteArray = 251, // Single-dimension only; followed by the count of elements, then the elements

        // Last but not least...
        Error = 255,
    }

    internal static class TypeUtilities {
        internal static bool IsOrleansPrimitive(this Type t) => t.IsPrimitive ||
                                                                t.IsEnum ||
                                                                t == typeof(string) ||
                                                                t == typeof(DateTime) ||
                                                                t == typeof(decimal) ||
                                                                t == typeof(Guid) ||
                                                                t.IsArray && t.GetElementType().IsOrleansPrimitive();

        private static readonly ConcurrentDictionary<Type, string> typeNameCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> typeKeyStringCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, byte[]> typeKeyCache = new ConcurrentDictionary<Type, byte[]>();

        private static readonly ConcurrentDictionary<Type, bool> shallowCopyableTypes = new ConcurrentDictionary<Type, bool> {
            [typeof(decimal)] = true,
            [typeof(DateTime)] = true,
            [typeof(TimeSpan)] = true,
            [typeof(IPAddress)] = true,
            [typeof(IPEndPoint)] = true,
            [typeof(SiloAddress)] = true,
            //[typeof(GrainId)] = true,
            //[typeof(ActivationId)] = true,
            //[typeof(ActivationAddress)] = true,
            //[typeof(CorrelationId)] = true,
            [typeof(string)] = true,
            [typeof(CancellationToken)] = true,
            [typeof(Guid)] = true,
        };

        internal static bool IsOrleansShallowCopyable(this Type t) {
            if (shallowCopyableTypes.TryGetValue(t, out var result)) {
                return result;
            }

            return shallowCopyableTypes.GetOrAdd(t, IsShallowCopyableInternal(t));
        }

        private static bool IsShallowCopyableInternal(Type t) {
            if (t.IsPrimitive || t.IsEnum) {
                return true;
            }

            if (t.IsDefined(typeof(ImmutableAttribute), false)) {
                return true;
            }

            if (t.IsConstructedGenericType) {
                var def = t.GetGenericTypeDefinition();

                if (def == typeof(Immutable<>)) {
                    return true;
                }

                if (def == typeof(Nullable<>)
                    || def == typeof(Tuple<>)
                    || def == typeof(Tuple<,>)
                    || def == typeof(Tuple<,,>)
                    || def == typeof(Tuple<,,,>)
                    || def == typeof(Tuple<,,,,>)
                    || def == typeof(Tuple<,,,,,>)
                    || def == typeof(Tuple<,,,,,,>)
                    || def == typeof(Tuple<,,,,,,,>)) {
                    return Array.TrueForAll(t.GenericTypeArguments, a => IsOrleansShallowCopyable(a));
                }
            }

            if (t.IsValueType && !t.IsGenericTypeDefinition) {
                return Array.TrueForAll(t.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic), f => IsOrleansShallowCopyable(f.FieldType));
            }

            if (typeof(Exception).IsAssignableFrom(t)) {
                return true;
            }

            return false;
        }

        internal static string OrleansTypeName(this Type t) {
            string name;
            if (typeNameCache.TryGetValue(t, out name)) {
                return name;
            }

            name = TypeUtils.GetTemplatedName(t, _ => !_.IsGenericParameter);
            typeNameCache[t] = name;
            return name;
        }

        public static byte[] OrleansTypeKey(this Type t) {
            byte[] key;
            if (typeKeyCache.TryGetValue(t, out key)) {
                return key;
            }

            key = Encoding.UTF8.GetBytes(t.OrleansTypeKeyString());
            typeKeyCache[t] = key;
            return key;
        }

        public static string OrleansTypeKeyString(this Type t) {
            string key;
            if (typeKeyStringCache.TryGetValue(t, out key)) {
                return key;
            }

            var sb = new StringBuilder();
            if (t.IsGenericTypeDefinition) {
                sb.Append(GetBaseTypeKey(t));
                sb.Append('\'');
                sb.Append(t.GetGenericArguments().Length);
            }
            else if (t.IsGenericType) {
                sb.Append(GetBaseTypeKey(t));
                sb.Append('<');
                var first = true;
                foreach (var genericArgument in t.GetGenericArguments()) {
                    if (!first) {
                        sb.Append(',');
                    }

                    first = false;
                    sb.Append(OrleansTypeKeyString(genericArgument));
                }

                sb.Append('>');
            }
            else if (t.IsArray) {
                sb.Append(OrleansTypeKeyString(t.GetElementType()));
                sb.Append('[');
                if (t.GetArrayRank() > 1) {
                    sb.Append(',', t.GetArrayRank() - 1);
                }

                sb.Append(']');
            }
            else {
                sb.Append(GetBaseTypeKey(t));
            }

            key = sb.ToString();
            typeKeyStringCache[t] = key;

            return key;
        }

        private static string GetBaseTypeKey(Type t) {
            var namespacePrefix = "";
            if (t.Namespace != null && !t.Namespace.StartsWith("System.", StringComparison.Ordinal) && !t.Namespace.Equals("System")) {
                namespacePrefix = t.Namespace + '.';
            }

            if (t.IsNestedPublic) {
                return namespacePrefix + OrleansTypeKeyString(t.DeclaringType) + "." + t.Name;
            }

            return namespacePrefix + t.Name;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static string GetLocationSafe(this Assembly a) {
            if (a.IsDynamic) {
                return "dynamic";
            }

            try {
                return a.Location;
            }
            catch (Exception) {
                return "unknown";
            }
        }

        /// <summary>
        /// Returns <see langword="true"/> if a type is accessible from C# code from the specified assembly, and <see langword="false"/> otherwise.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="assembly"></param>
        /// <returns></returns>
        public static bool IsAccessibleFromAssembly(Type type, Assembly assembly) {
            if (type.IsSpecialName) return false;
            if (type.GetCustomAttribute<CompilerGeneratedAttribute>() != null) return false;

            // Obsolete types can be accessed, however obsolete types which have IsError set cannot.
            var obsoleteAttr = type.GetCustomAttribute<ObsoleteAttribute>();
            if (obsoleteAttr != null && obsoleteAttr.IsError) return false;

            // Arrays are accessible if their element type is accessible.
            if (type.IsArray) return IsAccessibleFromAssembly(type.GetElementType(), assembly);

            // Pointer and ref types are not accessible.
            if (type.IsPointer || type.IsByRef) return false;

            // Generic types are only accessible if their generic arguments are accessible.
            if (type.IsConstructedGenericType) {
                foreach (var parameter in type.GetGenericArguments()) {
                    if (!IsAccessibleFromAssembly(parameter, assembly)) return false;
                }
            }
            else if (type.IsGenericTypeDefinition) {
                // Guard against unrepresentable type constraints, which appear when generating code for some languages, such as F#.
                foreach (var parameter in type.GetTypeInfo().GenericTypeParameters) {
                    foreach (var constraint in parameter.GetGenericParameterConstraints()) {
                        if (constraint == typeof(Array) || constraint == typeof(Delegate) || constraint == typeof(Enum)) return false;
                    }
                }
            }

            // Internal types are accessible only if the declaring assembly exposes its internals to the target assembly.
            if (type.IsNotPublic || type.IsNestedAssembly || type.IsNestedFamORAssem) {
                if (!AreInternalsVisibleTo(type.Assembly, assembly)) return false;
            }

            // Nested types which are private or protected are not accessible.
            if (type.IsNestedPrivate || type.IsNestedFamily || type.IsNestedFamANDAssem) return false;

            // Nested types are otherwise accessible if their declaring type is accessible.
            if (type.IsNested) {
                return IsAccessibleFromAssembly(type.DeclaringType, assembly);
            }

            return true;
        }

        /// <summary>
        /// Returns true if <paramref name="fromAssembly"/> has exposed its internals to <paramref name="toAssembly"/>, false otherwise.
        /// </summary>
        /// <param name="fromAssembly">The assembly containing internal types.</param>
        /// <param name="toAssembly">The assembly requiring access to internal types.</param>
        /// <returns>
        /// true if <paramref name="fromAssembly"/> has exposed its internals to <paramref name="toAssembly"/>, false otherwise
        /// </returns>
        private static bool AreInternalsVisibleTo(Assembly fromAssembly, Assembly toAssembly) {
            // If the to-assembly is null, it cannot have internals visible to it.
            if (toAssembly == null) {
                return false;
            }

            if (Equals(fromAssembly, toAssembly)) return true;

            // Check InternalsVisibleTo attributes on the from-assembly, pointing to the to-assembly.
            var fullName = toAssembly.GetName().FullName;
            var shortName = toAssembly.GetName().Name;
            var internalsVisibleTo = fromAssembly.GetCustomAttributes<InternalsVisibleToAttribute>();
            foreach (var attr in internalsVisibleTo) {
                if (string.Equals(attr.AssemblyName, fullName, StringComparison.Ordinal)) return true;
                if (string.Equals(attr.AssemblyName, shortName, StringComparison.Ordinal)) return true;
            }

            return false;
        }
    }


    /// <summary>
    /// A collection of utility functions for dealing with Type information.
    /// </summary>
    internal static class TypeUtils {
        private static readonly ConcurrentDictionary<Tuple<Type, TypeFormattingOptions>, string> ParseableNameCache = new ConcurrentDictionary<Tuple<Type, TypeFormattingOptions>, string>();

        public static string GetSimpleTypeName(Type type, Predicate<Type> fullName = null) {
            if (type.IsNestedPublic || type.IsNestedPrivate) {
                if (type.DeclaringType.IsGenericType) {
                    return GetTemplatedName(GetUntemplatedTypeName(type.DeclaringType.Name),
                                            type.DeclaringType,
                                            type.GetGenericArgumentsSafe(),
                                            _ => true) + "." + GetUntemplatedTypeName(type.Name);
                }

                return GetTemplatedName(type.DeclaringType) + "." + GetUntemplatedTypeName(type.Name);
            }

            if (type.IsGenericType) return GetSimpleTypeName(fullName != null && fullName(type) ? GetFullName(type) : type.Name);

            return fullName != null && fullName(type) ? GetFullName(type) : type.Name;
        }

        public static string GetUntemplatedTypeName(string typeName) {
            var i = typeName.IndexOf('`');
            if (i > 0) {
                typeName = typeName.Substring(0, i);
            }

            i = typeName.IndexOf('<');
            if (i > 0) {
                typeName = typeName.Substring(0, i);
            }

            return typeName;
        }

        public static string GetSimpleTypeName(string typeName) {
            var i = typeName.IndexOf('`');
            if (i > 0) {
                typeName = typeName.Substring(0, i);
            }

            i = typeName.IndexOf('[');
            if (i > 0) {
                typeName = typeName.Substring(0, i);
            }

            i = typeName.IndexOf('<');
            if (i > 0) {
                typeName = typeName.Substring(0, i);
            }

            return typeName;
        }

        public static string GetTemplatedName(Type t, Predicate<Type> fullName = null) {
            if (fullName == null) {
                fullName = _ => true; // default to full type names
            }

            if (t.IsGenericType) return GetTemplatedName(GetSimpleTypeName(t, fullName), t, t.GetGenericArgumentsSafe(), fullName);

            if (t.IsArray) {
                return GetTemplatedName(t.GetElementType(), fullName)
                       + "["
                       + new string(',', t.GetArrayRank() - 1)
                       + "]";
            }

            return GetSimpleTypeName(t, fullName);
        }

        public static string GetTemplatedName(string baseName, Type t, Type[] genericArguments, Predicate<Type> fullName) {
            if (!t.IsGenericType || t.DeclaringType != null && t.DeclaringType.IsGenericType) return baseName;
            var s = baseName;
            s += "<";
            s += GetGenericTypeArgs(genericArguments, fullName);
            s += ">";
            return s;
        }

        public static Type[] GetGenericArgumentsSafe(this Type type) {
            var result = type.GetGenericArguments();

            if (type.ContainsGenericParameters) {
                // Get generic parameter from generic type definition to have consistent naming for inherited interfaces
                // Example: interface IA<TName>, class A<TOtherName>: IA<OtherName>
                // in this case generic parameter name of IA interface from class A is OtherName instead of TName.
                // To avoid this situation use generic parameter from generic type definition.
                // Matching by position in array, because GenericParameterPosition is number across generic parameters.
                // For half open generic types (IA<int,T>) T will have position 0.
                var originalGenericArguments = type.GetGenericTypeDefinition().GetGenericArguments();
                if (result.Length != originalGenericArguments.Length) // this check may be redunant
                {
                    return result;
                }

                for (var i = 0; i < result.Length; i++) {
                    if (result[i].IsGenericParameter) {
                        result[i] = originalGenericArguments[i];
                    }
                }
            }

            return result;
        }

        public static string GetGenericTypeArgs(IEnumerable<Type> args, Predicate<Type> fullName) {
            var s = string.Empty;

            var first = true;

            foreach (var genericParameter in args) {
                if (!first) {
                    s += ",";
                }

                if (!genericParameter.IsGenericType) {
                    s += GetSimpleTypeName(genericParameter, fullName);
                }
                else {
                    s += GetTemplatedName(genericParameter, fullName);
                }

                first = false;
            }

            return s;
        }

        public static string GetParameterizedTemplateName(Type type, Predicate<Type> fullName = null, bool applyRecursively = false) {
            if (fullName == null) {
                fullName = tt => true;
            }

            if (type.IsGenericType) {
                return GetParameterizedTemplateName(GetSimpleTypeName(type, fullName), type, applyRecursively, fullName);
            }

            if (fullName != null && fullName(type) == true) {
                return type.FullName;
            }

            return type.Name;
        }

        public static string GetParameterizedTemplateName(string baseName, Type type, bool applyRecursively = false, Predicate<Type> fullName = null) {
            if (fullName == null) {
                fullName = tt => false;
            }

            if (!type.IsGenericType) return baseName;

            var s = baseName;
            s += "<";
            var first = true;
            foreach (var genericParameter in type.GetGenericArguments()) {
                if (!first) {
                    s += ",";
                }

                if (applyRecursively && genericParameter.IsGenericType) {
                    s += GetParameterizedTemplateName(genericParameter, null, applyRecursively);
                }
                else {
                    s += genericParameter.FullName == null || !fullName(genericParameter)
                        ? genericParameter.Name
                        : genericParameter.FullName;
                }

                first = false;
            }

            s += ">";
            return s;
        }

        public static string GetRawClassName(string baseName, Type t) => t.IsGenericType ? baseName + '`' + t.GetGenericArguments().Length : baseName;

        public static string GetRawClassName(string typeName) {
            var i = typeName.IndexOf('[');
            return i <= 0 ? typeName : typeName.Substring(0, i);
        }

        public static string GetFullName(Type t) {
            if (t == null) throw new ArgumentNullException(nameof(t));

            if (t.IsNested && !t.IsGenericParameter) {
                return t.Namespace + "." + t.DeclaringType.Name + "." + t.Name;
            }

            if (t.IsArray) {
                return GetFullName(t.GetElementType())
                       + "["
                       + new string(',', t.GetArrayRank() - 1)
                       + "]";
            }

            // using of t.FullName breaks interop with core and full .net in one cluster, because
            // FullName of types from corelib is different.
            // .net core int: [System.Int32, System.Private.CoreLib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=7cec85d7bea7798e]
            // full .net int: [System.Int32, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089]
            return t.FullName ?? (t.IsGenericParameter ? t.Name : t.Namespace + "." + t.Name);
        }

        /// <summary>
        /// Returns all fields of the specified type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>All fields of the specified type.</returns>
        public static IEnumerable<FieldInfo> GetAllFields(this Type type) {
            const BindingFlags AllFields =
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;
            var current = type;
            while (current != typeof(object) && current != null) {
                var fields = current.GetFields(AllFields);
                foreach (var field in fields) {
                    yield return field;
                }

                current = current.BaseType;
            }
        }

        /// <summary>
        /// Returns <see langword="true"/> if <paramref name="field"/> is marked as
        /// <see cref="FieldAttributes.NotSerialized"/>, <see langword="false"/> otherwise.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <returns>
        /// <see langword="true"/> if <paramref name="field"/> is marked as
        /// <see cref="FieldAttributes.NotSerialized"/>, <see langword="false"/> otherwise.
        /// </returns>
        public static bool IsNotSerialized(this FieldInfo field)
            => (field.Attributes & FieldAttributes.NotSerialized) == FieldAttributes.NotSerialized;

        /// <summary>
        /// decide whether the class is derived from Grain
        /// </summary>
        public static bool IsGrainClass(Type type) {
            var grainType = typeof(Grain);
            var grainChevronType = typeof(Grain<>);

            if (grainType == type || grainChevronType == type) return false;

            if (!grainType.IsAssignableFrom(type)) return false;

            // exclude generated classes.
            return !type.IsDefined(typeof(GeneratedCodeAttribute), false);
        }

        public static bool IsConcreteGrainClass(Type type, out IEnumerable<string> complaints, bool complain) {
            complaints = null;
            if (!IsGrainClass(type)) return false;
            if (!type.IsAbstract) return true;

            complaints = complain ? new[] { string.Format("Grain type {0} is abstract and cannot be instantiated.", type.FullName) } : null;
            return false;
        }

        public static bool IsConcreteGrainClass(Type type, out IEnumerable<string> complaints) => IsConcreteGrainClass(type, out complaints, true);

        /// <summary>
        /// Returns a value indicating whether or not the provided <paramref name="methodInfo"/> is a grain method.
        /// </summary>
        /// <param name="methodInfo">The method.</param>
        /// <returns>A value indicating whether or not the provided <paramref name="methodInfo"/> is a grain method.</returns>
        public static bool IsGrainMethod(MethodInfo methodInfo) {
            if (methodInfo == null) throw new ArgumentNullException("methodInfo", "Cannot inspect null method info");

            if (methodInfo.IsStatic || methodInfo.IsSpecialName || methodInfo.DeclaringType == null) {
                return false;
            }

            return methodInfo.DeclaringType.IsInterface
                   && typeof(IAddressable).IsAssignableFrom(methodInfo.DeclaringType);
        }

        /// <summary>
        /// Returns the non-generic type name without any special characters.
        /// </summary>
        /// <param name="type">
        /// The type.
        /// </param>
        /// <returns>
        /// The non-generic type name without any special characters.
        /// </returns>
        public static string GetUnadornedTypeName(this Type type) {
            var index = type.Name.IndexOf('`');

            // An ampersand can appear as a suffix to a by-ref type.
            return (index > 0 ? type.Name.Substring(0, index) : type.Name).TrimEnd('&');
        }

        internal static TypeFormattingOptions typeFormattingOptions_Default = new TypeFormattingOptions();

        /// <summary>Returns a string representation of <paramref name="type"/>.</summary>
        /// <param name="type">The type.</param>
        /// <param name="options">The type formatting options.</param>
        /// <param name="getNameFunc">The delegate used to get the unadorned, simple type name of <paramref name="type"/>.</param>
        /// <returns>A string representation of the <paramref name="type"/>.</returns>
        public static string GetParseableName(this Type type, TypeFormattingOptions options = null, Func<Type, string> getNameFunc = null) {
            //options = options ?? TypeFormattingOptions.Default;
            options = options ?? typeFormattingOptions_Default;


            // If a naming function has been specified, skip the cache.
            if (getNameFunc != null) return BuildParseableName();

            return ParseableNameCache.GetOrAdd(Tuple.Create(type, options), _ => BuildParseableName());

            string BuildParseableName() {
                var builder = new StringBuilder();
                GetParseableName(type,
                                 builder,
                                 new Queue<Type>(type.IsGenericTypeDefinition
                                                     ? type.GetGenericArguments()
                                                     : type.GenericTypeArguments),
                                 options,
                                 getNameFunc ?? (t => t.GetUnadornedTypeName() + options.NameSuffix));
                return builder.ToString();
            }
        }

        /// <summary>Returns a string representation of <paramref name="type"/>.</summary>
        /// <param name="type">The type.</param>
        /// <param name="builder">The <see cref="StringBuilder"/> to append results to.</param>
        /// <param name="typeArguments">The type arguments of <paramref name="type"/>.</param>
        /// <param name="options">The type formatting options.</param>
        /// <param name="getNameFunc">Delegate that returns name for a type.</param>
        private static void GetParseableName(
            Type type,
            StringBuilder builder,
            Queue<Type> typeArguments,
            TypeFormattingOptions options,
            Func<Type, string> getNameFunc) {
            if (type.IsArray) {
                var elementType = type.GetElementType().GetParseableName(options);
                if (!string.IsNullOrWhiteSpace(elementType)) {
                    builder.AppendFormat("{0}[{1}]",
                                         elementType,
                                         new string(',', type.GetArrayRank() - 1));
                }

                return;
            }

            if (type.IsGenericParameter) {
                if (options.IncludeGenericTypeParameters) {
                    builder.Append(type.GetUnadornedTypeName());
                }

                return;
            }

            if (type.DeclaringType != null) {
                // This is not the root type.
                GetParseableName(type.DeclaringType, builder, typeArguments, options, t => t.GetUnadornedTypeName());
                builder.Append(options.NestedTypeSeparator);
            }
            else if (!string.IsNullOrWhiteSpace(type.Namespace) && options.IncludeNamespace) {
                // This is the root type, so include the namespace.
                var namespaceName = type.Namespace;
                if (options.NestedTypeSeparator != '.') {
                    namespaceName = namespaceName.Replace('.', options.NestedTypeSeparator);
                }

                if (options.IncludeGlobal) {
                    builder.AppendFormat("global::");
                }

                builder.AppendFormat("{0}{1}", namespaceName, options.NestedTypeSeparator);
            }

            if (type.IsConstructedGenericType) {
                // Get the unadorned name, the generic parameters, and add them together.
                var unadornedTypeName = getNameFunc(type);
                builder.Append(EscapeIdentifier(unadornedTypeName));
                var generics =
                    Enumerable.Range(0, Math.Min(type.GetGenericArguments().Length, typeArguments.Count))
                              .Select(_ => typeArguments.Dequeue())
                              .ToList();
                if (generics.Count > 0 && options.IncludeTypeParameters) {
                    var genericParameters = string.Join(",",
                                                        generics.Select(generic => GetParseableName(generic, options)));
                    builder.AppendFormat("<{0}>", genericParameters);
                }
            }
            else if (type.IsGenericTypeDefinition) {
                // Get the unadorned name, the generic parameters, and add them together.
                var unadornedTypeName = getNameFunc(type);
                builder.Append(EscapeIdentifier(unadornedTypeName));
                var generics =
                    Enumerable.Range(0, Math.Min(type.GetGenericArguments().Length, typeArguments.Count))
                              .Select(_ => typeArguments.Dequeue())
                              .ToList();
                if (generics.Count > 0 && options.IncludeTypeParameters) {
                    var genericParameters = string.Join(",",
                                                        generics.Select(_ => options.IncludeGenericTypeParameters ? _.ToString() : string.Empty));
                    builder.AppendFormat("<{0}>", genericParameters);
                }
            }
            else {
                builder.Append(EscapeIdentifier(getNameFunc(type)));
            }
        }

        /// <summary>
        /// Returns the <see cref="MethodInfo"/> for the simple method call in the provided <paramref name="expression"/>.
        /// </summary>
        /// <typeparam name="T">
        /// The containing type of the method.
        /// </typeparam>
        /// <typeparam name="TResult">
        /// The return type of the method.
        /// </typeparam>
        /// <param name="expression">
        /// The expression.
        /// </param>
        /// <returns>
        /// The <see cref="MethodInfo"/> for the simple method call in the provided <paramref name="expression"/>.
        /// </returns>
        public static MethodInfo Method<T, TResult>(Expression<Func<T, TResult>> expression) {
            var methodCall = expression.Body as MethodCallExpression;
            if (methodCall != null) {
                return methodCall.Method;
            }

            throw new ArgumentException("Expression type unsupported.");
        }

        /// <summary>
        /// Returns the <see cref="PropertyInfo"/> for the simple member access in the provided <paramref name="expression"/>.
        /// </summary>
        /// <typeparam name="T">
        /// The containing type of the property.
        /// </typeparam>
        /// <typeparam name="TResult">
        /// The return type of the property.
        /// </typeparam>
        /// <param name="expression">
        /// The expression.
        /// </param>
        /// <returns>
        /// The <see cref="PropertyInfo"/> for the simple member access call in the provided <paramref name="expression"/>.
        /// </returns>
        public static PropertyInfo Property<T, TResult>(Expression<Func<T, TResult>> expression) {
            var property = expression.Body as MemberExpression;
            if (property != null) {
                return property.Member as PropertyInfo;
            }

            throw new ArgumentException("Expression type unsupported.");
        }

        /// <summary>
        /// Returns the <see cref="PropertyInfo"/> for the simple member access in the provided <paramref name="expression"/>.
        /// </summary>
        /// <typeparam name="TResult">
        /// The return type of the property.
        /// </typeparam>
        /// <param name="expression">
        /// The expression.
        /// </param>
        /// <returns>
        /// The <see cref="PropertyInfo"/> for the simple member access call in the provided <paramref name="expression"/>.
        /// </returns>
        public static PropertyInfo Property<TResult>(Expression<Func<TResult>> expression) {
            var property = expression.Body as MemberExpression;
            if (property != null) {
                return property.Member as PropertyInfo;
            }

            throw new ArgumentException("Expression type unsupported.");
        }

        /// <summary>Returns the <see cref="MethodInfo"/> for the simple method call in the provided <paramref name="expression"/>.</summary>
        /// <typeparam name="T">The containing type of the method.</typeparam>
        /// <param name="expression">The expression.</param>
        /// <returns>The <see cref="MethodInfo"/> for the simple method call in the provided <paramref name="expression"/>.</returns>
        public static MethodInfo Method<T>(Expression<Func<T>> expression) {
            var methodCall = expression.Body as MethodCallExpression;
            if (methodCall != null) {
                return methodCall.Method;
            }

            throw new ArgumentException("Expression type unsupported.");
        }

        /// <summary>Returns the <see cref="MethodInfo"/> for the simple method call in the provided <paramref name="expression"/>.
        /// </summary>
        /// <typeparam name="T">The containing type of the method.</typeparam>
        /// <param name="expression">The expression.</param>
        /// <returns>The <see cref="MethodInfo"/> for the simple method call in the provided <paramref name="expression"/>.</returns>
        public static MethodInfo Method<T>(Expression<Action<T>> expression) {
            var methodCall = expression.Body as MethodCallExpression;
            if (methodCall != null) {
                return methodCall.Method;
            }

            throw new ArgumentException("Expression type unsupported.");
        }

        /// <summary>
        /// Returns the <see cref="MethodInfo"/> for the simple method call in the provided <paramref name="expression"/>.
        /// </summary>
        /// <param name="expression">
        /// The expression.
        /// </param>
        /// <returns>
        /// The <see cref="MethodInfo"/> for the simple method call in the provided <paramref name="expression"/>.
        /// </returns>
        public static MethodInfo Method(Expression<Action> expression) {
            var methodCall = expression.Body as MethodCallExpression;
            if (methodCall != null) {
                return methodCall.Method;
            }

            throw new ArgumentException("Expression type unsupported.");
        }

        private static string EscapeIdentifier(string identifier) {
            if (IsCSharpKeyword(identifier)) return "@" + identifier;
            return identifier;
        }

        internal static bool IsCSharpKeyword(string identifier) {
            switch (identifier) {
                case "abstract":
                case "add":
                case "alias":
                case "as":
                case "ascending":
                case "async":
                case "await":
                case "base":
                case "bool":
                case "break":
                case "byte":
                case "case":
                case "catch":
                case "char":
                case "checked":
                case "class":
                case "const":
                case "continue":
                case "decimal":
                case "default":
                case "delegate":
                case "descending":
                case "do":
                case "double":
                case "dynamic":
                case "else":
                case "enum":
                case "event":
                case "explicit":
                case "extern":
                case "false":
                case "finally":
                case "fixed":
                case "float":
                case "for":
                case "foreach":
                case "from":
                case "get":
                case "global":
                case "goto":
                case "group":
                case "if":
                case "implicit":
                case "in":
                case "int":
                case "interface":
                case "internal":
                case "into":
                case "is":
                case "join":
                case "let":
                case "lock":
                case "long":
                case "nameof":
                case "namespace":
                case "new":
                case "null":
                case "object":
                case "operator":
                case "orderby":
                case "out":
                case "override":
                case "params":
                case "partial":
                case "private":
                case "protected":
                case "public":
                case "readonly":
                case "ref":
                case "remove":
                case "return":
                case "sbyte":
                case "sealed":
                case "select":
                case "set":
                case "short":
                case "sizeof":
                case "stackalloc":
                case "static":
                case "string":
                case "struct":
                case "switch":
                case "this":
                case "throw":
                case "true":
                case "try":
                case "typeof":
                case "uint":
                case "ulong":
                case "unchecked":
                case "unsafe":
                case "ushort":
                case "using":
                case "value":
                case "var":
                case "virtual":
                case "void":
                case "volatile":
                case "when":
                case "where":
                case "while":
                case "yield":
                    return true;
                default:
                    return false;
            }
        }
    }
}