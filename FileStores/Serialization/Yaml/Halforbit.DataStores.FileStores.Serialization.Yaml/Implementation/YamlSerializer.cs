using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Yaml.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using YamlDotNet.Serialization;
using ISerializer = Halforbit.DataStores.FileStores.Interface.ISerializer;

namespace Halforbit.DataStores.FileStores.Serialization.Yaml.Implementation
{
    public class YamlSerializer : ISerializer
    {
        const int _byteOrderMarkLength = 3;

        static byte[] _byteOrderMark = new byte[] { 0xEF, 0xBB, 0xBF };

        readonly Newtonsoft.Json.JsonSerializer _jsonSerializer;

        readonly Encoding _encoding;

        readonly Serializer _serializer = new Serializer();

        readonly Deserializer _deserializer = new Deserializer();

        readonly bool _omitGuidDashes;

        public YamlSerializer(string options)
        {
            if (!Enum.TryParse<YamlOptions>(options, out var o))
            {
                throw new ArgumentException($"Could not parse JSON options '{options}'.");
            }

            _jsonSerializer = new Newtonsoft.Json.JsonSerializer();

            _jsonSerializer.Converters.Add(new BigIntegerJsonConverter());

            if (o.HasFlag(YamlOptions.CamelCaseEnumValues))
            {
                _jsonSerializer.Converters.Add(new StringEnumConverter { CamelCaseText = true });
            }

            if (o.HasFlag(YamlOptions.CamelCasePropertyNames))
            {
                _jsonSerializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
            }

            if (o.HasFlag(YamlOptions.RemoveDefaultValues))
            {
                _jsonSerializer.DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate;
            }

            if (o.HasFlag(YamlOptions.OmitGuidDashes))
            {
                _jsonSerializer.Converters.Add(new GuidJsonConverter());

                _omitGuidDashes = true;
            }

            _encoding = new UTF8Encoding(false);
        }

        public Task<TValue> Deserialize<TValue>(byte[] data)
        {
            var hasBom = HasByteOrderMark(data);

            var text = _encoding.GetString(
                data,
                hasBom ? _byteOrderMarkLength : 0,
                hasBom ? data.Length - _byteOrderMarkLength : data.Length);

            // TODO: Something less ridiculous.

            var type = typeof(TValue);

            if (IsSimpleType(type))
            {
                var value = _deserializer.Deserialize<TValue>(text);

                if (value == null)
                {
                    return Task.FromResult(default(TValue));
                }
                else if (type.Equals(typeof(DateTime)) || type.Equals(typeof(DateTime?)))
                {
                    return Task.FromResult((TValue)(object)((DateTime)(object)value).ToUniversalTime());
                }
                else
                {
                    return Task.FromResult((TValue)(object)value);
                }
            }
            else if (IsArrayType(type))
            {
                var objectList = _deserializer.Deserialize<List<object>>(text);

                if (objectList == null)
                {
                    return Task.FromResult(default(TValue));
                }

                var count = objectList.Count;

                if (type.IsArray)
                {
                    var elementType = type.GetElementType();

                    var array = Array.CreateInstance(elementType, count);

                    for (var i = 0; i < count; i++)
                    {
                        array.SetValue(
                            JToken
                                .FromObject(objectList[i], _jsonSerializer)
                                .ToObject(elementType, _jsonSerializer),
                            i);
                    }

                    return Task.FromResult((TValue)(object)array);
                }
                else if (IsAssignableToGenericType(type, typeof(IReadOnlyList<>)))
                {
                    var elementType = type.GetGenericArguments()[0];

                    var list = typeof(List<>)
                        .MakeGenericType(elementType)
                        .GetConstructor(new[] { typeof(int) })
                        .Invoke(new object[] { count });

                    var addMethod = list.GetType().GetMethod(nameof(List<object>.Add));

                    for (var i = 0; i < count; i++)
                    {
                        addMethod.Invoke(
                            list,
                            new object[]
                            {
                                JToken
                                    .FromObject(objectList[i], _jsonSerializer)
                                    .ToObject(elementType, _jsonSerializer)
                            });
                    }

                    return Task.FromResult((TValue)list);
                }
                else
                {
                    throw new ArgumentException($"Cannot deserialize to type {type.Name}");
                }
            }
            else
            {
                var value = _deserializer.Deserialize<ExpandoObject>(text);

                if (value == null)
                {
                    return Task.FromResult(default(TValue));
                }
                else
                {
                    return Task.FromResult(JToken
                        .FromObject(value, _jsonSerializer)
                        .ToObject<TValue>(_jsonSerializer));
                }
            }
        }

        public Task<byte[]> Serialize<TValue>(TValue value)
        {
            // TODO: Something besides these silly gymnastics.

            var type = typeof(TValue);

            if (value == null)
            {
                return Task.FromResult(_encoding.GetBytes(_serializer.Serialize(null)));
            }
            else if (_omitGuidDashes &&
                (type.Equals(typeof(Guid)) || type.Equals(typeof(Guid?))))
            {
                return Serialize($"{value:N}");
            }
            else if (type.Equals(typeof(BigInteger)) || type.Equals(typeof(BigInteger?)))
            {
                return Task.FromResult(
                    _encoding.GetBytes(
                        _serializer.Serialize(((BigInteger)(object)value).ToString())));
            }
            else if (IsSimpleType(type))
            {
                return Task.FromResult(
                    _encoding.GetBytes(
                        _serializer.Serialize(JToken
                            .FromObject(value, _jsonSerializer)
                            .ToObject<TValue>(_jsonSerializer))));
            }
            else if (IsArrayType(type))
            {
                var json = JToken.FromObject(value, _jsonSerializer).ToString();

                var objectArray = _deserializer.Deserialize<object[]>(json);

                var yaml = _serializer.Serialize(objectArray);
                
                return Task.FromResult(_encoding.GetBytes(yaml));
            }
            else
            {
                return Task.FromResult(
                    _encoding.GetBytes(
                        _serializer.Serialize(JToken
                            .FromObject(value, _jsonSerializer)
                            .ToObject<ExpandoObject>(_jsonSerializer))));
            }
        }

        static bool IsSimpleType(Type type) => 
            type.IsPrimitive || 
            type.IsValueType ||
            type.Equals(typeof(string));

        static bool IsArrayType(Type type) => 
            typeof(IEnumerable).IsAssignableFrom(type) &&
            !typeof(JToken).IsAssignableFrom(type);

        static bool HasByteOrderMark(byte[] data)
        {
            if (data?.Length < _byteOrderMarkLength)
            {
                return false;
            }

            for (int i = 0; i < _byteOrderMarkLength; i++)
            {
                if (data[i] != _byteOrderMark[i])
                {
                    return false;
                }
            }

            return true;
        }

        static bool IsAssignableToGenericType(
            Type givenType, 
            Type genericType)
        {
            var interfaceTypes = givenType.GetInterfaces();

            foreach (var it in interfaceTypes)
            {
                if (it.IsGenericType && it.GetGenericTypeDefinition() == genericType)
                    return true;
            }

            if (givenType.IsGenericType && givenType.GetGenericTypeDefinition() == genericType)
                return true;

            Type baseType = givenType.BaseType;
            if (baseType == null) return false;

            return IsAssignableToGenericType(baseType, genericType);
        }
    }
}
