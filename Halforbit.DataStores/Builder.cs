using Halforbit.DataStores.DocumentStores.Interface;
using Halforbit.DataStores.FileStores;
using Halforbit.DataStores.FileStores.Compression.GZip.Implementation;
using Halforbit.DataStores.FileStores.Implementation;
using Halforbit.DataStores.FileStores.LocalStorage.Implementation;
using Halforbit.DataStores.FileStores.Serialization.ByteSerialization.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Implementation;
using Halforbit.DataStores.FileStores.Serialization.Json.Model;
using Halforbit.DataStores.Implementation;
using Halforbit.DataStores.Interface;
using Halforbit.DataStores.LocalStorage;
using Halforbit.DataStores.Validation.Interface;
using Halforbit.ObjectTools.DeferredConstruction;
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Halforbit.DataStores
{
    public class Builder :
        IConstructionNode,
        INeedsIntegration,
        INeedsRootPath,
        INeedsContentType,
        INeedsContentEncoding,
        INeedsSerialization,
        INeedsCompression,
        INeedsFileExtension,
        INeedsMap,
        INeedsDocumentMap
    {
        public Builder(Constructable root)
        {
            Root = root;
        }

        public Constructable Root{ get; }
    }

    public class Builder<TKey, TValue> :
        IConstructionNode,
        INeedsValidation<TKey, TValue>
    {
        public Builder(Constructable root)
        {
            Root = root;
        }

        public Constructable Root { get; }
    }

    public class Builder<TValue> :
        IConstructionNode,
        INeedsValidation<TValue>
    {
        public Builder(Constructable root)
        {
            Root = root;
        }

        public Constructable Root { get; }
    }

    public interface INeedsIntegration : IConstructionNode { }

    public interface INeedsMap : IConstructionNode { }

    public interface INeedsDocumentMap : IConstructionNode { }

    public interface INeedsValidation<TKey, TValue> : IConstructionNode { }

    public interface INeedsValidation<TValue> : IConstructionNode { }

    namespace LocalStorage
    {
        public interface INeedsRootPath : IConstructionNode { }
    }

    namespace FileStores
    {
        public interface INeedsContentType : IConstructionNode { }

        public interface INeedsContentEncoding : IConstructionNode { }

        public interface INeedsSerialization : IConstructionNode { }

        public interface INeedsCompression : IConstructionNode { }

        public interface INeedsFileExtension : IConstructionNode { }
    }

    public static class DataStore
    {
        public static INeedsIntegration Build()
        {
            return new Builder(null);
        }

        // File Storage

        public static INeedsSerialization Location<TConfig>(
            TConfig config,
            Func<TConfig, INeedsIntegration, INeedsSerialization> location)
        {
            return location(config, Build());
        }

        public static INeedsSerialization Location(
            Func<INeedsIntegration, INeedsSerialization> location)
        {
            return location(Build());
        }

        public static INeedsMap Format(
            this INeedsSerialization target,
            Func<INeedsSerialization, INeedsMap> format)
        {
            return format(target);
        }

        // Blob Storage

        public static INeedsContentType Location<TConfig>(
            TConfig config,
            Func<TConfig, INeedsIntegration, INeedsContentType> location)
        {
            return location(config, Build());
        }

        public static INeedsContentType Location(
            Func<INeedsIntegration, INeedsContentType> location)
        {
            return location(Build());
        }

        public static INeedsMap Format(
            this INeedsContentType target,
            Func<INeedsContentType, INeedsMap> format)
        {
            return format(target);
        }

        // Document Storage

        public static INeedsDocumentMap Location<TConfig>(
            TConfig config,
            Func<TConfig, INeedsIntegration, INeedsDocumentMap> location)
        {
            return location(config, Build());
        }

        public static INeedsDocumentMap Location(
            Func<INeedsIntegration, INeedsDocumentMap> location)
        {
            return location(Build());
        }
    }

    public static class DataStoreBuilderExtensions
    {
        // Mapping ////////////////////////////////////////////////////////////

        public static INeedsValidation<TKey, TValue> Map<TKey, TValue>(
            this INeedsDocumentMap target,
            Expression<Func<TValue, object>> partition,
            string keyMap)
            where TValue : IDocument
        {
            return new Builder<TKey, TValue>(target.Root
                .TypeArguments(typeof(TKey), typeof(TValue))
                .Argument("keyMap", $"{{{GetPropertyInfo(partition).Name}}}|{keyMap}"));
        }

        public static INeedsValidation<TKey, TValue> Map<TKey, TValue>(
            this INeedsDocumentMap target,
            string keyMap)
            where TValue : IDocument
        {
            return new Builder<TKey, TValue>(target.Root
                .TypeArguments(typeof(TKey), typeof(TValue))
                .Argument("keyMap", keyMap));
        }

        public static INeedsValidation<TKey, TValue> Map<TKey, TValue>(
            this INeedsMap target,
            string map)
        {
            return new Builder<TKey, TValue>(target.Root
                .TypeArguments(typeof(TKey), typeof(TValue))
                .Argument("keyMap", map));
        }

        public static INeedsValidation<TValue> Map<TValue>(
            this INeedsMap target,
            string map)
        {
            return new Builder<TValue>(target.Root
                .TypeArguments(typeof(object), typeof(TValue))
                .Argument("keyMap", map));
        }

        // Validation /////////////////////////////////////////////////////////

        public static IDataStore<TKey, TValue> Validation<TKey, TValue, TValidator>(
            this INeedsValidation<TKey, TValue> target,
            TValidator validator)
            where TValidator : IValidator<TKey, TValue>
        {
            return (IDataStore<TKey, TValue>)target.Root
                .Argument("validator", validator)
                .Construct();
        }

        public static IDataStore<TKey, TValue> NoValidation<TKey, TValue>(
            this INeedsValidation<TKey, TValue> target)
        {
            return (IDataStore<TKey, TValue>)target.Root
                .ArgumentNull("validator")
                .Construct();
        }

        public static IDataStore<TValue> Validation<TValue, TValidator>(
            this INeedsValidation<TValue> target,
            TValidator validator)
            where TValidator : IValidator<object, TValue>
        {
            return new SingletonDataStore<TValue>((IDataStore<object, TValue>)target.Root
                .Argument("validator", validator)
                .Construct());
        }

        public static IDataStore<TValue> NoValidation<TValue>(
            this INeedsValidation<TValue> target)
        {
            return new SingletonDataStore<TValue>((IDataStore<object, TValue>)target.Root
                .ArgumentNull("validator")
                .Construct());
        }

        static PropertyInfo GetPropertyInfo<TSource, TProperty>(
            Expression<Func<TSource, TProperty>> propertyLambda)
        {
            Type type = typeof(TSource);

            MemberExpression member;

            if (propertyLambda.Body is UnaryExpression u)
            {
                member = u.Operand as MemberExpression;
            }
            else
            {
                 member = propertyLambda.Body as MemberExpression;
            }

            if (member == null)
                throw new ArgumentException(string.Format(
                    "Expression '{0}' refers to a method, not a property.",
                    propertyLambda.ToString()));

            PropertyInfo propInfo = member.Member as PropertyInfo;
            if (propInfo == null)
                throw new ArgumentException(string.Format(
                    "Expression '{0}' refers to a field, not a property.",
                    propertyLambda.ToString()));

            if (type != propInfo.ReflectedType &&
                !type.IsSubclassOf(propInfo.ReflectedType))
                throw new ArgumentException(string.Format(
                    "Expression '{0}' refers to a property that is not from type {1}.",
                    propertyLambda.ToString(),
                    type));

            return propInfo;
        }
    }

    public static class FileStoresBuilderExtensions
    {
        public static INeedsCompression JsonSerialization(
            this INeedsSerialization target,
            JsonOptions options = JsonOptions.Default)
        {
            return new Builder(
                target.Root.Argument(
                    "serializer",
                    default(Constructable)
                        .Type(typeof(JsonSerializer))
                        .Argument("options", $"{options}")));
        }

        public static INeedsCompression ByteSerialization(
            this INeedsSerialization target) 
        {
            return new Builder(target.Root.Argument(
                "serializer",
                default(Constructable).Type(typeof(ByteSerializer))));
        }

        public static INeedsFileExtension NoCompression(this INeedsCompression target)
        {
            return new Builder(target.Root.ArgumentNull("compressor"));
        }

        public static INeedsFileExtension GZipCompression(
            this INeedsCompression target) 
        {
            return new Builder(target.Root.Argument(
                "compressor",
                default(Constructable).Type(typeof(GZipCompressor))));
        }

        public static INeedsMap FileExtension(
            this INeedsFileExtension target,
            string fileExtension)
        {
            return new Builder(target.Root.Argument("fileExtension", fileExtension));
        }
    }

    public static class LocalStorageBuilderExtensions
    {
        public static INeedsRootPath LocalStorage(
            this INeedsIntegration target) 
        {
            return new Builder(target.Root
                .Type(typeof(FileStoreDataStore<,>))
                .Argument("fileStore", default(Constructable).Type(typeof(LocalFileStore))));
        }

        public static INeedsSerialization RootPath(
            this INeedsRootPath target, 
            string rootPath) 
        {
            return new Builder(target.Root.Argument(
                "fileStore", 
                c => c.Argument("rootPath", rootPath)));
        }
    }
}
