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

        public Constructable Root { get; }
    }

    public class Builder<TKey, TValue> :
        IConstructionNode,
        IDataStoreDescription<TKey, TValue>
    {
        public Builder(Constructable root)
        {
            Root = root;
        }

        public Constructable Root { get; }
    }

    public class Builder<TValue> :
        IConstructionNode,
        IDataStoreDescription<TValue>
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

    public interface IDataStoreDescription<TKey, Value> : IConstructionNode { }

    public interface IDataStoreDescription<TValue> : IConstructionNode { }

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
        public static INeedsIntegration Describe()
        {
            return new Builder(null);
        }

        public static TResult With<TResult>(
            Func<INeedsIntegration, TResult> mixin)
        {
            return mixin(Describe());
        }

        public static TResult With<TParameter, TResult>(
            Func<INeedsIntegration, TParameter, TResult> mixin,
            TParameter parameter)
        {
            return mixin(Describe(), parameter);
        }
    }

    public static class DataStoreMixinExtensions
    {
        public static TResult With<TOperand, TResult>(
            this TOperand operand,
            Func<TOperand, TResult> mixin)
        {
            return mixin(operand);
        }

        public static TResult With<TOperand, TParameter, TResult>(
            this TOperand operand,
            Func<TOperand, TParameter, TResult> mixin,
            TParameter parameter)
        {
            return mixin(operand, parameter);
        }
    }

    public static class DataStoreBuilderExtensions
    {
        // Mapping ////////////////////////////////////////////////////////////

        public static IDataStoreDescription<TKey, TValue> Map<TKey, TValue>(
            this INeedsDocumentMap target,
            Expression<Func<TValue, object>> partition,
            string keyMap)
            where TValue : IDocument
        {
            return new Builder<TKey, TValue>(target.Root
                .TypeArguments(typeof(TKey), typeof(TValue))
                .Argument("keyMap", $"{{{GetPropertyInfo(partition).Name}}}|{keyMap}"));
        }

        public static IDataStoreDescription<TKey, TValue> Map<TKey, TValue>(
            this INeedsDocumentMap target,
            string keyMap)
            where TValue : IDocument
        {
            return new Builder<TKey, TValue>(target.Root
                .TypeArguments(typeof(TKey), typeof(TValue))
                .Argument("keyMap", keyMap));
        }

        public static IDataStoreDescription<TKey, TValue> Map<TKey, TValue>(
            this INeedsMap target,
            string map)
        {
            return new Builder<TKey, TValue>(target.Root
                .TypeArguments(typeof(TKey), typeof(TValue))
                .Argument("keyMap", map));
        }

        public static IDataStoreDescription<TValue> Map<TValue>(
            this INeedsMap target,
            string map)
        {
            return new Builder<TValue>(target.Root
                .TypeArguments(typeof(object), typeof(TValue))
                .Argument("keyMap", map));
        }

        // Validation /////////////////////////////////////////////////////////

        public static IDataStoreDescription<TKey, TValue> Validation<TKey, TValue, TValidator>(
            this IDataStoreDescription<TKey, TValue> target,
            TValidator validator)
            where TValidator : IValidator<TKey, TValue>
        {
            return new Builder<TKey, TValue>(
                target.Root.Argument("validator", validator));
        }

        public static IDataStoreDescription<TValue> Validation<TValue, TValidator>(
            this IDataStoreDescription<TValue> target,
            TValidator validator)
            where TValidator : IValidator<object, TValue>
        {
            return new Builder<TValue>(
                target.Root.Argument("validator", validator));
        }

        // Construction ///////////////////////////////////////////////////////

        public static IDataStore<TKey, TValue> Build<TKey, TValue>(
            this IDataStoreDescription<TKey, TValue> description)
        {
            return (IDataStore<TKey, TValue>)description.Root.Construct();
        }

        public static IDataStore<TValue> Build<TValue>(
            this IDataStoreDescription<TValue> description)
        {
            return new SingletonDataStore<TValue>(
                (IDataStore<object, TValue>)description.Root.Construct());
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
            this INeedsSerialization target)
        {
            return new Builder(
                target.Root.Argument(
                    "serializer",
                    default(Constructable)
                        .Type(typeof(JsonSerializer))
                        .Argument("options", $"{JsonOptions.Default}")));
        }

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
