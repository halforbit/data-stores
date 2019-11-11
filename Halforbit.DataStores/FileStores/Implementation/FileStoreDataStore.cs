using Halforbit.DataStores.FileStores.Exceptions;
using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.Interface;
using Halforbit.DataStores.Model;
using Halforbit.DataStores.Validation.Exceptions;
using Halforbit.DataStores.Validation.Interface;
using Halforbit.Facets.Attributes;
using Halforbit.ObjectTools.Collections;
using Halforbit.ObjectTools.Extensions;
using Halforbit.ObjectTools.InvariantExtraction.Implementation;
using Halforbit.ObjectTools.ObjectStringMap.Implementation;
using Halforbit.ObjectTools.ObjectStringMap.Interface;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Implementation
{
    public class FileStoreDataStore<TKey, TValue> :
        IDataStore<TKey, TValue>
    {
        static readonly bool _valueIsStream;

        readonly InvariantExtractor _invariantExtractor = new InvariantExtractor();

        readonly IFileStore _fileStore;

        readonly ISerializer _serializer;

        readonly ICompressor _compressor;

        readonly StringMap<TKey> _keyMap;

        readonly IValidator<TKey, TValue> _validator;

        readonly string _fileExtension;

        readonly Lazy<IDataStoreContext<TKey>> _context;

        static FileStoreDataStore()
        {
            _valueIsStream = typeof(TValue) == typeof(Stream);
        }

        public FileStoreDataStore(
            IFileStore fileStore,
            ISerializer serializer,
            [Optional]ICompressor compressor = null,
            [Optional]IValidator<TKey, TValue> validator = null,
            string keyMap = null,
            [Optional]string fileExtension = null)
        {
            _fileStore = fileStore ?? throw new ArgumentNullException(nameof(fileStore));

            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));

            _compressor = compressor;

            _validator = validator;

            _keyMap = keyMap ?? throw new ArgumentNullException(nameof(keyMap));

            _fileExtension = fileExtension ?? string.Empty;

            _context = new Lazy<IDataStoreContext<TKey>>(() => new DataStoreContext(
                _fileStore.FileStoreContext,
                _keyMap,
                _fileExtension));
        }

        string InvariantPathPrefix => _keyMap
            .Map(default(TKey), allowPartialMap: true);

        public IDataStoreContext<TKey> Context => _context.Value;

        public IStringMap<TKey> KeyMap => _keyMap;

        public async Task<bool> Create(
            TKey key,
            TValue value)
        {
            await ValidatePut(key, value);

            var path = GetPath(key);

            if (await _fileStore.Exists(path).ConfigureAwait(false))
            {
                return false;
            }

            if(_valueIsStream)
            {
                await _fileStore.WriteStream(path, value as Stream).ConfigureAwait(false);
            }
            else
            {
                var contents = await GetContents(value).ConfigureAwait(false);

                await _fileStore.WriteAllBytes(path, contents).ConfigureAwait(false);
            }

            return true;
        }

        public async Task<bool> Delete(TKey key)
        {
            await ValidateDelete(key);

            var path = GetPath(key);

            if (!await _fileStore.Exists(path).ConfigureAwait(false))
            {
                return false;
            }

            await _fileStore.Delete(path).ConfigureAwait(false);

            return true;
        }

        public async Task<bool> Exists(TKey key)
        {
            var path = GetPath(key);

            return await _fileStore.Exists(path).ConfigureAwait(false);
        }

        public async Task<TValue> Get(TKey key)
        {
            var path = GetPath(key);

            if (_valueIsStream)
            {
                throw new NotSupportedException();
            }
            else
            {
                return await GetValue(path).ConfigureAwait(false);
            }
        }

        public async Task<IEnumerable<TKey>> ListKeys(
            Expression<Func<TKey, bool>> predicate = null)
        {
            return (await ResolveKeyPaths(predicate).ConfigureAwait(false)).Select(kv => kv.Key);
        }

        public async Task<IEnumerable<TValue>> ListValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            var keys = await ResolveKeyPaths(predicate).ConfigureAwait(false);

            var tasks = keys
                .Select(async keyPath =>
                {
                    if(_valueIsStream)
                    {
                        throw new NotSupportedException();
                    }
                    else
                    {
                        return await GetValue($"{keyPath.Value}{_fileExtension}").ConfigureAwait(false);
                    }
                })
                .ToArray();

            await Task.WhenAll(tasks).ConfigureAwait(false);

            return tasks
                .Select(t => t.Result)
                .Where(e => !e.IsDefaultValue());
        }

        public async Task<IEnumerable<KeyValuePair<TKey, TValue>>> ListKeyValues(
            Expression<Func<TKey, bool>> predicate)
        {
            var tasks = (await ResolveKeyPaths(predicate).ConfigureAwait(false))
                .Select(async kv =>
                {
                    if(_valueIsStream)
                    {
                        throw new NotSupportedException();
                    }
                    else
                    {
                        return new KeyValuePair<TKey, TValue>(
                            kv.Key,
                            await GetValue($"{kv.Value}{_fileExtension}"));
                    }
                })
                .ToArray();

            await Task.WhenAll(tasks).ConfigureAwait(false);

            return tasks.Select(t => t.Result);
        }

        public async Task<bool> Update(
            TKey key,
            TValue value)
        {
            await ValidatePut(key, value);

            var path = GetPath(key);

            if (!await _fileStore.Exists(path).ConfigureAwait(false))
            {
                return false;
            }

            if (_valueIsStream)
            {
                await _fileStore.WriteStream(path, value as Stream).ConfigureAwait(false);
            }
            else
            {
                var contents = await GetContents(value).ConfigureAwait(false);

                await _fileStore.WriteAllBytes(path, contents).ConfigureAwait(false);
            }

            return true;
        }

        public async Task<bool> Upsert(
            TKey key,
            TValue value)
        {
            await ValidatePut(key, value);

            var path = GetPath(key);

            var exists = await _fileStore.Exists(path).ConfigureAwait(false);

            if(_valueIsStream)
            {
                await _fileStore.WriteStream(path, value as Stream).ConfigureAwait(false);
            }
            else
            {
                var contents = await GetContents(value).ConfigureAwait(false);

                await _fileStore.WriteAllBytes(path, contents).ConfigureAwait(false);
            }

            return exists;
        }

        public async Task<bool> Upsert(
            TKey key,
            Func<TValue, TValue> mutator)
        {
            var path = GetPath(key);

            var attemptsLeft = 100;

            while (attemptsLeft > 0)
            {
                var current = await GetValueWithETag(path).ConfigureAwait(false);

                var mutation = mutator(current.Item1);

                await ValidatePut(key, mutation);

                var success = default(bool);

                if(_valueIsStream)
                {
                    throw new NotSupportedException();
                }
                else
                {
                    var contents = await GetContents(mutation).ConfigureAwait(false);

                    success = await _fileStore.WriteAllBytes(
                        path: path,
                        contents: contents,
                        eTag: current.Item2).ConfigureAwait(false);
                }

                if (success)
                {
                    return true;
                }

                if (attemptsLeft < 100)
                {
                    //Console.WriteLine("Failed with attempts left " + attemptsLeft + " " + path);
                }

                Task.Delay(100).Wait();

                attemptsLeft--;
            }

            throw new Exception("Failed after all attempts to conditionally upsert.");
        }

        public async Task<bool> Upsert(TKey key, Func<TValue, Task<TValue>> mutator)
        {
            var path = GetPath(key);

            var attemptsLeft = 100;

            while (attemptsLeft > 0)
            {
                var current = await GetValueWithETag(path).ConfigureAwait(false);

                var mutation = await mutator(current.Item1);

                await ValidatePut(key, mutation);

                var success = default(bool);

                if (_valueIsStream)
                {
                    throw new NotSupportedException();
                }
                else
                {
                    var contents = await GetContents(mutation).ConfigureAwait(false);

                    success = await _fileStore.WriteAllBytes(
                        path: path,
                        contents: contents,
                        eTag: current.Item2).ConfigureAwait(false);
                }

                if (success)
                {
                    return true;
                }

                if (attemptsLeft < 100)
                {
                    //Console.WriteLine("Failed with attempts left " + attemptsLeft + " " + path);
                }

                Task.Delay(100).Wait();

                attemptsLeft--;
            }

            throw new Exception("Failed after all attempts to conditionally upsert.");
        }

        public IQuerySession<TKey, TValue> StartQuery()
        {
            return new QuerySession(this);
        }

        async Task<byte[]> GetContents(TValue value)
        {
            var contents = await _serializer.Serialize(value).ConfigureAwait(false);

            if (_compressor != null)
            {
                contents = await _compressor.Compress(contents).ConfigureAwait(false);
            }

            return contents;
        }

        async Task<TValue> GetValue(string path)
        {
            if (!await _fileStore.Exists(path).ConfigureAwait(false))
            {
                return default(TValue);
            }

            var contents = (await _fileStore.ReadAllBytes(path).ConfigureAwait(false)).Bytes;

            if (_compressor != null)
            {
                contents = await _compressor.Decompress(contents).ConfigureAwait(false);
            }

            try
            {
                return await _serializer.Deserialize<TValue>(contents).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new DeserializationException(
                    $"Error while deserializing a {typeof(TValue).Name}: {ex.Message}",
                    ex);
            }
        }

        async Task<Tuple<TValue, string>> GetValueWithETag(string path)
        {
            if (!await _fileStore.Exists(path).ConfigureAwait(false))
            {
                return Tuple.Create(default(TValue), default(string));
            }

            var readAllBytesResult = await _fileStore.ReadAllBytes(path, true).ConfigureAwait(false);

            var content = readAllBytesResult.Bytes;

            if (_compressor != null)
            {
                content = await _compressor.Decompress(content).ConfigureAwait(false);
            }

            try
            {
                return Tuple.Create(
                    await _serializer.Deserialize<TValue>(content).ConfigureAwait(false),
                    readAllBytesResult.ETag);
            }
            catch (Exception ex)
            {
                throw new DeserializationException(
                    $"Error while deserializing a {typeof(TValue).Name}: {ex.Message}",
                    ex);
            }
        }

        async Task<IEnumerable<KeyValuePair<TKey, string>>> ResolveKeyPaths(
            string keyStringPrefix)
        {
            var files = await _fileStore.GetFiles(
                keyStringPrefix,// InvariantPathPrefix,
                _fileExtension).ConfigureAwait(false);

            var keyPaths = files
                .Select(p => p.Substring(0, p.Length - _fileExtension.Length))
                .Where(p => p.StartsWith(keyStringPrefix))
                .Select(path => new KeyValuePair<TKey, string>(_keyMap.Map(path), path))
                .Where(kv => !kv.Key.IsDefaultValue());

            return keyPaths.OrderBy(pk => pk.Value);
        }

        async Task<IEnumerable<KeyValuePair<TKey, string>>> ResolveKeyPaths(
            Expression<Func<TKey, bool>> selector)
        {
            var keyStringPrefix = ResolveKeyStringPrefix(selector);

            var keyPaths = await ResolveKeyPaths(keyStringPrefix).ConfigureAwait(false);

            if (selector != null)
            {
                Func<TKey, bool> selectorFunc = selector.Compile();

                keyPaths = keyPaths.Where(kv => selectorFunc(kv.Key));
            }

            return keyPaths;
        }

        string ResolveKeyStringPrefix(Expression<Func<TKey, bool>> selector)
        {
            var memberValues = EmptyReadOnlyDictionary<string, object>.Instance as
                IReadOnlyDictionary<string, object>;

            if (selector != null)
            {
                memberValues = _invariantExtractor.ExtractInvariantDictionary(
                    selector,
                    out Expression<Func<TKey, bool>> invariantExpression);
            }

            return EvaluatePath(memberValues, true);
        }

        string EvaluatePath(
            IReadOnlyDictionary<string, object> memberValues,
            bool allowPartialMap = false)
        {
            try
            {
                return _keyMap.Map(memberValues, allowPartialMap);
            }
            catch (ArgumentNullException ex)
            {
                throw new IncompleteKeyException(
                    $"Path for {typeof(TValue).Name} could not be evaluated " +
                    $"because key {typeof(TKey).Name} was missing a value for {ex.ParamName}.");
            }
        }

        string GetPath(TKey key) => $"{_keyMap.Map(key)}{_fileExtension}";

        async Task ValidatePut(TKey key, TValue value)
        {
            if(_validator != null)
            {
                var validationErrors = await _validator.ValidatePut(key, value, _keyMap);

                if (validationErrors?.Any() ?? false)
                {
                    throw new ValidationException(validationErrors);
                }
            }
        }

        async Task ValidateDelete(TKey key)
        {
            if(_validator != null)
            {
                var validationErrors = await _validator.ValidateDelete(key, _keyMap);

                if (validationErrors?.Any() ?? false)
                {
                    throw new ValidationException(validationErrors);
                }
            }
        }

        public async Task<bool> GetToStream(TKey key, Stream stream)
        {
            var path = GetPath(key);

            return await _fileStore.ReadStream(path, stream);
        }

        class QuerySession : IQuerySession<TKey, TValue>
        {
            readonly FileStoreDataStore<TKey, TValue> _dataStore;

            public QuerySession(FileStoreDataStore<TKey, TValue> dataStore)
            {
                _dataStore = dataStore;
            }

            public void Dispose() { }

            public IQueryable<TValue> Query(Expression<Func<TKey, bool>> selector = null)
            {
                var memberValues = EmptyReadOnlyDictionary<string, object>.Instance as
                    IReadOnlyDictionary<string, object>;

                if (selector != null)
                {
                    memberValues = _dataStore._invariantExtractor.ExtractInvariantDictionary(
                        selector,
                        out Expression<Func<TKey, bool>> invariantExpression);
                }

                var prefix = _dataStore.EvaluatePath(memberValues, allowPartialMap: true);

                var keys = _dataStore.ResolveKeyPaths(prefix).Result;

                var tasks = keys
                    .Select(async keyPath => await _dataStore.GetValue(
                        $"{keyPath.Value}{_dataStore._fileExtension}").ConfigureAwait(false))
                    .ToArray();

                Task.WaitAll(tasks);

                return tasks
                    .Select(t => t.Result)
                    .Where(e => !e.IsDefaultValue())
                    .AsQueryable();
            }
        }

        class DataStoreContext : IDataStoreContext<TKey>
        {
            readonly IFileStoreContext _fileStoreContext;

            readonly StringMap<TKey> _keyMap;

            readonly string _fileExtension;

            public DataStoreContext(
                IFileStoreContext fileStoreContext,
                StringMap<TKey> keyMap,
                string fileExtension)
            {
                _fileStoreContext = fileStoreContext;

                _keyMap = keyMap;

                _fileExtension = fileExtension;
            }

            public async Task<string> AcquireLease(TKey key, TimeSpan leaseTime)
            {
                return await _fileStoreContext.AcquireLease(GetPath(key), leaseTime);
            }

            public async Task BreakLease(TKey key, TimeSpan breakReleaseTime)
            {
                await _fileStoreContext.BreakLease(GetPath(key), breakReleaseTime);
            }

            public async Task<string> ChangeLease(TKey key, string currentLeaseId)
            {
                return await _fileStoreContext.ChangeLease(GetPath(key), currentLeaseId);
            }

            public async Task<EntityInfo> GetEntityInfo(TKey key)
            {
                return await _fileStoreContext.GetEntityInfo(GetPath(key)).ConfigureAwait(false);
            }

            public async Task<IReadOnlyDictionary<string, string>> GetMetadata(
                TKey key,
                bool percentDecodeValues = true)
            {
                var keyValues = await _fileStoreContext.GetMetadata(GetPath(key), percentDecodeValues).ConfigureAwait(false);

                return keyValues;
            }

            public async Task<Uri> GetSharedAccessUrl(
                TKey key,
                DateTime expiration,
                Access access)
            {
                return await _fileStoreContext.GetSharedAccessUrl(
                    GetPath(key),
                    expiration,
                    access).ConfigureAwait(false);
            }

            public Task<IReadOnlyDictionary<TKey, EntityInfo>> ListEntityInfos(
                Expression<Func<TKey, bool>> selector = null)
            {
                throw new NotImplementedException();
            }

            public Task<IReadOnlyDictionary<TKey, IReadOnlyDictionary<string, string>>> ListMetadatas(
                Expression<Func<TKey, bool>> selector = null)
            {
                throw new NotImplementedException();
            }

            public async Task ReleaseLease(TKey key, string leaseId)
            {
                await _fileStoreContext.ReleaseLease(GetPath(key), leaseId);
            }

            public async Task RenewLease(TKey key, string leaseId)
            {
                await _fileStoreContext.RenewLease(GetPath(key), leaseId);
            }

            public async Task SetEntityInfo(
                TKey key,
                EntityInfo entityInfo)
            {
                await _fileStoreContext.SetEntityInfo(
                    GetPath(key),
                    entityInfo).ConfigureAwait(false);
            }

            public async Task SetMetadata(
                TKey key,
                IReadOnlyDictionary<string, string> keyValues,
                bool percentEncodeValues = true)
            {
                await _fileStoreContext.SetMetadata(
                    GetPath(key),
                    keyValues,
                    percentEncodeValues).ConfigureAwait(false);
            }

            string GetPath(TKey key) => $"{_keyMap.Map(key)}{_fileExtension}";
        }
    }
}
