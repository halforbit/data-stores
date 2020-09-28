using Halforbit.DataStores.FileStores.Exceptions;
using Halforbit.DataStores.Internal;
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
        
        readonly IReadOnlyList<IObserver<TKey, TValue>> _typedObservers;
        
        readonly IReadOnlyList<IObserver> _untypedObservers;

        readonly IReadOnlyList<IMutator<TKey, TValue>> _typedMutators;

        readonly IReadOnlyList<IMutator> _untypedMutators;

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
            [Optional]string fileExtension = null,
            [Optional]IReadOnlyList<IObserver<TKey, TValue>> typedObservers = null,
            [Optional]IReadOnlyList<IObserver> untypedObservers = null,
            [Optional]IReadOnlyList<IMutator<TKey, TValue>> typedMutators = null,
            [Optional]IReadOnlyList<IMutator> untypedMutators = null)
        {
            _fileStore = fileStore ?? throw new ArgumentNullException(nameof(fileStore));

            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));

            _compressor = compressor;

            _validator = validator;

            _keyMap = keyMap ?? throw new ArgumentNullException(nameof(keyMap));

            _fileExtension = fileExtension ?? string.Empty;

            _typedObservers = typedObservers ?? EmptyReadOnlyList<IObserver<TKey, TValue>>.Instance;
            
            _untypedObservers = untypedObservers ?? EmptyReadOnlyList<IObserver>.Instance;

            _typedMutators = typedMutators ?? EmptyReadOnlyList<IMutator<TKey, TValue>>.Instance;

            _untypedMutators = untypedMutators ?? EmptyReadOnlyList<IMutator>.Instance;

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
            key.ThrowIfKeyIsDefaultValue();

            value = await MutatePut(key, value).ConfigureAwait(false);

            await ValidatePut(key, value).ConfigureAwait(false);

            var path = GetPath(key);

            if (await _fileStore.Exists(path).ConfigureAwait(false))
            {
                return false;
            }

            await ObserveBeforePut(key, value).ConfigureAwait(false);

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

        public Task<IReadOnlyList<KeyValuePair<TKey, bool>>> Create(
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            foreach (var kv in values) kv.Key.ThrowIfKeyIsDefaultValue();

            return this.BulkCreate(values);
        }

        public async Task<bool> Delete(TKey key)
        {
            key.ThrowIfKeyIsDefaultValue();

            await ValidateDelete(key).ConfigureAwait(false);

            var path = GetPath(key);

            if (!await _fileStore.Exists(path).ConfigureAwait(false))
            {
                return false;
            }

            foreach (var observer in _typedObservers) await observer.BeforeDelete(key);

            foreach (var observer in _untypedObservers) await observer.BeforeDelete(key);

            await _fileStore.Delete(path).ConfigureAwait(false);

            return true;
        }

        public Task<IReadOnlyList<KeyValuePair<TKey,bool>>> Delete(
            IEnumerable<TKey> keys)
        {
            foreach (var k in keys) k.ThrowIfKeyIsDefaultValue();

            return this.BulkDelete(keys);
        }

        public async Task<bool> Exists(TKey key)
        {
            key.ThrowIfKeyIsDefaultValue();

            var path = GetPath(key);

            return await _fileStore.Exists(path).ConfigureAwait(false);
        }

        public async Task<TValue> Get(TKey key)
        {
            key.ThrowIfKeyIsDefaultValue();

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

        public Task<IReadOnlyList<KeyValuePair<TKey,TValue>>> Get(
            IEnumerable<TKey> keys)
        {
            foreach (var k in keys) k.ThrowIfKeyIsDefaultValue();

            return this.BulkGet(keys);
        }

        public async Task<IEnumerable<TKey>> ListKeys(
            Expression<Func<TKey, bool>> predicate = null)
        {
            return (await ResolveKeyPaths(predicate).ConfigureAwait(false)).Select(kv => kv.Key);
        }

        public async Task<IEnumerable<TValue>> ListValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            if (_valueIsStream)
            {
                throw new NotSupportedException();
            }
            
            var keys = await ResolveKeyPaths(predicate).ConfigureAwait(false);

            var values = await keys.SelectAsync(keyPath =>
                GetValue($"{keyPath.Value}{_fileExtension}"),
                maxPending: DataStoresConcurrency.MaxOperations).ConfigureAwait(false);

            return values.Where(e => !e.IsDefaultValue());
        }

        public async Task<IEnumerable<KeyValuePair<TKey, TValue>>> ListKeyValues(
            Expression<Func<TKey, bool>> predicate)
        {
            if (_valueIsStream)
            {
                throw new NotSupportedException();
            }

            var keys = await ResolveKeyPaths(predicate).ConfigureAwait(false);

            var values = await keys.SelectAsync(async kv =>
                    new KeyValuePair<TKey, TValue>(kv.Key, await GetValue($"{kv.Value}{_fileExtension}")),
                maxPending: DataStoresConcurrency.MaxOperations).ConfigureAwait(false);

            return values;
        }

        public async Task<bool> Update(
            TKey key,
            TValue value)
        {
            key.ThrowIfKeyIsDefaultValue();

            value = await MutatePut(key, value).ConfigureAwait(false);

            await ValidatePut(key, value).ConfigureAwait(false);

            var path = GetPath(key);

            if (!await _fileStore.Exists(path).ConfigureAwait(false))
            {
                return false;
            }

            await ObserveBeforePut(key, value).ConfigureAwait(false);

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

        public Task<IReadOnlyList<KeyValuePair<TKey,bool>>> Update(
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            foreach (var kv in values) kv.Key.ThrowIfKeyIsDefaultValue();

            return this.BulkUpdate(values);
        }

        public async Task Upsert(
            TKey key,
            TValue value)
        {
            key.ThrowIfKeyIsDefaultValue();

            value = await MutatePut(key, value).ConfigureAwait(false);

            await ValidatePut(key, value).ConfigureAwait(false);

            var path = GetPath(key);

            await ObserveBeforePut(key, value).ConfigureAwait(false);

            if (_valueIsStream)
            {
                await _fileStore.WriteStream(path, value as Stream).ConfigureAwait(false);
            }
            else
            {
                var contents = await GetContents(value).ConfigureAwait(false);

                await _fileStore.WriteAllBytes(path, contents).ConfigureAwait(false);
            }
        }

        public Task Upsert(
            IEnumerable<KeyValuePair<TKey, TValue>> values)
        {
            foreach (var kv in values) kv.Key.ThrowIfKeyIsDefaultValue();

            return this.BulkUpsert(values);
        }

        public async Task Upsert(
            TKey key,
            Func<TValue, TValue> mutator)
        {
            key.ThrowIfKeyIsDefaultValue();

            var path = GetPath(key);

            var attemptsLeft = 100;

            while (attemptsLeft > 0)
            {
                var current = await GetValueWithETag(path).ConfigureAwait(false);

                var mutation = await MutatePut(key, mutator(current.Item1)).ConfigureAwait(false);

                await ValidatePut(key, mutation).ConfigureAwait(false);

                await ObserveBeforePut(key, mutation).ConfigureAwait(false);

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
                    return;
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

        public async Task Upsert(TKey key, Func<TValue, Task<TValue>> mutator)
        {
            key.ThrowIfKeyIsDefaultValue();

            var path = GetPath(key);

            var attemptsLeft = 100;

            while (attemptsLeft > 0)
            {
                var current = await GetValueWithETag(path).ConfigureAwait(false);

                var mutated = await mutator(current.Item1).ConfigureAwait(false);
                var mutation = await MutatePut(key, mutated).ConfigureAwait(false);

                await ValidatePut(key, mutation).ConfigureAwait(false);

                await ObserveBeforePut(key, mutation).ConfigureAwait(false);

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
                    return;
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

        public IQueryable<TValue> Query(Expression<Func<TKey, bool>> predicate = null)
        {
            return StartQuery().Query(predicate);
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
            if (_validator != null)
            {
                var validationErrors = await _validator.ValidatePut(key, value, _keyMap).ConfigureAwait(false);;

                if (validationErrors?.Any() ?? false)
                {
                    throw new ValidationException(validationErrors);
                }
            }
        }

        async Task ValidateDelete(TKey key)
        {
            if (_validator != null)
            {
                var validationErrors = await _validator.ValidateDelete(key, _keyMap).ConfigureAwait(false);;

                if (validationErrors?.Any() ?? false)
                {
                    throw new ValidationException(validationErrors);
                }
            }
        }

        async Task<TValue> MutatePut(TKey key, TValue value)
        {
            foreach (var mutator in _typedMutators)
                value = await mutator.MutatePut(key, value).ConfigureAwait(false);

            foreach (var mutator in _untypedMutators)
                value = (TValue)await mutator.MutatePut(key, value).ConfigureAwait(false);

            return value;
        }

        async Task ObserveBeforePut(TKey key, TValue value)
        {
            foreach (var observer in _typedObservers)
                await observer.BeforePut(key, value).ConfigureAwait(false);

            foreach (var observer in _untypedObservers)
                await observer.BeforePut(key, value).ConfigureAwait(false);
        }

        public async Task<bool> GetToStream(TKey key, Stream stream)
        {
            key.ThrowIfKeyIsDefaultValue();

            var path = GetPath(key);

            return await _fileStore.ReadStream(path, stream).ConfigureAwait(false);
        }

        public async Task<IEnumerable<TResult>> BatchQuery<TItem, TResult>(
            IEnumerable<TItem> items,
            Func<IEnumerable<TItem>, IQueryable<TValue>, IQueryable<TResult>> query,
            Expression<Func<TKey, bool>> predicate = null,
            int batchSize = 500)
        {
            throw new NotImplementedException();
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
                return await _fileStoreContext.AcquireLease(GetPath(key), leaseTime).ConfigureAwait(false);
            }

            public async Task BreakLease(TKey key, TimeSpan breakReleaseTime)
            {
                await _fileStoreContext.BreakLease(GetPath(key), breakReleaseTime).ConfigureAwait(false);
            }

            public async Task<string> ChangeLease(TKey key, string currentLeaseId)
            {
                return await _fileStoreContext.ChangeLease(GetPath(key), currentLeaseId).ConfigureAwait(false);
            }

            public async Task<EntityInfo> GetEntityInfo(TKey key)
            {
                return await _fileStoreContext.GetEntityInfo(GetPath(key)).ConfigureAwait(false);
            }

            public async Task<Uri> GetEntityUrl(TKey key)
            {
                return await _fileStoreContext.GetEntityUrl(GetPath(key)).ConfigureAwait(false);
            }

            public async Task<IReadOnlyDictionary<string, string>> GetMetadata(
                TKey key,
                bool percentDecodeValues = true)
            {
                var keyValues = await _fileStoreContext.GetMetadata(GetPath(key),
                    percentDecodeValues).ConfigureAwait(false);

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
                await _fileStoreContext.ReleaseLease(GetPath(key), leaseId).ConfigureAwait(false);
            }

            public async Task RenewLease(TKey key, string leaseId)
            {
                await _fileStoreContext.RenewLease(GetPath(key), leaseId).ConfigureAwait(false);
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
