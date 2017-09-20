using Halforbit.DataStores.FileStores.Exceptions;
using Halforbit.DataStores.FileStores.Interface;
using Halforbit.DataStores.Interface;
using Halforbit.Facets.Attributes;
using Halforbit.ObjectTools.Extensions;
using Halforbit.ObjectTools.InvariantExtraction.Implementation;
using Halforbit.ObjectTools.ObjectStringMap.Implementation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Halforbit.DataStores.FileStores.Implementation
{
    public class FileStoreDataStore<TKey, TValue> : 
        IDataStore<TKey, TValue>
    {
        readonly InvariantExtractor _invariantExtractor = new InvariantExtractor();

        readonly IFileStore _fileStore;

        readonly ISerializer _serializer;

        readonly ICompressor _compressor;

        readonly StringMap<TKey> _keyMap;

        readonly string _fileExtension;

        public FileStoreDataStore(
            IFileStore fileStore,
            ISerializer serializer,
            [Optional]ICompressor compressor = null,
            string keyMap = null,
            [Optional]string fileExtension = null)
        {
            _fileStore = fileStore ?? throw new ArgumentNullException(nameof(fileStore));

            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));

            _compressor = compressor;

            _keyMap = keyMap ?? throw new ArgumentNullException(nameof(keyMap));

            _fileExtension = fileExtension ?? string.Empty;
        }

        string InvariantPathPrefix => _keyMap
            .Map(default(TKey), allowPartialMap: true);

        public async Task<bool> Create(
            TKey key, 
            TValue value)
        {
            var path = GetPath(key);

            if (await _fileStore.Exists(path))
            {
                return false;
            }

            var contents = await GetContents(value);

            await _fileStore.WriteAllBytes(path, contents);

            return true;
        }

        public async Task<bool> Delete(TKey key)
        {
            var path = GetPath(key);

            if(!await _fileStore.Exists(path))
            {
                return false;
            }

            await _fileStore.Delete(path);

            return true;
        }

        public async Task<bool> Exists(TKey key)
        {
            var path = GetPath(key);

            return await _fileStore.Exists(path);
        }

        public async Task<TValue> Get(TKey key)
        {
            var path = GetPath(key);

            return await GetValue(path);
        }

        public async Task<IEnumerable<TKey>> ListKeys(
            Expression<Func<TKey, bool>> predicate = null)
        {
            return (await ResolveKeyPaths(predicate)).Select(kv => kv.Key);
        }

        public async Task<IEnumerable<TValue>> ListValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            var keys = await ResolveKeyPaths(predicate);

            var tasks = keys
                .Select(async keyPath => await GetValue($"{keyPath.Value}{_fileExtension}"))
                .ToArray();

            await Task.WhenAll(tasks);

            return tasks
                .Select(t => t.Result)
                .Where(e => !e.IsDefaultValue());
        }

        public async Task<IEnumerable<KeyValuePair<TKey, TValue>>> ListKeyValues(
            Expression<Func<TKey, bool>> predicate)
        {
            var tasks = (await ResolveKeyPaths(predicate))
                .Select(async kv => new KeyValuePair<TKey, TValue>(
                    kv.Key, 
                    await GetValue($"{kv.Value}{_fileExtension}")))
                .ToArray();

            await Task.WhenAll(tasks);

            return tasks.Select(t => t.Result);
        }

        public async Task<bool> Update(
            TKey key, 
            TValue value)
        {
            var path = GetPath(key);

            if(!await _fileStore.Exists(path))
            {
                return false;
            }

            var contents = await GetContents(value);

            await _fileStore.WriteAllBytes(path, contents);

            return true;
        }

        public async Task<bool> Upsert(
            TKey key, 
            TValue value)
        {
            var path = GetPath(key);

            var exists = await _fileStore.Exists(path);

            var contents = await GetContents(value);

            await _fileStore.WriteAllBytes(path, contents);

            return exists;
        }

        public async Task<bool> Upsert(
            TKey key, 
            Func<TValue, TValue> mutator)
        {
            var path = GetPath(key);

            var attemptsLeft = 100;

            while(attemptsLeft > 0)
            {
                var current = await GetValueWithETag(path);

                var mutation = mutator(current.Item1);

                var contents = await GetContents(mutation);

                var success = await _fileStore.WriteAllBytes(
                    path: path,
                    contents: contents,
                    eTag: current.Item2);

                if(success)
                {
                    return true;
                }

                if(attemptsLeft < 100)
                {
                    //Console.WriteLine("Failed with attempts left " + attemptsLeft + " " + path);
                }
                
                Task.Delay(100).Wait();

                attemptsLeft--;
            }

            throw new Exception("Failed after all attempts to conditionally upsert.");
        }

        public IQueryable<TValue> Query(TKey partialKey = default(TKey))
        {
            throw new NotImplementedException();
        }

        async Task<byte[]> GetContents(TValue value)
        {
            var contents = await _serializer.Serialize(value);

            if (_compressor != null)
            {
                contents = await _compressor.Compress(contents);
            }

            return contents;
        }

        async Task<TValue> GetValue(string path)
        {
            if (!await _fileStore.Exists(path))
            {
                return default(TValue);
            }

            var contents = (await _fileStore.ReadAllBytes(path)).Bytes;
            
            if(_compressor != null)
            {
                contents = await _compressor.Decompress(contents);
            }

            try
            {
                return await _serializer.Deserialize<TValue>(contents);
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
            if (!await _fileStore.Exists(path))
            {
                return Tuple.Create(default(TValue), default(string));
            }

            var readAllBytesResult = await _fileStore.ReadAllBytes(path, true);

            var content = readAllBytesResult.Bytes;

            if (_compressor != null)
            {
                content = await _compressor.Decompress(content);
            }

            try
            {
                return Tuple.Create(
                    await _serializer.Deserialize<TValue>(content),
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
            Expression<Func<TKey, bool>> selector)
        {
            var keyStringPrefix = ResolveKeyStringPrefix(selector);

            var files = await _fileStore.GetFiles(
                keyStringPrefix,// InvariantPathPrefix,
                _fileExtension);

            var keyPaths = files
                .Select(p => p.Substring(0, p.Length - _fileExtension.Length))
                .Where(p => p.StartsWith(keyStringPrefix))
                .Select(path => new KeyValuePair<TKey, string>(_keyMap.Map(path), path))
                .Where(kv => !kv.Key.IsDefaultValue());

            if (selector != null)
            {
                Func<TKey, bool> selectorFunc = selector.Compile();

                keyPaths = keyPaths.Where(kv => selectorFunc(kv.Key));
            }

            return keyPaths.OrderBy(pk => pk.Value);
        }

        string ResolveKeyStringPrefix(Expression<Func<TKey, bool>> selector)
        {
            var key = default(TKey);

            if (selector != null)
            {
                Expression<Func<TKey, bool>> invariantExpression;

                key = _invariantExtractor.ExtractInvariants(
                    selector,
                    out invariantExpression);
            }

            return EvaluatePath(key, true);
        }

        string EvaluatePath(
            TKey key, 
            bool allowPartialMap = false)
        {
            try
            {
                return _keyMap.Map(key, allowPartialMap);
            }
            catch (ArgumentNullException ex)
            {
                throw new IncompleteKeyException(
                    $"Path for {typeof(TValue).Name} could not be evaluated " +
                    $"because key {typeof(TKey).Name} was missing a value for {ex.ParamName}.");
            }
        }

        string GetPath(TKey key) => $"{_keyMap.Map(key)}{_fileExtension}";
    }
}
