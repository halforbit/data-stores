using Halforbit.DataStores.FileStores.Exceptions;
using Halforbit.DataStores.Interface;
using Halforbit.DataStores.TableStores.AzureTables.Exceptions;
using Halforbit.DataStores.Validation.Exceptions;
using Halforbit.DataStores.Validation.Interface;
using Halforbit.Facets.Attributes;
using Halforbit.ObjectTools.Collections;
using Halforbit.ObjectTools.InvariantExtraction.Implementation;
using Halforbit.ObjectTools.ObjectStringMap.Implementation;
using Halforbit.ObjectTools.ObjectStringMap.Interface;
using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Halforbit.DataStores.TableStores.AzureTables.Implementation
{
    public class AzureTableStore<TKey, TValue> :
        IDataStore<TKey, TValue>
    {
        readonly string _connectionString;

        readonly string _tableName;

        readonly InvariantExtractor _invariantExtractor = new InvariantExtractor();

        readonly StringMap<TKey> _keyMap;

        readonly IValidator<TKey, TValue> _validator;

        const char KeyMapDelimiter = '|';

        static readonly Regex InvalidCharactersRegex = 
            new Regex(@"/\#?", RegexOptions.Compiled);

        public AzureTableStore(
            string connectionString,
            string tableName,
            string keyMap,
            [Optional]IValidator<TKey, TValue> validator = null)
        {
            _connectionString = connectionString;

            _tableName = tableName;

            if (InvalidCharactersRegex.IsMatch(keyMap)) 
            {
                throw new InvalidAzureTableKeyMapException(
                    "Key map includes characters that are invalid in the context of Azure Table Stores.");
            }
            
            _keyMap = keyMap;

            _validator = validator;
        }

        public IDataStoreContext<TKey> Context => throw new NotImplementedException();

        public IStringMap<TKey> KeyMap => _keyMap;
        
        public async Task<bool> Create(TKey key, TValue value)
        {
            //TODO: At present, we can only use objects that have a default constructor and properties that have a setter.
            //This limitation is due to our usage of the TableEntityAdapter<T> class. A custom implementation for reading
            //and writing our TableEntity objects should allow us to lift this limitation in the future.
            /*******
            Strategy for building objects to send to Table Storage:
            1. Identify all properties (with public getters?).
            2. For each property, write its name/value as an entry in the dictionary.
            3. Any complex properties should be serialized recursively.
            4. Write the object.

            Partition Key: typeof(TValue).Name;
            Row Key: KeyMap.Map(key); - with regex replacement of invalid characters
            ********/

            /*******
            Strategy for reconstructing objects from Table Storage:
            1. Look for constructors. If a default constructor exists, use it.
            2. If there is no default constructor, look for the constructor with the most arguments.
            2b. Alternatively, try to find the best one with the known properties.
            3. Convert all of the properties into the proper name format (use a regex for matching).
            4. Create an instance of the object with the chosen constructor and properties.
            5. For any properties with public setters that are not in the constructor, set them.
            6. Return the object.
            ********/

            if (await Exists(key)) 
            {
                return false;
            }

            await ValidatePut(key, value);

            var tableEntity = ConvertToTableEntity(key, value);

            return await ExecuteTableOperationAsync(
                TableOperation.Insert(tableEntity), 
                HttpStatusCode.NoContent);
        }

        public async Task<bool> Delete(TKey key)
        {
            var existingTableEntity = await GetTableEntity(key);

            if (existingTableEntity == null) 
            {
                return false;
            }

            await ValidateDelete(key);

            return await ExecuteTableOperationAsync(
                TableOperation.Delete(existingTableEntity), 
                HttpStatusCode.NoContent);
        }

        public async Task<bool> Exists(TKey key)
        {
            return await Get(key) != null;
        }

        public async Task<TValue> Get(TKey key)
        {
            var tableEntity = await GetTableEntity(key);

            return tableEntity == null
                ? default(TValue)
                : tableEntity.OriginalEntity;
        }

        public async Task<IEnumerable<TKey>> ListKeys(Expression<Func<TKey, bool>> predicate = null)
        {
            return (await ListTableEntities(predicate))
                .Select(tableEntity => GetKey(tableEntity.PartitionKey, tableEntity.RowKey));
        }

        public async Task<IEnumerable<TValue>> ListValues(Expression<Func<TKey, bool>> predicate = null)
        {
            return (await ListTableEntities(predicate))
                .Select(tableEntity => tableEntity.OriginalEntity);
        }

        public async Task<IEnumerable<KeyValuePair<TKey, TValue>>> ListKeyValues(
            Expression<Func<TKey, bool>> predicate = null)
        {
            return (await ListTableEntities(predicate))
                .ToDictionary(
                    tableEntity => GetKey(tableEntity.PartitionKey, tableEntity.RowKey),
                    tableEntity => tableEntity.OriginalEntity);
        }

        public async Task<bool> Update(TKey key, TValue value)
        {
            var existingTableEntity = await GetTableEntity(key);
            if (existingTableEntity == null) 
            {
                return false;
            }

            await ValidatePut(key, value);

            var updatedTableEntity = ConvertToTableEntity(key, value, existingTableEntity.ETag);

            return await ExecuteTableOperationAsync(
                TableOperation.Replace(updatedTableEntity), 
                HttpStatusCode.NoContent);
        }

        public async Task<bool> Upsert(TKey key, TValue value)
        {
            await ValidatePut(key, value);

            var tableEntity = ConvertToTableEntity(key, value);

            return await ExecuteTableOperationAsync(
                TableOperation.InsertOrReplace(tableEntity), 
                HttpStatusCode.NoContent);
        }

        public async Task<bool> Upsert(TKey key, Func<TValue, TValue> mutator)
        {
            var existing = await Get(key).ConfigureAwait(false);

            if (existing == null) 
            {
                return false;
            }

            var mutation = mutator(existing);

            await ValidatePut(key, mutation);

            var tableEntity = ConvertToTableEntity(key, mutation);

            return await ExecuteTableOperationAsync(
                TableOperation.Replace(tableEntity), 
                HttpStatusCode.NoContent);
        }

        public Task<bool> Upsert(TKey key, Func<TValue, Task<TValue>> mutator)
        {
            throw new NotImplementedException();
        }

        public IQuerySession<TKey, TValue> StartQuery()
        {
            throw new NotSupportedException();
        }

        CloudTable GetTable() 
        {
            var storageAccount = CloudStorageAccount.Parse(_connectionString);
            var tableClient = storageAccount.CreateCloudTableClient();
            return tableClient.GetTableReference(_tableName);
        }

        (string PartitionKey, string RowKey) GetPartitionAndRowKey(TKey key)
        {
            var mappedInput = _keyMap.Map(key);

            var splitKeys = mappedInput.Split(KeyMapDelimiter);
            if (splitKeys.Length != 2) 
            {
                throw new InvalidAzureTableKeyMapException(
                    $"Azure Table Store key map must include a partition and row key, " +
                    $"separated by the '{KeyMapDelimiter}' character.");
            }

            return (splitKeys[0], splitKeys[1]);
        }

        TKey GetKey(string partitionKey, string rowKey)
        {
            return _keyMap.Map($"{partitionKey}{KeyMapDelimiter}{rowKey}");
        }

        TableEntityAdapter<TValue> ConvertToTableEntity(
            TKey key,
            TValue value,
            string eTag = null) 
        {
            (var partitionKey, var rowKey) = GetPartitionAndRowKey(key);

            return new TableEntityAdapter<TValue>(
                value,
                partitionKey,
                rowKey
            )
            {
                ETag = eTag
            };
        }

        async Task<TableEntityAdapter<TValue>> GetTableEntity(TKey key)
        {
            (var partitionKey, var rowKey) = GetPartitionAndRowKey(key);

            var retrieveOperation = TableOperation.Retrieve<TableEntityAdapter<TValue>>(
                partitionKey,
                rowKey);

            var result = await GetTable().ExecuteAsync(retrieveOperation).ConfigureAwait(false);

            return result?.Result as TableEntityAdapter<TValue>;
        }

        async Task<IEnumerable<TableEntityAdapter<TValue>>> ListTableEntities(
            Expression<Func<TKey, bool>> predicate = null)
        {
            var query = GetTableQuery(predicate);

            var table = GetTable();
            var continuationToken = default(TableContinuationToken);
            var results = new List<TableEntityAdapter<TValue>>();

            do
            {
                var tableQueryResult = await table.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);

                continuationToken = tableQueryResult.ContinuationToken;

                results.AddRange(tableQueryResult.Results);
            } 
            while(continuationToken != null);

            return results;
        }

        TableQuery<TableEntityAdapter<TValue>> GetTableQuery(
            Expression<Func<TKey, bool>> predicate = null) 
        {
            var keyStringPrefix = ResolveKeyStringPrefix(predicate);

            var splitKeyStringPrefix = keyStringPrefix.Split(KeyMapDelimiter);

            var partialPartitionKey = splitKeyStringPrefix[0];
            var partialRowKey = splitKeyStringPrefix.Length > 1 ? splitKeyStringPrefix[1] : string.Empty;

            var filterCondition = string.Empty;
            if (!string.IsNullOrWhiteSpace(partialRowKey)) 
            {
                filterCondition = TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partialPartitionKey),
                    TableOperators.And,
                    GetStartsWithFilter("RowKey", partialRowKey));
            }
            else if (!string.IsNullOrWhiteSpace(partialPartitionKey)) 
            {
                filterCondition = GetStartsWithFilter("PartitionKey", partialPartitionKey);
            }

            return new TableQuery<TableEntityAdapter<TValue>>().Where(filterCondition);
        }

        static string GetStartsWithFilter(string columnName, string prefix)
        {
            var prefixLength = prefix.Length;
            var nextChar = (char)(prefix[prefixLength - 1] + 1);
            var incrementedPrefix = prefix.Substring(0, prefixLength - 1) + nextChar;

            return TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition(columnName, QueryComparisons.GreaterThanOrEqual, prefix),
                TableOperators.And,
                TableQuery.GenerateFilterCondition(columnName, QueryComparisons.LessThan, incrementedPrefix));
        }

        async Task<bool> ExecuteTableOperationAsync(TableOperation operation, HttpStatusCode expectedStatusCode) 
        {
            var result = await GetTable().ExecuteAsync(operation).ConfigureAwait(false);

            return result?.HttpStatusCode == (int)expectedStatusCode;
        }

        //TODO: This method (and a couple below it) were lifted from FileStoreDataStore. Consolidate this code for reuse.
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

            return EvaluateKeyMap(memberValues, true);
        }

        string EvaluateKeyMap(
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
                    $"Key map for {typeof(TValue).Name} could not be evaluated " +
                    $"because key {typeof(TKey).Name} was missing a value for {ex.ParamName}.");
            }
        }

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

        public Task<bool> GetToStream(TKey key, Stream stream)
        {
            throw new NotImplementedException();
        }
    }
}
