using Elasticsearch.Net;
using FluentHelper.ElasticSearch.Interfaces;
using Nest;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("FluentHelper.ElasticSearch.Tests")]
namespace FluentHelper.ElasticSearch.Common
{
    internal sealed class ElasticWrapper : IElasticWrapper
    {
        private IElasticClient? _client;
        private readonly IElasticConfig _elasticConfig;
        private readonly Dictionary<Type, IElasticMap> _entityMappingList;

        public ElasticWrapper(IElasticConfig elasticConfig, IEnumerable<IElasticMap> mappings)
        {
            _client = null;
            _elasticConfig = elasticConfig;

            _entityMappingList = new Dictionary<Type, IElasticMap>();
            foreach (var elasticMap in mappings)
            {
                elasticMap.Map();
                elasticMap.Verify();

                _entityMappingList.Add(elasticMap.GetMapType(), elasticMap);
            }
        }

        internal void CreateDbContext()
        {
            _client = null;

            var esSettings = new ConnectionSettings(new Uri(_elasticConfig.ConnectionUrl!)).DisableDirectStreaming(_elasticConfig.DebugQuery);

            if (_elasticConfig.EnableApiVersioningHeader)
                esSettings.EnableApiVersioningHeader();

            if (!string.IsNullOrWhiteSpace(_elasticConfig.CertificateFingerprint))
                esSettings.CertificateFingerprint(_elasticConfig.CertificateFingerprint);

            if (_elasticConfig.BasicAuthentication != null)
                esSettings.BasicAuthentication(_elasticConfig.BasicAuthentication.Value.Username, _elasticConfig.BasicAuthentication.Value.Password);

            if (_elasticConfig.RequestTimeout.HasValue)
                esSettings.RequestTimeout(_elasticConfig.RequestTimeout.Value);

            SetMappings(esSettings);
            _client = new ElasticClient(esSettings);

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Trace, null, "ElasticClient created", Array.Empty<string?>());
        }

        internal void SetMappings(ConnectionSettings esSettings)
        {
            foreach (var m in _entityMappingList)
                m.Value.ApplySpecialMap(esSettings);
        }

        public IElasticClient GetContext()
        {
            if (_client == null)
                CreateDbContext();

            return _client!;
        }

        public IElasticClient CreateNewContext()
        {
            Dispose();
            return GetContext();
        }

        public void Add<TEntity>(TEntity inputData) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);

            var addResponse = GetContext().Index(inputData, x => x.Index(indexName));
            AfterQueryResponse(addResponse);

            if (addResponse == null || !addResponse.IsValid || (addResponse.Result != Result.Created && addResponse.Result != Result.Updated))
                throw new InvalidOperationException("Could not add data", new Exception(JsonConvert.SerializeObject(addResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "Added {inputData} to {indexName}", new string?[] { inputData?.ToString(), indexName });
        }

        public async Task AddAsync<TEntity>(TEntity inputData) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);

            var addResponse = await GetContext().IndexAsync(inputData, x => x.Index(indexName)).ConfigureAwait(false);
            AfterQueryResponse(addResponse);

            if (addResponse == null || !addResponse.IsValid || (addResponse.Result != Result.Created && addResponse.Result != Result.Updated))
                throw new InvalidOperationException("Could not add data", new Exception(JsonConvert.SerializeObject(addResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "Added {inputData} to {indexName}", new string?[] { inputData?.ToString(), indexName });
        }

        public int BulkAdd<TEntity>(IEnumerable<TEntity> inputList) where TEntity : class
        {
            if (inputList == null || !inputList.Any())
                return 0;

            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];

            var groupedInputList = inputList.GroupBy(inputData => GetIndexName(mapInstance, inputData)).Select(x => new
            {
                IndexName = x.Key,
                InputList = x.ToList()
            });

            int totalIndexedElements = 0;
            foreach (var groupedInputData in groupedInputList)
            {
                int indexedElements = 0;

                try
                {
                    while (indexedElements < groupedInputData.InputList.Count)
                    {
                        var inputListToAdd = groupedInputData.InputList.Skip(indexedElements).Take(_elasticConfig.BulkInsertChunkSize).ToList();
                        _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, null, "Indexing {addNumber} elements into {indexName}. {bulkProgress}", new string?[] { inputListToAdd.Count.ToString(), groupedInputData.IndexName, $"{indexedElements}/{groupedInputData.InputList}" });

                        var bulkResponse = GetContext().Bulk(b => b.Index(groupedInputData.IndexName).IndexMany(inputListToAdd));
                        AfterQueryResponse(bulkResponse);

                        if (bulkResponse == null || !bulkResponse.IsValid || bulkResponse.Errors)
                            throw new InvalidOperationException("Could not bulkadd data", new Exception(JsonConvert.SerializeObject(bulkResponse)));

                        _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, null, "Added {addNumber} to {indexName}", new string?[] { inputListToAdd.Count.ToString(), groupedInputData.IndexName });
                        indexedElements += inputListToAdd.Count();
                        totalIndexedElements += indexedElements;
                    }
                }
                catch (Exception ex)
                {
                    _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Error, ex, "Could not BulkAdd some data on index {indexName}", new string?[] { groupedInputData.IndexName });
                }
                finally
                {
                    _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "BulkAdded {bulkProgress} to {indexName}", new string?[] { $"{indexedElements}/{groupedInputData.InputList.Count}", groupedInputData.IndexName });
                }
            }

            return totalIndexedElements;
        }

        public async Task<int> BulkAddAsync<TEntity>(IEnumerable<TEntity> inputList) where TEntity : class
        {
            if (inputList == null || !inputList.Any())
                return 0;

            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];

            var groupedInputList = inputList.GroupBy(inputData => GetIndexName(mapInstance, inputData)).Select(x => new
            {
                IndexName = x.Key,
                InputList = x.ToList()
            });

            int totalIndexedElements = 0;
            foreach (var groupedInputData in groupedInputList)
            {
                int indexedElements = 0;

                try
                {
                    while (indexedElements < groupedInputData.InputList.Count)
                    {
                        var inputListToAdd = groupedInputData.InputList.Skip(indexedElements).Take(_elasticConfig.BulkInsertChunkSize).ToList();
                        _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, null, "Indexing {addNumber} elements into {indexName}. {bulkProgress}", new string?[] { inputListToAdd.Count.ToString(), groupedInputData.IndexName, $"{indexedElements}/{groupedInputData.InputList}" });

                        var bulkResponse = await GetContext().BulkAsync(b => b.Index(groupedInputData.IndexName).IndexMany(inputListToAdd)).ConfigureAwait(false);
                        AfterQueryResponse(bulkResponse);

                        if (bulkResponse == null || !bulkResponse.IsValid || bulkResponse.Errors)
                            throw new InvalidOperationException("Could not bulkadd data", new Exception(JsonConvert.SerializeObject(bulkResponse)));

                        _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, null, "Added {addNumber} to {indexName}", new string?[] { inputListToAdd.Count.ToString(), groupedInputData.IndexName });
                        indexedElements += inputListToAdd.Count();
                        totalIndexedElements += indexedElements;
                    }
                }
                catch (Exception ex)
                {
                    _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Error, ex, "Could not BulkAdd some data on index {indexName}", new string?[] { groupedInputData.IndexName });
                }
                finally
                {
                    _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "BulkAdded {bulkProgress} to {indexName}", new string?[] { $"{indexedElements}/{groupedInputData.InputList.Count}", groupedInputData.IndexName });
                }
            }

            return totalIndexedElements;
        }

        public void AddOrUpdate<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts = 0) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);

            IElasticFieldUpdater<TEntity>? elasticFieldUpdater = fieldUpdaterExpr != null ? fieldUpdaterExpr(new ElasticFieldUpdater<TEntity>(mapInstance.IdPropertyName)) : null;
            var updateObj = GetExpandoObject(inputData, elasticFieldUpdater);

            var inputId = GetFieldValue(inputData, mapInstance.IdPropertyName);
            var updateResponse = GetContext().Update<TEntity, ExpandoObject>(inputId!.ToString(), x => x.Index(indexName).Doc(updateObj).Upsert(inputData).Refresh(Refresh.True).RetryOnConflict(retryOnConflicts));
            AfterQueryResponse(updateResponse);

            if (updateResponse == null || !updateResponse.IsValid || (updateResponse.Result != Result.Created && updateResponse.Result != Result.Updated && updateResponse.Result != Result.Noop))
                throw new InvalidOperationException("Could not update data", new Exception(JsonConvert.SerializeObject(updateResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "AddedOrUpdated {inputData} to {indexName}", new string?[] { inputData?.ToString(), indexName });
        }

        public async Task AddOrUpdateAsync<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts = 0) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);

            IElasticFieldUpdater<TEntity>? elasticFieldUpdater = fieldUpdaterExpr != null ? fieldUpdaterExpr(new ElasticFieldUpdater<TEntity>(mapInstance.IdPropertyName)) : null;
            var updateObj = GetExpandoObject(inputData, elasticFieldUpdater);

            var inputId = GetFieldValue(inputData, mapInstance.IdPropertyName);
            var updateResponse = await GetContext().UpdateAsync<TEntity, ExpandoObject>(inputId!.ToString(), x => x.Index(indexName).Doc(updateObj).Upsert(inputData).Refresh(Refresh.True).RetryOnConflict(retryOnConflicts)).ConfigureAwait(false); ;
            AfterQueryResponse(updateResponse);

            if (updateResponse == null || !updateResponse.IsValid || (updateResponse.Result != Result.Created && updateResponse.Result != Result.Updated && updateResponse.Result != Result.Noop))
                throw new InvalidOperationException("Could not update data", new Exception(JsonConvert.SerializeObject(updateResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "AddedOrUpdated {inputData} to {indexName}", new string?[] { inputData?.ToString(), indexName });
        }

        public IEnumerable<TEntity> Query<TEntity>(object? baseObjectFilter, ElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];

            string indexNames = GetIndexNamesForQueries(mapInstance, baseObjectFilter);

            var searchDescriptor = new SearchDescriptor<TEntity>();
            searchDescriptor.Index(indexNames);
            searchDescriptor.IgnoreUnavailable();

            if (queryParameters != null)
            {
                searchDescriptor.Query(q => queryParameters.Query);

                if (queryParameters.SourceFilter != null)
                    searchDescriptor.Source(s => queryParameters.SourceFilter!);

                if (queryParameters.Sort != null)
                    searchDescriptor.Sort(s => queryParameters.Sort!);

                searchDescriptor.Skip(queryParameters.Skip).Take(queryParameters.Take);
            }

            var queryResponse = GetContext().Search<TEntity>(s => searchDescriptor);
            AfterQueryResponse(queryResponse);

            if (queryResponse == null || !queryResponse.IsValid)
                throw new InvalidOperationException("Could not get data", new Exception(JsonConvert.SerializeObject(queryResponse)));

            return queryResponse.Documents.AsEnumerable();
        }

        public async Task<IEnumerable<TEntity>> QueryAsync<TEntity>(object? baseObjectFilter, ElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];

            string indexNames = GetIndexNamesForQueries(mapInstance, baseObjectFilter);

            var searchDescriptor = new SearchDescriptor<TEntity>();
            searchDescriptor.Index(indexNames);
            searchDescriptor.IgnoreUnavailable();

            if (queryParameters != null)
            {
                searchDescriptor.Query(q => queryParameters.Query);

                if (queryParameters.SourceFilter != null)
                    searchDescriptor.Source(s => queryParameters.SourceFilter!);

                if (queryParameters.Sort != null)
                    searchDescriptor.Sort(s => queryParameters.Sort!);

                searchDescriptor.Skip(queryParameters.Skip).Take(queryParameters.Take);
            }

            var queryResponse = await GetContext().SearchAsync<TEntity>(s => searchDescriptor).ConfigureAwait(false);
            AfterQueryResponse(queryResponse);

            if (queryResponse == null || !queryResponse.IsValid)
                throw new InvalidOperationException("Could not get data", new Exception(JsonConvert.SerializeObject(queryResponse)));

            return queryResponse.Documents.AsEnumerable();
        }

        public long Count<TEntity>(object? baseObjectFilter, ElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexNames = GetIndexNamesForQueries(mapInstance, baseObjectFilter);

            var countDescriptor = new CountDescriptor<TEntity>();
            countDescriptor.Index(indexNames);
            countDescriptor.IgnoreUnavailable();

            if (queryParameters != null)
                countDescriptor.Query(q => queryParameters.Query);

            var countResponse = GetContext().Count<TEntity>(c => countDescriptor);
            AfterQueryResponse(countResponse);

            if (countResponse == null || !countResponse.IsValid)
                throw new InvalidOperationException("Could not count data", new Exception(JsonConvert.SerializeObject(countResponse)));

            return countResponse.Count;
        }

        public async Task<long> CountAsync<TEntity>(object? baseObjectFilter, ElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexNames = GetIndexNamesForQueries(mapInstance, baseObjectFilter);

            var countDescriptor = new CountDescriptor<TEntity>();
            countDescriptor.Index(indexNames);
            countDescriptor.IgnoreUnavailable();

            if (queryParameters != null)
                countDescriptor.Query(q => queryParameters.Query);

            var countResponse = await GetContext().CountAsync<TEntity>(c => countDescriptor).ConfigureAwait(false);
            AfterQueryResponse(countResponse);

            if (countResponse == null || !countResponse.IsValid)
                throw new InvalidOperationException("Could not count data", new Exception(JsonConvert.SerializeObject(countResponse)));

            return countResponse.Count;
        }

        public void Delete<TEntity>(TEntity inputData) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);

            var deleteResponse = GetContext().Delete<TEntity>(inputData, x => x.Index(indexName));
            AfterQueryResponse(deleteResponse);

            if (deleteResponse == null || !deleteResponse.IsValid || deleteResponse.Result != Result.Deleted)
                throw new InvalidOperationException("Could not delete data", new Exception(JsonConvert.SerializeObject(deleteResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "Deleted {inputData} from {indexName}", new string?[] { inputData?.ToString(), indexName });
        }

        public async Task DeleteAsync<TEntity>(TEntity inputData) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);

            var deleteResponse = await GetContext().DeleteAsync<TEntity>(inputData, x => x.Index(indexName)).ConfigureAwait(false);
            AfterQueryResponse(deleteResponse);

            if (deleteResponse == null || !deleteResponse.IsValid || deleteResponse.Result != Result.Deleted)
                throw new InvalidOperationException("Could not delete data", new Exception(JsonConvert.SerializeObject(deleteResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "Deleted {inputData} from {indexName}", new string?[] { inputData?.ToString(), indexName });
        }

        public string GetIndexName<TEntity>(ElasticMap<TEntity> elasticMap, TEntity inputData) where TEntity : class
        {
            string indexName = string.IsNullOrWhiteSpace(_elasticConfig.IndexPrefix) ? $"{elasticMap.BaseIndexName}" : $"{_elasticConfig.IndexPrefix}-{elasticMap.BaseIndexName}";

            if (!string.IsNullOrWhiteSpace(_elasticConfig.IndexSuffix))
                indexName += $"-{_elasticConfig.IndexSuffix}";

            string indexCalculated = elasticMap.IndexCalculator!.CalcEntityIndex(inputData);
            if (!string.IsNullOrWhiteSpace(indexCalculated))
                indexName += $"-{indexCalculated}";

            indexName = indexName.ToLower();
            VerifyIndexNames(indexName, false);
            return indexName;
        }

        public string GetIndexNamesForQueries<TEntity>(ElasticMap<TEntity> elasticMap, object? baseObjectFilter) where TEntity : class
        {
            string fixedIndexName = string.IsNullOrWhiteSpace(_elasticConfig.IndexPrefix) ? $"{elasticMap.BaseIndexName}" : $"{_elasticConfig.IndexPrefix}-{elasticMap.BaseIndexName}";

            if (!string.IsNullOrWhiteSpace(_elasticConfig.IndexSuffix))
                fixedIndexName += $"-{_elasticConfig.IndexSuffix}";

            var indexesToQuery = elasticMap.IndexCalculator!.CalcQueryIndex(baseObjectFilter).Select(x => x.ToLower());

            string indexNames = indexesToQuery.Any() ? $"{fixedIndexName}-{string.Join($",{fixedIndexName}-".ToLower(), indexesToQuery)}" : fixedIndexName;
            VerifyIndexNames(indexNames, true);
            return indexNames;
        }

        public void VerifyIndexNames(string indexNames, bool isRetrieveQuery)
        {
            string[] indexList = indexNames.Split(',');
            foreach (string indexName in indexList)
            {
                if (indexName.Length > 255)
                    throw new Exception($"Index name {indexName} exceeded maximum length of 255 characters");

                if (indexName == "." || indexName == "..")
                    throw new Exception($"Index name cannot be '.' or '..'");

                if (indexName.StartsWith("-") || indexName.StartsWith("_") || indexName.StartsWith("+"))
                    throw new Exception($"Index name {indexName} cannot start with '-' or '_' or '+'");

                if (indexName.Contains("\\") || indexName.Contains("/") || indexName.Contains("?") ||
                        indexName.Contains("\"") || indexName.Contains("<") || indexName.Contains(">") || indexName.Contains("|") ||
                        indexName.Contains(" ") || indexName.Contains(",") || indexName.Contains("#") || (indexName.Contains("*") && !isRetrieveQuery))
                    throw new Exception($"Index name {indexName} cannot contain '\', '/', '*', '?', '\"', '<', '>', '|', ' ', ',', '#'");
            }
        }

        public void AfterQueryResponse(IResponse queryResponse)
        {
            if (queryResponse == null)
                return;

            if (!queryResponse.IsValid)
            {
                _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Error, queryResponse!.OriginalException, queryResponse!.DebugInformation, Array.Empty<string?>());
                return;
            }

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, null, queryResponse!.DebugInformation, Array.Empty<string?>());
        }

        public void Dispose()
        {
            _client = null;
        }

        private object? GetFieldValue<TEntity>(TEntity inputData, string fieldName) where TEntity : class
        {
            if (inputData == null)
                return null;

            foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(inputData.GetType()))
                if (property.Name == fieldName)
                    return property.GetValue(inputData);

            return null;
        }

        private ExpandoObject GetExpandoObject<TEntity>(TEntity inputData, IElasticFieldUpdater<TEntity>? elasticFieldUpdater) where TEntity : class
        {
            if (elasticFieldUpdater == null)
                throw new Exception("ElasticFieldUpdater cannot be null");

            var expandoInstance = new ExpandoObject();

            foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(inputData.GetType()))
                if (elasticFieldUpdater.GetFieldList().Contains(property.Name))
                    expandoInstance.TryAdd($"{char.ToLower(property.Name[0]) + property.Name.Substring(1)}", property.GetValue(inputData));

            return expandoInstance!;
        }
    }
}
