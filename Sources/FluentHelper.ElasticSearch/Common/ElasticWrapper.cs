using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Elastic.Transport.Products.Elasticsearch;
using FluentHelper.ElasticSearch.Interfaces;
using FluentHelper.ElasticSearch.QueryParameters;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("FluentHelper.ElasticSearch.Tests")]
namespace FluentHelper.ElasticSearch.Common
{
    internal sealed class ElasticWrapper : IElasticWrapper
    {
        private ElasticsearchClient? _client;
        private readonly IElasticConfig _elasticConfig;
        private readonly Dictionary<Type, IElasticMap> _entityMappingList;

        public ElasticWrapper(IElasticConfig elasticConfig, IEnumerable<IElasticMap> mappings)
        {
            _client = null;
            _elasticConfig = elasticConfig;

            _entityMappingList = [];
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

            var staticNodePool = new StaticNodePool(_elasticConfig.ConnectionsPool);
            var esSettings = new ElasticsearchClientSettings(staticNodePool);

            if (_elasticConfig.EnableDebug)
                esSettings.EnableDebugMode(_elasticConfig.RequestCompleted!);

            if (!string.IsNullOrWhiteSpace(_elasticConfig.CertificateFingerprint))
                esSettings.CertificateFingerprint(_elasticConfig.CertificateFingerprint);

            if (_elasticConfig.BasicAuthentication != null)
                esSettings.Authentication(new BasicAuthentication(_elasticConfig.BasicAuthentication.Value.Username, _elasticConfig.BasicAuthentication.Value.Password));

            if (_elasticConfig.RequestTimeout.HasValue)
                esSettings.RequestTimeout(_elasticConfig.RequestTimeout.Value);

            SetMappings(esSettings);
            _client = new ElasticsearchClient(esSettings);

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Trace, null, "ElasticClient created", []);
        }

        internal void SetMappings(ElasticsearchClientSettings esSettings)
        {
            foreach (var m in _entityMappingList)
                m.Value.ApplySpecialMap(esSettings);
        }

        public ElasticsearchClient GetContext()
        {
            if (_client == null)
                CreateDbContext();

            return _client!;
        }

        public ElasticsearchClient CreateNewContext()
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

            if (addResponse == null || !addResponse.IsValidResponse || (addResponse.Result != Result.Created && addResponse.Result != Result.Updated))
                throw new InvalidOperationException("Could not add data", new Exception(JsonConvert.SerializeObject(addResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "Added {inputData} to {indexName}", [inputData?.ToString(), indexName]);
        }

        public async Task AddAsync<TEntity>(TEntity inputData) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);

            var addResponse = await GetContext().IndexAsync(inputData, x => x.Index(indexName)).ConfigureAwait(false);
            AfterQueryResponse(addResponse);

            if (addResponse == null || !addResponse.IsValidResponse || (addResponse.Result != Result.Created && addResponse.Result != Result.Updated))
                throw new InvalidOperationException("Could not add data", new Exception(JsonConvert.SerializeObject(addResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "Added {inputData} to {indexName}", [inputData?.ToString(), indexName]);
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
                        _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, null, "Indexing {addNumber} elements into {indexName}. {bulkProgress}", [inputListToAdd.Count.ToString(), groupedInputData.IndexName, $"{indexedElements}/{groupedInputData.InputList}"]);

                        var bulkResponse = GetContext().Bulk(b => b.Index(groupedInputData.IndexName).IndexMany(inputListToAdd));
                        AfterQueryResponse(bulkResponse);

                        if (bulkResponse == null || !bulkResponse.IsValidResponse || bulkResponse.Errors)
                            throw new InvalidOperationException("Could not bulkadd data", new Exception(JsonConvert.SerializeObject(bulkResponse)));

                        _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, null, "Added {addNumber} to {indexName}", [inputListToAdd.Count.ToString(), groupedInputData.IndexName]);
                        indexedElements += inputListToAdd.Count;
                        totalIndexedElements += indexedElements;
                    }
                }
                catch (Exception ex)
                {
                    _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Error, ex, "Could not BulkAdd some data on index {indexName}", [groupedInputData.IndexName]);
                }
                finally
                {
                    _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "BulkAdded {bulkProgress} to {indexName}", [$"{indexedElements}/{groupedInputData.InputList.Count}", groupedInputData.IndexName]);
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
                        _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, null, "Indexing {addNumber} elements into {indexName}. {bulkProgress}", [inputListToAdd.Count.ToString(), groupedInputData.IndexName, $"{indexedElements}/{groupedInputData.InputList}"]);

                        var bulkResponse = await GetContext().BulkAsync(b => b.Index(groupedInputData.IndexName).IndexMany(inputListToAdd)).ConfigureAwait(false);
                        AfterQueryResponse(bulkResponse);

                        if (bulkResponse == null || !bulkResponse.IsValidResponse || bulkResponse.Errors)
                            throw new InvalidOperationException("Could not bulkadd data", new Exception(JsonConvert.SerializeObject(bulkResponse)));

                        _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, null, "Added {addNumber} to {indexName}", [inputListToAdd.Count.ToString(), groupedInputData.IndexName]);
                        indexedElements += inputListToAdd.Count;
                        totalIndexedElements += indexedElements;
                    }
                }
                catch (Exception ex)
                {
                    _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Error, ex, "Could not BulkAdd some data on index {indexName}", [groupedInputData.IndexName]);
                }
                finally
                {
                    _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "BulkAdded {bulkProgress} to {indexName}", [$"{indexedElements}/{groupedInputData.InputList.Count}", groupedInputData.IndexName]);
                }
            }

            return totalIndexedElements;
        }

        public void AddOrUpdate<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts = 0) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);

            IElasticFieldUpdater<TEntity>? elasticFieldUpdater = fieldUpdaterExpr != null ? fieldUpdaterExpr(new ElasticFieldUpdater<TEntity>(mapInstance.IdPropertyName)) : null;
            var updateObj = inputData.GetExpandoObject(elasticFieldUpdater);

            var inputId = inputData.GetFieldValue(mapInstance.IdPropertyName);
            var updateResponse = GetContext().Update<TEntity, ExpandoObject>(indexName, inputId!.ToString()!, x => x.Doc(updateObj).Upsert(inputData).Refresh(Refresh.True).RetryOnConflict(retryOnConflicts));
            AfterQueryResponse(updateResponse);

            if (updateResponse == null || !updateResponse.IsValidResponse || (updateResponse.Result != Result.Created && updateResponse.Result != Result.Updated && updateResponse.Result != Result.NoOp))
                throw new InvalidOperationException("Could not update data", new Exception(JsonConvert.SerializeObject(updateResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "AddedOrUpdated {inputData} to {indexName}", [inputData?.ToString(), indexName]);
        }

        public async Task AddOrUpdateAsync<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts = 0) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);

            IElasticFieldUpdater<TEntity>? elasticFieldUpdater = fieldUpdaterExpr != null ? fieldUpdaterExpr(new ElasticFieldUpdater<TEntity>(mapInstance.IdPropertyName)) : null;
            var updateObj = inputData.GetExpandoObject(elasticFieldUpdater);

            var inputId = inputData.GetFieldValue(mapInstance.IdPropertyName);
            var updateResponse = await GetContext().UpdateAsync<TEntity, ExpandoObject>(indexName, inputId!.ToString()!, x => x.Doc(updateObj).Upsert(inputData).Refresh(Refresh.True).RetryOnConflict(retryOnConflicts)).ConfigureAwait(false);
            AfterQueryResponse(updateResponse);

            if (updateResponse == null || !updateResponse.IsValidResponse || (updateResponse.Result != Result.Created && updateResponse.Result != Result.Updated && updateResponse.Result != Result.NoOp))
                throw new InvalidOperationException("Could not update data", new Exception(JsonConvert.SerializeObject(updateResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "AddedOrUpdated {inputData} to {indexName}", [inputData?.ToString(), indexName]);
        }

        public IEnumerable<TEntity> Query<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];

            string indexNames = GetIndexNamesForQueries(mapInstance, baseObjectFilter);

            var searchDescriptor = new SearchRequestDescriptor<TEntity>();
            searchDescriptor.Index(indexNames);
            searchDescriptor.IgnoreUnavailable();

            if (queryParameters != null)
            {
                if (queryParameters.QueryDescriptor != null)
                    searchDescriptor.Query(queryParameters.QueryDescriptor);

                if (queryParameters.SourceConfig != null)
                    searchDescriptor.Source(queryParameters.SourceConfig);

                if (queryParameters.SortOptionsDescriptor != null)
                    searchDescriptor.Sort(queryParameters.SortOptionsDescriptor);

                searchDescriptor.From(queryParameters.Skip).Size(queryParameters.Take);
            }

            var queryResponse = GetContext().Search(searchDescriptor);
            AfterQueryResponse(queryResponse);

            if (queryResponse == null || !queryResponse.IsValidResponse)
                throw new InvalidOperationException("Could not get data", new Exception(JsonConvert.SerializeObject(queryResponse)));

            return queryResponse.Documents.AsEnumerable();
        }

        public async Task<IEnumerable<TEntity>> QueryAsync<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];

            string indexNames = GetIndexNamesForQueries(mapInstance, baseObjectFilter);

            var searchDescriptor = new SearchRequestDescriptor<TEntity>();
            searchDescriptor.Index(indexNames);
            searchDescriptor.IgnoreUnavailable();

            if (queryParameters != null)
            {
                if (queryParameters.QueryDescriptor != null)
                    searchDescriptor.Query(queryParameters.QueryDescriptor);

                if (queryParameters.SourceConfig != null)
                    searchDescriptor.Source(queryParameters.SourceConfig);

                if (queryParameters.SortOptionsDescriptor != null)
                    searchDescriptor.Sort(queryParameters.SortOptionsDescriptor);

                searchDescriptor.From(queryParameters.Skip).Size(queryParameters.Take);
            }

            var queryResponse = await GetContext().SearchAsync(searchDescriptor).ConfigureAwait(false);
            AfterQueryResponse(queryResponse);

            if (queryResponse == null || !queryResponse.IsValidResponse)
                throw new InvalidOperationException("Could not get data", new Exception(JsonConvert.SerializeObject(queryResponse)));

            return queryResponse.Documents.AsEnumerable();
        }

        public long Count<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexNames = GetIndexNamesForQueries(mapInstance, baseObjectFilter);

            var countDescriptor = new CountRequestDescriptor<TEntity>();
            countDescriptor.Indices(indexNames);
            countDescriptor.IgnoreUnavailable();

            if (queryParameters != null && queryParameters.QueryDescriptor != null)
                countDescriptor.Query(queryParameters.QueryDescriptor);

            var countResponse = GetContext().Count(countDescriptor);
            AfterQueryResponse(countResponse);

            if (countResponse == null || !countResponse.IsValidResponse)
                throw new InvalidOperationException("Could not count data", new Exception(JsonConvert.SerializeObject(countResponse)));

            return countResponse.Count;
        }

        public async Task<long> CountAsync<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexNames = GetIndexNamesForQueries(mapInstance, baseObjectFilter);

            var countDescriptor = new CountRequestDescriptor<TEntity>();
            countDescriptor.Indices(indexNames);
            countDescriptor.IgnoreUnavailable();

            if (queryParameters != null && queryParameters.QueryDescriptor != null)
                countDescriptor.Query(queryParameters.QueryDescriptor);

            var countResponse = await GetContext().CountAsync(countDescriptor).ConfigureAwait(false);
            AfterQueryResponse(countResponse);

            if (countResponse == null || !countResponse.IsValidResponse)
                throw new InvalidOperationException("Could not count data", new Exception(JsonConvert.SerializeObject(countResponse)));

            return countResponse.Count;
        }

        public void Delete<TEntity>(TEntity inputData) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);
            var inputId = inputData.GetFieldValue(mapInstance.IdPropertyName);

            var deleteResponse = GetContext().Delete<TEntity>(indexName, inputId!.ToString()!, r => r.Refresh(Refresh.True));
            AfterQueryResponse(deleteResponse);

            if (deleteResponse == null || !deleteResponse.IsValidResponse || deleteResponse.Result != Result.Deleted)
                throw new InvalidOperationException("Could not delete data", new Exception(JsonConvert.SerializeObject(deleteResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "Deleted {inputData} from {indexName}", [inputData?.ToString(), indexName]);
        }

        public async Task DeleteAsync<TEntity>(TEntity inputData) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);
            var inputId = inputData.GetFieldValue(mapInstance.IdPropertyName);

            var deleteResponse = await GetContext().DeleteAsync<TEntity>(indexName, inputId!.ToString()!, r => r.Refresh(Refresh.True)).ConfigureAwait(false);
            AfterQueryResponse(deleteResponse);

            if (deleteResponse == null || !deleteResponse.IsValidResponse || deleteResponse.Result != Result.Deleted)
                throw new InvalidOperationException("Could not delete data", new Exception(JsonConvert.SerializeObject(deleteResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "Deleted {inputData} from {indexName}", [inputData?.ToString(), indexName]);
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
            indexName.ThrowIfIndexInvalid(false);
            return indexName;
        }

        public string GetIndexNamesForQueries<TEntity>(ElasticMap<TEntity> elasticMap, object? baseObjectFilter) where TEntity : class
        {
            string fixedIndexName = string.IsNullOrWhiteSpace(_elasticConfig.IndexPrefix) ? $"{elasticMap.BaseIndexName}" : $"{_elasticConfig.IndexPrefix}-{elasticMap.BaseIndexName}";

            if (!string.IsNullOrWhiteSpace(_elasticConfig.IndexSuffix))
                fixedIndexName += $"-{_elasticConfig.IndexSuffix}";

            var indexesToQuery = elasticMap.IndexCalculator!.CalcQueryIndex(baseObjectFilter).Select(x => x.ToLower());

            string indexNames = indexesToQuery.Any() ? $"{fixedIndexName}-{string.Join($",{fixedIndexName}-".ToLower(), indexesToQuery)}" : fixedIndexName;
            indexNames.ThrowIfIndexInvalid(false);
            return indexNames;
        }



        public void AfterQueryResponse(ElasticsearchResponse queryResponse)
        {
            if (queryResponse == null)
                return;

            if (!queryResponse.IsValidResponse)
            {
                if (!queryResponse.TryGetOriginalException(out var originalException))
                    originalException = null;

                _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Error, originalException, queryResponse!.DebugInformation, []);
                return;
            }

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, null, queryResponse!.DebugInformation, []);
        }

        public void Dispose()
        {
            _client = null;
        }


    }
}
