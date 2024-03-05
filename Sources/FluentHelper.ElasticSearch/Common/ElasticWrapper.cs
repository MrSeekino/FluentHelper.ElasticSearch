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
using System.Threading;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("FluentHelper.ElasticSearch.Tests")]
namespace FluentHelper.ElasticSearch.Common
{
    internal sealed class ElasticWrapper : IElasticWrapper
    {
        private ElasticsearchClient? _client;
        private readonly IElasticConfig _elasticConfig;
        private readonly Dictionary<Type, IElasticMap> _entityMappingList;

        public int MappingLength => _entityMappingList.Count;

        public ElasticWrapper(IElasticConfig elasticConfig, IEnumerable<IElasticMap> mappings)
            : this(null, elasticConfig, mappings) { }

        public ElasticWrapper(ElasticsearchClient? client, IElasticConfig elasticConfig, IEnumerable<IElasticMap> mappings)
        {
            _client = client;
            _elasticConfig = elasticConfig;

            _entityMappingList = [];
            foreach (var elasticMap in mappings)
            {
                elasticMap.Map();
                elasticMap.Verify();

                _entityMappingList.Add(elasticMap.GetMappingType(), elasticMap);
            }
        }

        internal void CreateClient()
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

            foreach (var m in _entityMappingList)
                m.Value.ApplyMapping(esSettings);

            _client = new ElasticsearchClient(esSettings);

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Trace, null, "ElasticClient created", []);
        }

        public ElasticsearchClient GetOrCreateClient()
        {
            if (_client == null)
                CreateClient();

            return _client!;
        }

        public void Add<TEntity>(TEntity inputData) where TEntity : class
        {
            var addResponse = GetOrCreateClient().Index(inputData, GetIndexName(inputData, out _));
            CheckAddResponse(inputData, addResponse);
        }

        public async Task AddAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class
        {
            var addResponse = await GetOrCreateClient().IndexAsync(inputData, GetIndexName(inputData, out _), cancellationToken);
            CheckAddResponse(inputData, addResponse);
        }

        private void CheckAddResponse<TEntity>(TEntity inputData, IndexResponse addResponse) where TEntity : class
        {
            AfterQueryResponse(addResponse);

            if (!addResponse.IsValidResponse || (addResponse.Result != Result.Created && addResponse.Result != Result.Updated))
                throw new InvalidOperationException("Could not add data", new Exception(JsonConvert.SerializeObject(addResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "Added {inputData} to {indexName}", [inputData.ToString(), addResponse.Index]);
        }

        public int BulkAdd<TEntity>(IEnumerable<TEntity> inputList) where TEntity : class
        {
            var groupedInputList = PrepareBulkData(inputList);

            int totalIndexedElements = 0;
            foreach (var groupedInputData in groupedInputList)
            {
                var bulkResponse = GetOrCreateClient().Bulk(b => b.Index(groupedInputData.IndexName).IndexMany(groupedInputData.Items));
                int addedItems = CheckBulkAddResponse(bulkResponse, groupedInputData.Items.Count, groupedInputData.IndexName);
                totalIndexedElements += addedItems;
            }

            return totalIndexedElements;
        }

        public async Task<int> BulkAddAsync<TEntity>(IEnumerable<TEntity> inputList, CancellationToken cancellationToken = default) where TEntity : class
        {
            var groupedInputList = PrepareBulkData(inputList);

            int totalIndexedElements = 0;
            foreach (var groupedInputData in groupedInputList)
            {
                var bulkResponse = await GetOrCreateClient().BulkAsync(b => b.Index(groupedInputData.IndexName).IndexMany(groupedInputData.Items), cancellationToken).ConfigureAwait(false);
                int addedItems = CheckBulkAddResponse(bulkResponse, groupedInputData.Items.Count, groupedInputData.IndexName);
                totalIndexedElements += addedItems;
            }

            return totalIndexedElements;
        }

        private List<(string IndexName, List<TEntity> Items)> PrepareBulkData<TEntity>(IEnumerable<TEntity> inputList) where TEntity : class
        {
            if (!inputList.Any())
                return [];

            List<(string indexName, List<TEntity> Items)> preparedBulk = [];

            var groupedInput = inputList.GroupBy(inputData => GetIndexName(inputData, out _)).Select(x =>
            {
                (string IndexName, List<TEntity> Items) tuple = new(x.Key, x.ToList());
                return tuple;
            });

            foreach (var itemToSplit in groupedInput)
            {
                if (itemToSplit.Items.Count <= _elasticConfig.BulkInsertChunkSize)
                {
                    preparedBulk.Add(itemToSplit);
                    continue;
                }

                for (var i = 0; i < itemToSplit.Items.Count; i += _elasticConfig.BulkInsertChunkSize)
                {
                    var splittedItems = itemToSplit.Items.GetRange(i, Math.Min(_elasticConfig.BulkInsertChunkSize, itemToSplit.Items.Count - i));

                    (string indexName, List<TEntity>) splittedTuple = new(itemToSplit.IndexName, splittedItems);
                    preparedBulk.Add(splittedTuple);
                }
            }

            return preparedBulk;
        }

        private int CheckBulkAddResponse(BulkResponse bulkResponse, int addNumber, string indexName)
        {
            AfterQueryResponse(bulkResponse);

            if (!bulkResponse.IsValidResponse || bulkResponse.Errors)
            {
                bulkResponse.TryGetOriginalException(out Exception? exception);
                _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Error, exception, "Could not BulkAdd some data on index {indexName}", [indexName]);
                return 0;
            }

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, null, "Added {addNumber} to {indexName}", [addNumber, indexName]);
            return addNumber;
        }

        public void AddOrUpdate<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts = 0) where TEntity : class
        {
            var addOrUpdateParameters = GetAddOrUpdatedParameters(inputData, fieldUpdaterExpr, retryOnConflicts);
            var updateResponse = GetOrCreateClient().Update(addOrUpdateParameters.IndexName, addOrUpdateParameters.Id, addOrUpdateParameters.ConfigureRequest);
            CheckUpdateResponse(inputData, updateResponse);
        }

        public async Task AddOrUpdateAsync<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, CancellationToken cancellationToken = default) where TEntity : class
            => await AddOrUpdateAsync(inputData, fieldUpdaterExpr, 0, cancellationToken);

        public async Task AddOrUpdateAsync<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts, CancellationToken cancellationToken = default) where TEntity : class
        {
            var addOrUpdateParameters = GetAddOrUpdatedParameters(inputData, fieldUpdaterExpr, retryOnConflicts);
            var updateResponse = await GetOrCreateClient().UpdateAsync(addOrUpdateParameters.IndexName, addOrUpdateParameters.Id, addOrUpdateParameters.ConfigureRequest, cancellationToken).ConfigureAwait(false);
            CheckUpdateResponse(inputData, updateResponse);
        }

        private (string IndexName, Id Id, Action<UpdateRequestDescriptor<TEntity, ExpandoObject>> ConfigureRequest) GetAddOrUpdatedParameters<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts) where TEntity : class
        {
            string indexName = GetIndexName(inputData, out ElasticMap<TEntity> mapInstance);

            var inputId = inputData.GetFieldValue(mapInstance.IdPropertyName);

            var elasticFieldUpdater = fieldUpdaterExpr(new ElasticFieldUpdater<TEntity>(mapInstance.IdPropertyName));
            var updateObj = inputData.GetExpandoObject(elasticFieldUpdater);
            Action<UpdateRequestDescriptor<TEntity, ExpandoObject>> configureRequest = x => x.Doc(updateObj).Upsert(inputData).Refresh(Refresh.True).RetryOnConflict(retryOnConflicts);

            return new(indexName, inputId!.ToString()!, configureRequest);
        }

        private void CheckUpdateResponse<TEntity>(TEntity inputData, UpdateResponse<TEntity> updateResponse) where TEntity : class
        {
            AfterQueryResponse(updateResponse);

            if (!updateResponse.IsValidResponse || (updateResponse.Result != Result.Created && updateResponse.Result != Result.Updated && updateResponse.Result != Result.NoOp))
                throw new InvalidOperationException("Could not update data", new Exception(JsonConvert.SerializeObject(updateResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "AddedOrUpdated {inputData} to {indexName}", [inputData.ToString(), updateResponse.Index]);
        }

        public IEnumerable<TEntity> Query<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            var searchDescriptor = GetSearchRequestDescriptor(baseObjectFilter, queryParameters);
            var queryResponse = GetOrCreateClient().Search(searchDescriptor);
            return CheckSearchResponse(queryResponse);
        }

        public async Task<IEnumerable<TEntity>> QueryAsync<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters, CancellationToken cancellationToken = default) where TEntity : class
        {
            var searchDescriptor = GetSearchRequestDescriptor(baseObjectFilter, queryParameters);
            var queryResponse = await GetOrCreateClient().SearchAsync(searchDescriptor, cancellationToken).ConfigureAwait(false);
            return CheckSearchResponse(queryResponse);
        }

        private SearchRequestDescriptor<TEntity> GetSearchRequestDescriptor<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            string indexNames = GetIndexNamesForQueries<TEntity>(baseObjectFilter);

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

            return searchDescriptor;
        }

        private IReadOnlyCollection<TEntity> CheckSearchResponse<TEntity>(SearchResponse<TEntity> queryResponse)
        {
            AfterQueryResponse(queryResponse);

            if (queryResponse == null || !queryResponse.IsValidResponse)
                throw new InvalidOperationException("Could not get data", new Exception(JsonConvert.SerializeObject(queryResponse)));

            return queryResponse.Documents;
        }

        public long Count<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            var countDescriptor = GetCountRequestDescriptor(baseObjectFilter, queryParameters);
            var countResponse = GetOrCreateClient().Count(countDescriptor);
            return CheckCountResponse(countResponse);
        }

        public async Task<long> CountAsync<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters, CancellationToken cancellationToken = default) where TEntity : class
        {
            var countDescriptor = GetCountRequestDescriptor(baseObjectFilter, queryParameters);
            var countResponse = await GetOrCreateClient().CountAsync(countDescriptor, cancellationToken).ConfigureAwait(false);
            return CheckCountResponse(countResponse);
        }

        private CountRequestDescriptor<TEntity> GetCountRequestDescriptor<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            string indexNames = GetIndexNamesForQueries<TEntity>(baseObjectFilter);

            var countDescriptor = new CountRequestDescriptor<TEntity>();
            countDescriptor.Indices(indexNames);
            countDescriptor.IgnoreUnavailable();

            if (queryParameters != null && queryParameters.QueryDescriptor != null)
                countDescriptor.Query(queryParameters.QueryDescriptor);

            return countDescriptor;
        }

        private long CheckCountResponse(CountResponse countResponse)
        {
            AfterQueryResponse(countResponse);

            if (!countResponse.IsValidResponse)
                throw new InvalidOperationException("Could not count data", new Exception(JsonConvert.SerializeObject(countResponse)));

            return countResponse.Count;
        }

        public void Delete<TEntity>(TEntity inputData) where TEntity : class
        {
            var deleteParameters = GetDeleteParameters(inputData);
            var deleteResponse = GetOrCreateClient().Delete<TEntity>(deleteParameters.IndexName, deleteParameters.Id, r => r.Refresh(Refresh.True));
            CheckDeleteResponse(inputData, deleteResponse);
        }

        public async Task DeleteAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class
        {
            var deleteParameters = GetDeleteParameters(inputData);
            var deleteResponse = await GetOrCreateClient().DeleteAsync<TEntity>(deleteParameters.IndexName, deleteParameters.Id, r => r.Refresh(Refresh.True), cancellationToken).ConfigureAwait(false);
            CheckDeleteResponse(inputData, deleteResponse);
        }

        private (string IndexName, Id Id) GetDeleteParameters<TEntity>(TEntity inputData) where TEntity : class
        {
            string indexName = GetIndexName(inputData, out ElasticMap<TEntity> mapInstance);
            var inputId = inputData.GetFieldValue(mapInstance.IdPropertyName);

            return new(indexName, inputId!.ToString()!);
        }

        private void CheckDeleteResponse<TEntity>(TEntity inputData, DeleteResponse deleteResponse) where TEntity : class
        {
            AfterQueryResponse(deleteResponse);

            if (!deleteResponse.IsValidResponse || deleteResponse.Result != Result.Deleted)
                throw new InvalidOperationException("Could not delete data", new Exception(JsonConvert.SerializeObject(deleteResponse)));

            _elasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, null, "Deleted {inputData} from {indexName}", [inputData.ToString(), deleteResponse.Index]);
        }

        public string GetIndexName<TEntity>(TEntity inputData, out ElasticMap<TEntity> mapInstance) where TEntity : class
        {
            mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string indexName = string.IsNullOrWhiteSpace(_elasticConfig.IndexPrefix) ? $"{mapInstance.BaseIndexName}" : $"{_elasticConfig.IndexPrefix}-{mapInstance.BaseIndexName}";

            if (!string.IsNullOrWhiteSpace(_elasticConfig.IndexSuffix))
                indexName += $"-{_elasticConfig.IndexSuffix}";

            string indexCalculated = mapInstance.IndexCalculator!.GetIndexPostfixByEntity(inputData);
            if (!string.IsNullOrWhiteSpace(indexCalculated))
                indexName += $"-{indexCalculated}";

            indexName = indexName.ToLower();
            indexName.ThrowIfIndexInvalid(false);
            return indexName;
        }

        public string GetIndexNamesForQueries<TEntity>(object? baseObjectFilter) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)_entityMappingList[typeof(TEntity)];
            string fixedIndexName = string.IsNullOrWhiteSpace(_elasticConfig.IndexPrefix) ? $"{mapInstance.BaseIndexName}" : $"{_elasticConfig.IndexPrefix}-{mapInstance.BaseIndexName}";

            if (!string.IsNullOrWhiteSpace(_elasticConfig.IndexSuffix))
                fixedIndexName += $"-{_elasticConfig.IndexSuffix}";

            var indexesToQuery = mapInstance.IndexCalculator!.GetIndexPostfixByFilter(baseObjectFilter).Select(x => x.ToLower());

            string indexNames = indexesToQuery.Any() ? $"{fixedIndexName}-{string.Join($",{fixedIndexName}-".ToLower(), indexesToQuery)}" : fixedIndexName;
            indexNames.ThrowIfIndexInvalid(true);
            return indexNames;
        }

        private void AfterQueryResponse(ElasticsearchResponse queryResponse)
        {
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
