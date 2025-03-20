using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Elastic.Transport.Products.Elasticsearch;
using FluentHelper.ElasticSearch.IndexCalculators;
using FluentHelper.ElasticSearch.Interfaces;
using FluentHelper.ElasticSearch.QueryParameters;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Security;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("FluentHelper.ElasticSearch.Tests")]
namespace FluentHelper.ElasticSearch.Common
{
    internal sealed class ElasticWrapper : IElasticWrapper
    {
        private readonly static JsonSerializerOptions _jsonSerializerOptions = new() { ReferenceHandler = ReferenceHandler.IgnoreCycles };

        private ElasticsearchClient? _client;
        private readonly ILogger _logger;
        private readonly IElasticConfig _elasticConfig;
        private readonly Dictionary<Type, IElasticMap> _entityMappingList;

        public int MappingLength => _entityMappingList.Count;

        public ElasticWrapper(ILoggerFactory loggerFactory, IElasticConfig elasticConfig, IEnumerable<IElasticMap> mappings)
            : this(null, loggerFactory, elasticConfig, mappings) { }

        public ElasticWrapper(ElasticsearchClient? client, ILoggerFactory loggerFactory, IElasticConfig elasticConfig, IEnumerable<IElasticMap> mappings)
        {
            _logger = loggerFactory.CreateLogger("FluentHelper.ElasticSearch");
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

        private void Log(LogLevel logLevel, Exception? exception, string message, object?[] args)
        {
            if (_elasticConfig.LogAction != null)
                _elasticConfig.LogAction.Invoke(logLevel, exception, message, args);
            else
                _logger.Log(logLevel, exception, message, args);
        }

        internal void CreateClient()
        {
            _client = null;

            var staticNodePool = new StaticNodePool(_elasticConfig.ConnectionsPool);
            var esSettings = new ElasticsearchClientSettings(staticNodePool);

            if (_elasticConfig.EnableDebug)
                esSettings.EnableDebugMode(_elasticConfig.RequestCompleted!);

            if (!_elasticConfig.SkipCertificateValidation)
            {
                if (_elasticConfig.CertificateFile != null)
                {
                    esSettings.ServerCertificateValidationCallback((message, certificate, chain, sslErrors) =>
                    {
                        if (sslErrors == SslPolicyErrors.None)
                            return true;

                        if (certificate == null)
                            return false;

                        if (chain == null)
                            return false;

                        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                        chain.ChainPolicy.CustomTrustStore.Add(_elasticConfig.CertificateFile);

                        return chain.Build(new X509Certificate2(certificate));
                    });
                }
                else if (!string.IsNullOrWhiteSpace(_elasticConfig.CertificateFingerprint))
                    esSettings.CertificateFingerprint(_elasticConfig.CertificateFingerprint);
            }
            else
                esSettings.ServerCertificateValidationCallback(CertificateValidations.AllowAll);

            if (_elasticConfig.BasicAuthentication != null)
                esSettings.Authentication(new BasicAuthentication(_elasticConfig.BasicAuthentication.Value.Username, _elasticConfig.BasicAuthentication.Value.Password));

            if (_elasticConfig.RequestTimeout.HasValue)
                esSettings.RequestTimeout(_elasticConfig.RequestTimeout.Value);

            if (_elasticConfig.DisablePing)
                esSettings.DisablePing();

            foreach (var m in _entityMappingList)
                m.Value.ApplyMapping(esSettings);

            _client = new ElasticsearchClient(esSettings);

            Log(Microsoft.Extensions.Logging.LogLevel.Trace, null, "ElasticClient created", []);
        }

        public ElasticsearchClient GetOrCreateClient()
        {
            if (_client == null)
                CreateClient();

            return _client!;
        }

        public void Add<TEntity>(TEntity inputData) where TEntity : class
        {
            CreateIndexFromData(inputData);
            IndexName indexName = GetIndexName(inputData);
            var addResponse = GetOrCreateClient().Index(inputData, indexName);
            CheckAddResponse(inputData, addResponse);
        }

        public async Task AddAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class
        {
            await CreateIndexFromDataAsync(inputData, cancellationToken);
            IndexName indexName = GetIndexName(inputData);
            var addResponse = await GetOrCreateClient().IndexAsync(inputData, indexName, cancellationToken);
            CheckAddResponse(inputData, addResponse);
        }

        private void CheckAddResponse<TEntity>(TEntity inputData, IndexResponse addResponse) where TEntity : class
        {
            AfterQueryResponse(addResponse);

            if (!addResponse.IsValidResponse || (addResponse.Result != Result.Created && addResponse.Result != Result.Updated))
                throw new InvalidOperationException("Could not add data", new Exception(SerializeResponse(addResponse)));

            Log(Microsoft.Extensions.Logging.LogLevel.Information, null, "Added {inputData} to {indexName}", [inputData.ToString(), addResponse.Index]);
        }

        public int BulkAdd<TEntity>(IEnumerable<TEntity> inputList) where TEntity : class
        {
            var groupedInputList = PrepareBulkData(inputList);

            int totalIndexedElements = 0;
            foreach (var groupedInputData in groupedInputList)
            {
                CreateIndex<TEntity>(groupedInputData.IndexName);
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
                await CreateIndexAsync<TEntity>(groupedInputData.IndexName, cancellationToken);
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

            var groupedInput = inputList.GroupBy(inputData => GetIndexName(inputData)).Select(x =>
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
            AfterQueryResponse(bulkResponse, false);

            if (!bulkResponse.IsValidResponse || bulkResponse.Errors)
            {
                bulkResponse.TryGetOriginalException(out Exception? exception);
                Log(Microsoft.Extensions.Logging.LogLevel.Error, exception, "Could not BulkAdd some data on index {indexName}", [indexName]);
                return 0;
            }

            Log(Microsoft.Extensions.Logging.LogLevel.Debug, null, "Added {addNumber} to {indexName}", [addNumber, indexName]);
            return addNumber;
        }

        public void AddOrUpdate<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts = 0) where TEntity : class
        {
            var addOrUpdateParameters = GetAddOrUpdatedParameters(inputData, fieldUpdaterExpr, retryOnConflicts);
            CreateIndex<TEntity>(addOrUpdateParameters.IndexName);
            var updateResponse = GetOrCreateClient().Update(addOrUpdateParameters.IndexName, addOrUpdateParameters.Id, addOrUpdateParameters.ConfigureRequest);
            CheckUpdateResponse(inputData, updateResponse);
        }

        public async Task AddOrUpdateAsync<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, CancellationToken cancellationToken = default) where TEntity : class
            => await AddOrUpdateAsync(inputData, fieldUpdaterExpr, 0, cancellationToken);

        public async Task AddOrUpdateAsync<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts, CancellationToken cancellationToken = default) where TEntity : class
        {
            var addOrUpdateParameters = GetAddOrUpdatedParameters(inputData, fieldUpdaterExpr, retryOnConflicts);
            await CreateIndexAsync<TEntity>(addOrUpdateParameters.IndexName, cancellationToken);
            var updateResponse = await GetOrCreateClient().UpdateAsync(addOrUpdateParameters.IndexName, addOrUpdateParameters.Id, addOrUpdateParameters.ConfigureRequest, cancellationToken).ConfigureAwait(false);
            CheckUpdateResponse(inputData, updateResponse);
        }

        private (string IndexName, Id Id, Action<UpdateRequestDescriptor<TEntity, ExpandoObject>> ConfigureRequest) GetAddOrUpdatedParameters<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts) where TEntity : class
        {
            string indexName = GetIndexName(inputData, out IElasticMap mapInstance);

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
                throw new InvalidOperationException("Could not update data", new Exception(SerializeResponse(updateResponse)));

            Log(Microsoft.Extensions.Logging.LogLevel.Information, null, "AddedOrUpdated {inputData} to {indexName}", [inputData.ToString(), updateResponse.Index]);
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
                throw new InvalidOperationException("Could not get data", new Exception(SerializeResponse(queryResponse)));

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
                throw new InvalidOperationException("Could not count data", new Exception(SerializeResponse(countResponse)));

            return countResponse.Count;
        }

        public void Delete<TEntity>(TEntity inputData) where TEntity : class
        {
            var deleteParameters = GetIndexAndIdParameters(inputData);
            var deleteResponse = GetOrCreateClient().Delete<TEntity>(deleteParameters.IndexName, deleteParameters.Id, r => r.Refresh(Refresh.True));
            CheckDeleteResponse(inputData, deleteResponse);
        }

        public async Task DeleteAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class
        {
            var deleteParameters = GetIndexAndIdParameters(inputData);
            var deleteResponse = await GetOrCreateClient().DeleteAsync<TEntity>(deleteParameters.IndexName, deleteParameters.Id, r => r.Refresh(Refresh.True), cancellationToken).ConfigureAwait(false);
            CheckDeleteResponse(inputData, deleteResponse);
        }

        private void CheckDeleteResponse<TEntity>(TEntity inputData, DeleteResponse deleteResponse) where TEntity : class
        {
            AfterQueryResponse(deleteResponse);

            if (!deleteResponse.IsValidResponse || deleteResponse.Result != Result.Deleted)
                throw new InvalidOperationException("Could not delete data", new Exception(SerializeResponse(deleteResponse)));

            Log(Microsoft.Extensions.Logging.LogLevel.Information, null, "Deleted {inputData} from {indexName}", [inputData.ToString(), deleteResponse.Index]);
        }

        public bool Exists<TEntity>(TEntity inputData) where TEntity : class
        {
            var existsParameters = GetIndexAndIdParameters(inputData);
            var existsResponse = GetOrCreateClient().Exists<TEntity>(existsParameters.IndexName, existsParameters.Id, x => { });
            return CheckExistsResponse(existsResponse);
        }

        public async Task<bool> ExistsAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class
        {
            var existsParameters = GetIndexAndIdParameters(inputData);
            var existsResponse = await GetOrCreateClient().ExistsAsync<TEntity>(existsParameters.IndexName, existsParameters.Id, x => { }, cancellationToken);
            return CheckExistsResponse(existsResponse);
        }

        private bool CheckExistsResponse(Elastic.Clients.Elasticsearch.ExistsResponse existsReponse)
        {
            AfterQueryResponse(existsReponse, false);

            return existsReponse.Exists;
        }

        public TEntity? GetSource<TEntity>(TEntity inputData) where TEntity : class
        {
            var existsParameters = GetIndexAndIdParameters(inputData);
            var getResponse = GetOrCreateClient().Get<TEntity>(existsParameters.IndexName, existsParameters.Id);
            return CheckGetResponse(getResponse);
        }

        public async Task<TEntity?> GetSourceAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class
        {
            var existsParameters = GetIndexAndIdParameters(inputData);
            var getResponse = await GetOrCreateClient().GetAsync<TEntity>(existsParameters.IndexName, existsParameters.Id, cancellationToken);
            return CheckGetResponse(getResponse);
        }

        private (string IndexName, Id Id) GetIndexAndIdParameters<TEntity>(TEntity inputData) where TEntity : class
        {
            string indexName = GetIndexName(inputData, out IElasticMap mapInstance);
            var inputId = inputData.GetFieldValue(mapInstance.IdPropertyName);

            return new(indexName, inputId!.ToString()!);
        }

        private TEntity? CheckGetResponse<TEntity>(GetResponse<TEntity> getResponse) where TEntity : class
        {
            AfterQueryResponse(getResponse, false);

            return getResponse.Source;
        }

        private string GetIndexName<TEntity>(TEntity inputData, out IElasticMap mapInstance) where TEntity : class
        {
            mapInstance = _entityMappingList[typeof(TEntity)];
            string indexName = string.IsNullOrWhiteSpace(_elasticConfig.IndexPrefix) ? $"{mapInstance.BaseIndexName}" : $"{_elasticConfig.IndexPrefix}-{mapInstance.BaseIndexName}";

            if (!string.IsNullOrWhiteSpace(_elasticConfig.IndexSuffix))
                indexName += $"-{_elasticConfig.IndexSuffix}";

            string indexCalculated = ((IElasticIndexCalculator<TEntity>?)mapInstance.IndexCalculator)!.GetIndexPostfixByEntity(inputData);
            if (!string.IsNullOrWhiteSpace(indexCalculated))
                indexName += $"-{indexCalculated}";

            indexName = indexName.ToLower();
            indexName.ThrowIfIndexInvalid(false);
            return indexName;
        }

        public string GetIndexName<TEntity>(TEntity inputData) where TEntity : class
        {
            string indexName = GetIndexName(inputData, out _);
            return indexName;
        }

        public string GetIndexNamesForQueries<TEntity>(object? baseObjectFilter) where TEntity : class
        {
            var mapInstance = _entityMappingList[typeof(TEntity)];
            string fixedIndexName = string.IsNullOrWhiteSpace(_elasticConfig.IndexPrefix) ? $"{mapInstance.BaseIndexName}" : $"{_elasticConfig.IndexPrefix}-{mapInstance.BaseIndexName}";

            if (!string.IsNullOrWhiteSpace(_elasticConfig.IndexSuffix))
                fixedIndexName += $"-{_elasticConfig.IndexSuffix}";

            var indexesToQuery = mapInstance.IndexCalculator!.GetIndexPostfixByFilter(baseObjectFilter).Select(x => x.ToLower());

            string indexNames = indexesToQuery.Any() ? $"{fixedIndexName}-{string.Join($",{fixedIndexName}-".ToLower(), indexesToQuery)}" : fixedIndexName;
            indexNames.ThrowIfIndexInvalid(true);
            return indexNames;
        }

        public bool CreateIndexFromData<TEntity>(TEntity inputData) where TEntity : class
        {
            string indexName = GetIndexName(inputData);
            return CreateIndex<TEntity>(indexName);
        }

        public bool CreateIndex<TEntity>(string indexName) where TEntity : class
        {
            var existResponse = GetOrCreateClient().Indices.Exists(indexName);
            if (!CheckIndexExistsResponse(existResponse))
            {
                var requestDescriptor = GetCreateIndexRequestParameters<TEntity>(indexName);
                var createIndexReponse = GetOrCreateClient().Indices.Create(requestDescriptor);
                CheckCreateIndexResponse<TEntity>(createIndexReponse);

                return true;
            }

            return false;
        }

        public async Task<bool> CreateIndexFromDataAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class
        {
            string indexName = GetIndexName(inputData);
            var result = await CreateIndexAsync<TEntity>(indexName, cancellationToken);
            return result;
        }

        public async Task<bool> CreateIndexAsync<TEntity>(string indexName, CancellationToken cancellationToken = default) where TEntity : class
        {
            var existResponse = await GetOrCreateClient().Indices.ExistsAsync(indexName, cancellationToken);
            if (!CheckIndexExistsResponse(existResponse))
            {
                var requestDescriptor = GetCreateIndexRequestParameters<TEntity>(indexName);
                var createIndexReponse = await GetOrCreateClient().Indices.CreateAsync(requestDescriptor, cancellationToken);
                CheckCreateIndexResponse<TEntity>(createIndexReponse);

                return true;
            }

            return false;
        }

        private CreateIndexRequestDescriptor GetCreateIndexRequestParameters<TEntity>(string indexName) where TEntity : class
        {
            var mapInstance = _entityMappingList[typeof(TEntity)];
            CreateIndexRequestDescriptor createIndexRequestDescriptor = new(indexName);

            if (!mapInstance.CreateTemplate)
            {
                if (mapInstance.IndexMappings != null)
                    createIndexRequestDescriptor.Mappings(m => m.Properties(mapInstance.IndexMappings));

                if (mapInstance.IndexSettings != null)
                    createIndexRequestDescriptor.Settings(mapInstance.IndexSettings);
            }

            return createIndexRequestDescriptor;
        }

        private bool CheckIndexExistsResponse(Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse existsReponse)
        {
            AfterQueryResponse(existsReponse, false);

            return existsReponse.Exists;
        }

        private void CheckCreateIndexResponse<TEntity>(CreateIndexResponse createIndexReponse) where TEntity : class
        {
            AfterQueryResponse(createIndexReponse);

            if (!createIndexReponse.IsValidResponse)
                throw new InvalidOperationException($"Could not create index {createIndexReponse.Index} for {typeof(TEntity).Name}", new Exception(SerializeResponse(createIndexReponse)));

            Log(Microsoft.Extensions.Logging.LogLevel.Information, null, "Index {indexName} created", [createIndexReponse.Index]);
        }

        public (int CreatedTemplates, int AlreadyExistingTemplates, int FailedTemplates, int TotalDefinedTemplates) CreateAllMappedIndexTemplate()
        {
            (int CreatedTemplates, int AlreadyExistingTemplates, int FailedTemplates, int TotalDefinedTemplates) result = new();

            foreach (var (_, mapInstance) in _entityMappingList.Where(x => x.Value.CreateTemplate))
            {
                try
                {
                    result.TotalDefinedTemplates++;

                    bool templateCreated = CreateIndexTemplate(mapInstance);

                    if (templateCreated)
                        result.CreatedTemplates++;
                    else
                        result.AlreadyExistingTemplates++;
                }
                catch
                {
                    result.FailedTemplates++;
                }
            }

            return result;
        }

        public async Task<(int CreatedTemplates, int AlreadyExistingTemplates, int FailedTemplates, int TotalDefinedTemplates)> CreateAllMappedIndexTemplateAsync()
        {
            (int CreatedTemplates, int AlreadyExistingTemplates, int FailedTemplates, int TotalDefinedTemplates) result = new();

            foreach (var (_, mapInstance) in _entityMappingList.Where(x => x.Value.CreateTemplate))
            {
                try
                {
                    result.TotalDefinedTemplates++;

                    bool templateCreated = await CreateIndexTemplateAsync(mapInstance);

                    if (templateCreated)
                        result.CreatedTemplates++;
                    else
                        result.AlreadyExistingTemplates++;
                }
                catch
                {
                    result.FailedTemplates++;
                }
            }

            return result;
        }

        public bool CreateIndexTemplate<TEntity>(IElasticMap? mapInstance = null) where TEntity : class
        {
            mapInstance ??= _entityMappingList[typeof(TEntity)];

            if (mapInstance.GetMappingType() != typeof(TEntity))
                throw new InvalidOperationException($"'MapInstance' and 'TEntity' must be of the same type");

            return CreateIndexTemplate(mapInstance);
        }

        public bool CreateIndexTemplate(IElasticMap mapInstance)
        {
            var templateParameters = GetCreateTemplateParameters(mapInstance);

            var existsIndexTemplateResponse = GetOrCreateClient().Indices.ExistsIndexTemplate(templateParameters.TemplateName);
            if (!CheckIndexTemplateExistsResponse(existsIndexTemplateResponse))
            {
                var requestDescriptor = GetIndexTemplateRequest(templateParameters.TemplateName, templateParameters.IndexPatterns, mapInstance);
                var putTemplateResponse = GetOrCreateClient().Indices.PutIndexTemplate(requestDescriptor);
                CheckPutIndexTemplateResponse(putTemplateResponse, templateParameters.TemplateName, templateParameters.IndexPatterns, mapInstance);

                return true;
            }

            return false;
        }

        public async Task<bool> CreateIndexTemplateAsync<TEntity>(CancellationToken cancellationToken = default) where TEntity : class
            => await CreateIndexTemplateAsync<TEntity>(null, cancellationToken);

        public async Task<bool> CreateIndexTemplateAsync<TEntity>(IElasticMap? mapInstance, CancellationToken cancellationToken = default) where TEntity : class
        {
            mapInstance ??= _entityMappingList[typeof(TEntity)];

            if (mapInstance.GetMappingType() != typeof(TEntity))
                throw new InvalidOperationException($"'MapInstance' and 'TEntity' must be of the same type");

            var templateCreated = await CreateIndexTemplateAsync(mapInstance, cancellationToken);
            return templateCreated;
        }

        public async Task<bool> CreateIndexTemplateAsync(IElasticMap mapInstance, CancellationToken cancellationToken = default)
        {
            var templateParameters = GetCreateTemplateParameters(mapInstance);

            var existsIndexTemplateResponse = await GetOrCreateClient().Indices.ExistsIndexTemplateAsync(templateParameters.TemplateName, cancellationToken);
            if (!CheckIndexTemplateExistsResponse(existsIndexTemplateResponse))
            {
                var requestDescriptor = GetIndexTemplateRequest(templateParameters.TemplateName, templateParameters.IndexPatterns, mapInstance);
                var putTemplateResponse = await GetOrCreateClient().Indices.PutIndexTemplateAsync(requestDescriptor, cancellationToken);
                CheckPutIndexTemplateResponse(putTemplateResponse, templateParameters.TemplateName, templateParameters.IndexPatterns, mapInstance);

                return true;
            }

            return false;
        }

        private (string TemplateName, string IndexPatterns) GetCreateTemplateParameters(IElasticMap mapInstance)
        {
            string baseIndexPattern = string.IsNullOrWhiteSpace(_elasticConfig.IndexPrefix) ? $"{mapInstance.BaseIndexName}" : $"{_elasticConfig.IndexPrefix}-{mapInstance.BaseIndexName}";

            string templateName = string.IsNullOrWhiteSpace(mapInstance.TemplateName) ? $"{baseIndexPattern}_template" : mapInstance.TemplateName;
            string indexPatterns = $"{baseIndexPattern}{mapInstance.IndexCalculator!.GetBaseIndexWildcard()}";

            return new(templateName, indexPatterns);
        }

        private static PutIndexTemplateRequestDescriptor GetIndexTemplateRequest(string templateName, string indexPatterns, IElasticMap mapInstance)
        {
            PutIndexTemplateRequestDescriptor requestDescriptor = new(templateName);
            requestDescriptor.IndexPatterns(indexPatterns);

            requestDescriptor.Template(t =>
            {
                if (mapInstance.IndexMappings != null)
                    t.Mappings(m => m.Properties(mapInstance.IndexMappings));

                if (mapInstance.IndexSettings != null)
                    t.Settings(mapInstance.IndexSettings);
            });

            return requestDescriptor;
        }

        private bool CheckIndexTemplateExistsResponse(ExistsIndexTemplateResponse existsReponse)
        {
            AfterQueryResponse(existsReponse, false);

            return existsReponse.Exists;
        }

        private void CheckPutIndexTemplateResponse(PutIndexTemplateResponse putTemplateResponse, string templateName, string indexPatterns, IElasticMap mapInstance)
        {
            AfterQueryResponse(putTemplateResponse);

            if (!putTemplateResponse.IsValidResponse)
                throw new InvalidOperationException($"Could not create template {templateName} for {mapInstance.GetMappingType().Name} with patterns {indexPatterns}", new Exception(SerializeResponse(putTemplateResponse)));

            Log(Microsoft.Extensions.Logging.LogLevel.Information, null, "Template {templateName} created", [templateName]);
        }

        public HealthReportResponse GetHealthReport(bool verbose = false, int size = 1000)
        {
            return GetOrCreateClient().HealthReport(x =>
            {
                x.Size(size).Verbose(verbose);
            });
        }

        public async Task<HealthReportResponse> GetHealthReportAsync(bool verbose = false, int size = 1000)
        {
            return await GetOrCreateClient().HealthReportAsync(x =>
            {
                x.Size(size).Verbose(verbose);
            });
        }

        private void AfterQueryResponse(ElasticsearchResponse queryResponse, bool trapWhenInvalid = true)
        {
            if (trapWhenInvalid && !queryResponse.IsValidResponse)
            {
                if (!queryResponse.TryGetOriginalException(out var originalException))
                    originalException = null;

                Log(Microsoft.Extensions.Logging.LogLevel.Error, originalException, queryResponse!.DebugInformation, []);
                return;
            }

            Log(Microsoft.Extensions.Logging.LogLevel.Debug, null, queryResponse!.DebugInformation, []);
        }

        private string SerializeResponse<T>(T response)
        {
            return System.Text.Json.JsonSerializer.Serialize(response, _jsonSerializerOptions);
        }

        public void Dispose()
        {
            _client = null;
        }
    }
}
