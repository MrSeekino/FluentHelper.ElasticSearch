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

[assembly: InternalsVisibleTo("FluentHelper.ElasticSearch.Tests")]
namespace FluentHelper.ElasticSearch.Common
{
    internal sealed class ElasticWrapper : IElasticWrapper
    {
        internal IElasticClient? Client { get; set; }
        internal IElasticConfig ElasticConfig { get; set; }

        internal Dictionary<Type, IElasticMap> EntityMappingList { get; set; } = new Dictionary<Type, IElasticMap>();

        public ElasticWrapper(IElasticConfig elasticConfig, IEnumerable<IElasticMap> mappings)
        {
            Client = null;

            ElasticConfig = elasticConfig;

            foreach (var elasticMap in mappings)
            {
                elasticMap.Map();
                elasticMap.Verify();

                EntityMappingList.Add(elasticMap.GetMapType(), elasticMap);
            }
        }

        internal void CreateDbContext()
        {
            Client = null;

            var esSettings = new ConnectionSettings(new Uri(ElasticConfig.ConnectionUrl!)).DisableDirectStreaming(ElasticConfig.DebugQuery);

            if (ElasticConfig.EnableApiVersioningHeader)
                esSettings.EnableApiVersioningHeader();

            if (!string.IsNullOrWhiteSpace(ElasticConfig.CertificateFingerprint))
                esSettings.CertificateFingerprint(ElasticConfig.CertificateFingerprint);

            if (ElasticConfig.BasicAuthentication != null)
                esSettings.BasicAuthentication(ElasticConfig.BasicAuthentication.Value.Username, ElasticConfig.BasicAuthentication.Value.Password);

            if (ElasticConfig.RequestTimeout.HasValue)
                esSettings.RequestTimeout(ElasticConfig.RequestTimeout.Value);

            SetMappings(esSettings);
            Client = new ElasticClient(esSettings);

            ElasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Trace, "ElasticClient created", null);
        }

        internal void SetMappings(ConnectionSettings esSettings)
        {
            foreach (var m in EntityMappingList)
                m.Value.ApplySpecialMap(esSettings);
        }

        public IElasticClient GetContext()
        {
            if (Client == null)
                CreateDbContext();

            return Client!;
        }

        public IElasticClient CreateNewContext()
        {
            Dispose();

            return GetContext();
        }

        public void Add<TEntity>(TEntity inputData) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)EntityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);

            var addResponse = GetContext().Index(inputData, x => x.Index(indexName));
            AfterQueryResponse(addResponse);

            if (addResponse == null || !addResponse.IsValid || (addResponse.Result != Result.Created && addResponse.Result != Result.Updated))
                throw new InvalidOperationException("Could not add data", new Exception(JsonConvert.SerializeObject(addResponse)));

            ElasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, $"Added {inputData} to {indexName}", null);
        }

        public int BulkAdd<TEntity>(IEnumerable<TEntity> inputList) where TEntity : class
        {
            if (inputList == null || !inputList.Any())
                return 0;

            var mapInstance = (ElasticMap<TEntity>)EntityMappingList[typeof(TEntity)];

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
                        var inputListToAdd = groupedInputData.InputList.Skip(indexedElements).Take(ElasticConfig.BulkInsertChunkSize).ToList();
                        ElasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, $"Indexing {inputListToAdd.Count} elements into {groupedInputData.IndexName}. {indexedElements}/{groupedInputData.InputList}", null);

                        var bulkResponse = GetContext().Bulk(b => b.Index(groupedInputData.IndexName).IndexMany(inputListToAdd));
                        AfterQueryResponse(bulkResponse);

                        if (bulkResponse == null || !bulkResponse.IsValid || bulkResponse.Errors)
                            throw new InvalidOperationException("Could not bulkadd data", new Exception(JsonConvert.SerializeObject(bulkResponse)));

                        ElasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, $"Added {inputListToAdd.Count} to {groupedInputData.IndexName}", null);
                        indexedElements += inputListToAdd.Count();
                        totalIndexedElements += indexedElements;
                    }
                }
                catch (Exception ex)
                {
                    ElasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Error, $"Could not BulkAdd some data on index {groupedInputData.IndexName}", ex);
                }
                finally
                {
                    ElasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, $"BulkAdded {indexedElements}/{groupedInputData.InputList.Count} to {groupedInputData.IndexName}", null);
                }
            }

            return totalIndexedElements;
        }

        public void AddOrUpdate<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)EntityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);

            IElasticFieldUpdater<TEntity>? elasticFieldUpdater = fieldUpdaterExpr != null ? fieldUpdaterExpr(new ElasticFieldUpdater<TEntity>(mapInstance.IdPropertyName)) : null;
            var updateObj = GetExpandoObject(inputData, elasticFieldUpdater);

            var inputId = GetFieldValue(inputData, mapInstance.IdPropertyName);
            var updateResponse = GetContext().Update<TEntity, ExpandoObject>(inputId!.ToString(), x => x.Index(indexName).Doc(updateObj).Upsert(inputData).Refresh(Refresh.True));
            AfterQueryResponse(updateResponse);

            if (updateResponse == null || !updateResponse.IsValid || (updateResponse.Result != Result.Created && updateResponse.Result != Result.Updated && updateResponse.Result != Result.Noop))
                throw new InvalidOperationException("Could not update data", new Exception(JsonConvert.SerializeObject(updateResponse)));

            ElasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, $"AddedOrUpdated {inputData} to {indexName}", null);
        }

        public IEnumerable<TEntity> Query<TEntity>(object? baseObjectFilter, ElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)EntityMappingList[typeof(TEntity)];

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

        public long Count<TEntity>(object? baseObjectFilter, ElasticQueryParameters<TEntity>? queryParameters) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)EntityMappingList[typeof(TEntity)];
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

        public void Delete<TEntity>(TEntity inputData) where TEntity : class
        {
            var mapInstance = (ElasticMap<TEntity>)EntityMappingList[typeof(TEntity)];
            string indexName = GetIndexName(mapInstance, inputData);

            var deleteResponse = GetContext().Delete<TEntity>(inputData, x => x.Index(indexName));
            AfterQueryResponse(deleteResponse);

            if (deleteResponse == null || !deleteResponse.IsValid || deleteResponse.Result != Result.Deleted)
                throw new InvalidOperationException("Could not delete data", new Exception(JsonConvert.SerializeObject(deleteResponse)));

            ElasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Information, $"Deleted {inputData} from {indexName}", null);
        }

        public string GetIndexName<TEntity>(ElasticMap<TEntity> elasticMap, TEntity inputData) where TEntity : class
        {
            string indexName = string.IsNullOrWhiteSpace(ElasticConfig.IndexPrefix) ? $"{elasticMap.BaseIndexName}" : $"{ElasticConfig.IndexPrefix}-{elasticMap.BaseIndexName}";

            if (!string.IsNullOrWhiteSpace(ElasticConfig.IndexSuffix))
                indexName += $"-{ElasticConfig.IndexSuffix}";

            string indexCalculated = elasticMap.IndexCalculator!.CalcEntityIndex(inputData);
            if (!string.IsNullOrWhiteSpace(indexCalculated))
                indexName += $"-{indexCalculated}";

            indexName = indexName.ToLower();
            VerifyIndexNames(indexName, false);
            return indexName;
        }

        public string GetIndexNamesForQueries<TEntity>(ElasticMap<TEntity> elasticMap, object? baseObjectFilter) where TEntity : class
        {
            string fixedIndexName = string.IsNullOrWhiteSpace(ElasticConfig.IndexPrefix) ? $"{elasticMap.BaseIndexName}" : $"{ElasticConfig.IndexPrefix}-{elasticMap.BaseIndexName}";

            if (!string.IsNullOrWhiteSpace(ElasticConfig.IndexSuffix))
                fixedIndexName += $"-{ElasticConfig.IndexSuffix}";

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
                ElasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Error, queryResponse!.DebugInformation, queryResponse!.OriginalException);
                return;
            }

            ElasticConfig.LogAction?.Invoke(Microsoft.Extensions.Logging.LogLevel.Debug, queryResponse!.DebugInformation, null);
        }

        public void Dispose()
        {
            Client = null;
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
