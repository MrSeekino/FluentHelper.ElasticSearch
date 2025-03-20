using Elastic.Clients.Elasticsearch;
using FluentHelper.ElasticSearch.QueryParameters;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FluentHelper.ElasticSearch.Interfaces
{
    public interface IElasticWrapper : IDisposable
    {
        /// <summary>
        /// Number of mappings instantiated
        /// </summary>
        /// 
        int MappingLength { get; }

        /// <summary>
        /// Get the current ElasticsearchClient
        /// </summary>
        /// <returns>The current 'ElasticsearchClient' client</returns>
        ElasticsearchClient GetOrCreateClient();

        /// <summary>
        /// Add an item to the index following its mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be added</param>
        [Obsolete("Use the async version")]
        void Add<TEntity>(TEntity inputData) where TEntity : class;

        /// <summary>
        /// Add an item to the index following its mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be added</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        Task AddAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Add multiple items to the index following its mapping with configured chunks
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputList">the data to be added</param>
        /// <returns>The total number of items added</returns>
        [Obsolete("Use the async version")]
        int BulkAdd<TEntity>(IEnumerable<TEntity> inputList) where TEntity : class;
        /// <summary>
        /// Add multiple items to the index following its mapping with configured chunks
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputList">the data to be added</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        /// <returns>The total number of items added</returns>
        Task<int> BulkAddAsync<TEntity>(IEnumerable<TEntity> inputList, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Add or updated an item to the index following its mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be added or updated</param>
        /// <param name="fieldUpdaterExpr">the fields to be updated if item already exists</param>
        /// <param name="retryOnConflicts">number of retry if update fails due to concurrency updates</param>
        [Obsolete("Use the async version")]
        void AddOrUpdate<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts = 0) where TEntity : class;
        /// <summary>
        /// Add or updated an item to the index following its mapping without retries on fails
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be added or updated</param>
        /// <param name="fieldUpdaterExpr">the fields to be updated if item already exists</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        Task AddOrUpdateAsync<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, CancellationToken cancellationToken = default) where TEntity : class;
        /// <summary>
        /// Add or updated an item to the index following its mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be added or updated</param>
        /// <param name="fieldUpdaterExpr">the fields to be updated if item already exists</param>
        /// <param name="retryOnConflicts">number of retry if update fails due to concurrency updates</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        Task AddOrUpdateAsync<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Search data on indexes for the specified type
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="baseObjectFilter">the base filter to be used when using custom index calculator</param>
        /// <param name="queryParameters">specific queryparameters for the query</param>
        /// <returns>The list of data matching the query</returns>
        [Obsolete("Use the async version")]
        IEnumerable<TEntity> Query<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class;
        /// <summary>
        /// Search data on indexes for the specified type
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="baseObjectFilter">the base filter to be used when using custom index calculator</param>
        /// <param name="queryParameters">specific queryparameters for the query</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        /// <returns>The list of data matching the query</returns>
        Task<IEnumerable<TEntity>> QueryAsync<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Count the number of elements for the specified type
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="baseObjectFilter">the base filter to be used when using custom index calculator</param>
        /// <param name="queryParameters">specific queryparameters for the query</param>
        /// <returns>The number of items matching the query</returns>
        [Obsolete("Use the async version")]
        long Count<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class;
        /// <summary>
        /// Count the number of elements for the specified type
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="baseObjectFilter">the base filter to be used when using custom index calculator</param>
        /// <param name="queryParameters">specific queryparameters for the query</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        /// <returns>The number of items matching the query</returns>
        Task<long> CountAsync<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Delete the specified elements from the index using the mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be deleted</param>
        [Obsolete("Use the async version")]
        void Delete<TEntity>(TEntity inputData) where TEntity : class;
        /// <summary>
        /// Delete the specified elements from the index using the mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be deleted</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        Task DeleteAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Check if the specifed elements exist in the index that is supposed to based on mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to verify the existence</param>
        /// <returns>true if data is found</returns>
        [Obsolete("Use the async version")]
        bool Exists<TEntity>(TEntity inputData) where TEntity : class;
        /// <summary>
        /// Check if the specifed elements exist in the index that is supposed to based on mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to verify the existence</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        /// <returns>true if data is found</returns>
        Task<bool> ExistsAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Get the specified source using the configured mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to get all the fields. Only the field that is mapped as the Id is needed for the call</param>
        /// <returns>The entity data if present</returns>
        [Obsolete("Use the async version")]
        TEntity? GetSource<TEntity>(TEntity inputData) where TEntity : class;
        /// <summary>
        /// Get the specified source using the configured mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to get all the fields. Only the field that is mapped as the Id is needed for the call</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        /// <returns>The entity data if present</returns>
        Task<TEntity?> GetSourceAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Get the indexname for the inputdata based on mappings
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data that the index will be calculated on</param>
        /// <returns>the name of the index the input data will be mapped to</returns>
        string GetIndexName<TEntity>(TEntity inputData) where TEntity : class;
        /// <summary>
        /// Get all the indexes that will be used for a query on the specified filter
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="baseObjectFilter">the base filter to be used when using custom index calculator</param>
        /// <returns>the name of the indexes that match the object filter</returns>
        string GetIndexNamesForQueries<TEntity>(object? baseObjectFilter) where TEntity : class;

        /// <summary>
        /// Create an index with the provided name and configured mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="indexName">the name of the index</param>
        /// <returns>true if the index is created, false if already exists, throws if fails</returns>
        [Obsolete("Use the async version")]
        bool CreateIndex<TEntity>(string indexName) where TEntity : class;
        /// <summary>
        /// Create an index with the provided name and configured mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="indexName">the name of the index</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        /// <returns>true if the index is created, false if already exists, throws if fails</returns>
        Task<bool> CreateIndexAsync<TEntity>(string indexName, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Create an index based in the input data and configured mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the input data used to calculate index parameters</param>
        /// <returns>true if the index is created, false if already exists, throws if fails</returns>
        [Obsolete("Use the async version")]
        bool CreateIndexFromData<TEntity>(TEntity inputData) where TEntity : class;
        /// <summary>
        /// Create an index based in the input data and configured mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the input data used to calculate index parameters</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        /// <returns>true if the index is created, false if already exists, throws if fails</returns>
        Task<bool> CreateIndexFromDataAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Create templates for all the defined mappings
        /// </summary>
        /// <returns>the number of templates created, alreadyexisting, failed and the number of total templates defined</returns>
        (int CreatedTemplates, int AlreadyExistingTemplates, int FailedTemplates, int TotalDefinedTemplates) CreateAllMappedIndexTemplate();
        /// <summary>
        /// Create templates for all the defined mappings
        /// </summary>
        /// <returns>the number of templates created, alreadyexisting, failed and the number of total templates defined</returns>
        Task<(int CreatedTemplates, int AlreadyExistingTemplates, int FailedTemplates, int TotalDefinedTemplates)> CreateAllMappedIndexTemplateAsync();

        /// <summary>
        /// Create an index template compliant to the configured mapping for the type
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="mapInstance">the mapping instance to be used</param>
        /// <returns>true if the template is created, false if already exists, throws if fails</returns>
        [Obsolete("Use the async version")]
        bool CreateIndexTemplate<TEntity>(IElasticMap? mapInstance = null) where TEntity : class;
        /// <summary>
        /// Create an index template compliant to the configured mapping for the type
        /// </summary>
        /// <param name="mapInstance">the mapping instance to be used</param>
        /// <returns>true if the template is created, false if already exists, throws if fails</returns>
        [Obsolete("Use the async version")]
        bool CreateIndexTemplate(IElasticMap mapInstance);
        /// <summary>
        /// Create an index template compliant to the configured mapping for the type
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        /// <returns>true if the template is created, false if already exists, throws if fails</returns>
        Task<bool> CreateIndexTemplateAsync<TEntity>(CancellationToken cancellationToken = default) where TEntity : class;
        /// <summary>
        /// Create an index template compliant to the configured mapping for the type
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="mapInstance">the map instance to be used. If null it is automatically retrieved from mappings</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        /// <returns>true if the template is created, false if already exists, throws if fails</returns>
        Task<bool> CreateIndexTemplateAsync<TEntity>(IElasticMap? mapInstance, CancellationToken cancellationToken = default) where TEntity : class;
        /// <summary>
        /// Create an index template compliant to the configured mapping for the type
        /// </summary>
        /// <param name="mapInstance">the mapping instance to be used</param>
        /// <param name="cancellationToken">the token to cancel the operation</param>
        /// <returns>true if the template is created, false if already exists, throws if fails</returns>
        Task<bool> CreateIndexTemplateAsync(IElasticMap mapInstance, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get the health report of the elastic cluster
        /// </summary>
        /// <param name="verbose">true if the report has to be detailed</param>
        /// <param name="size">maximum number of affected resources to return</param>
        /// <returns>an HealthReportResponse if the query could be done, throws if fails</returns>
        HealthReportResponse GetHealthReport(bool verbose = false, int size = 1000);
        /// <summary>
        /// Get the health report of the elastic cluster
        /// </summary>
        /// <param name="verbose">true if the report has to be detailed</param>
        /// <param name="size">maximum number of affected resources to return</param>
        /// <returns>an HealthReportResponse if the query could be done, throws if fails</returns>
        Task<HealthReportResponse> GetHealthReportAsync(bool verbose = false, int size = 1000);
    }
}
