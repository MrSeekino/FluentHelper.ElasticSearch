using Elastic.Clients.Elasticsearch;
using FluentHelper.ElasticSearch.Common;
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
        int MappingLength { get; }

        /// <summary>
        /// Get the current ElasticsearchClient
        /// </summary>
        /// <returns></returns>
        ElasticsearchClient GetOrCreateClient();

        /// <summary>
        /// Add an item to the index following its mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be added</param>
        void Add<TEntity>(TEntity inputData) where TEntity : class;

        /// <summary>
        /// Add an item to the index following its mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be added</param>
        Task AddAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Add multiple items to the index following its mapping with configured chunks
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputList">the data to be added</param>
        /// <returns></returns>
        int BulkAdd<TEntity>(IEnumerable<TEntity> inputList) where TEntity : class;
        /// <summary>
        /// Add multiple items to the index following its mapping with configured chunks
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputList">the data to be added</param>
        Task<int> BulkAddAsync<TEntity>(IEnumerable<TEntity> inputList, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Add or updated an item to the index following its mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be added or updated</param>
        /// <param name="fieldUpdaterExpr">the fields to be updated if item already exists</param>
        /// <param name="retryOnConflicts">number of retry if update fails due to concurrency updates</param>
        void AddOrUpdate<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts = 0) where TEntity : class;
        /// <summary>
        /// Add or updated an item to the index following its mapping without retries on fails
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be added or updated</param>
        /// <param name="fieldUpdaterExpr">the fields to be updated if item already exists</param>
        Task AddOrUpdateAsync<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, CancellationToken cancellationToken = default) where TEntity : class;
        /// <summary>
        /// Add or updated an item to the index following its mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be added or updated</param>
        /// <param name="fieldUpdaterExpr">the fields to be updated if item already exists</param>
        /// <param name="retryOnConflicts">number of retry if update fails due to concurrency updates</param>
        Task AddOrUpdateAsync<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Search data on indexes for the specified type
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="baseObjectFilter">the base filter to be used when using custom index calculator</param>
        /// <param name="queryParameters">specific queryparameters for the query</param>
        /// <returns></returns>
        IEnumerable<TEntity> Query<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class;
        Task<IEnumerable<TEntity>> QueryAsync<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Count the number of elements for the specified type
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="baseObjectFilter">the base filter to be used when using custom index calculator</param>
        /// <param name="queryParameters">specific queryparameters for the query</param>
        /// <returns></returns>
        long Count<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class;
        Task<long> CountAsync<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Delete the specified elements from the index using the mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be deleted</param>
        void Delete<TEntity>(TEntity inputData) where TEntity : class;
        /// <summary>
        /// Delete the specified elements from the index using the mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to be deleted</param>
        Task DeleteAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class;

        /// <summary>
        /// Check if the specifed elements exist in the index that is supposed to based on mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to verify the existence</param>
        /// <returns></returns>
        bool Exists<TEntity>(TEntity inputData) where TEntity : class;
        /// <summary>
        /// Check if the specifed elements exist in the index that is supposed to based on mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to verify the existence</param>
        /// <returns></returns>
        Task<bool> ExistsAsync<TEntity>(TEntity inputData) where TEntity : class;

        /// <summary>
        /// Get the specified source using the configured mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to get all the fields. Only the field that is mapped as the Id is needed for the call</param>
        /// <returns></returns>
        TEntity? GetSource<TEntity>(TEntity inputData) where TEntity : class;
        /// <summary>
        /// Get the specified source using the configured mapping
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data to get all the fields. Only the field that is mapped as the Id is needed for the call</param>
        /// <returns></returns>
        Task<TEntity?> GetSourceAsync<TEntity>(TEntity inputData) where TEntity : class;

        /// <summary>
        /// Get the indexname for the inputdata based on mappings
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="inputData">the data that the index will be calculated on</param>
        /// <param name="mapInstance">the mapping instance used for the calculation</param>
        /// <returns></returns>
        string GetIndexName<TEntity>(TEntity inputData, out ElasticMap<TEntity> mapInstance) where TEntity : class;
        /// <summary>
        /// Get all the indexes that will be used for a query on the specified filter
        /// </summary>
        /// <typeparam name="TEntity">the type of data</typeparam>
        /// <param name="baseObjectFilter">the base filter to be used when using custom index calculator</param>
        /// <returns></returns>
        string GetIndexNamesForQueries<TEntity>(object? baseObjectFilter) where TEntity : class;
    }
}
