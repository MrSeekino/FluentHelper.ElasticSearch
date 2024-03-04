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
        int MappingLength { get; }

        ElasticsearchClient GetOrCreateClient();

        void Add<TEntity>(TEntity inputData) where TEntity : class;
        Task AddAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class;
        int BulkAdd<TEntity>(IEnumerable<TEntity> inputList) where TEntity : class;
        Task<int> BulkAddAsync<TEntity>(IEnumerable<TEntity> inputList, CancellationToken cancellationToken = default) where TEntity : class;
        void AddOrUpdate<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts = 0) where TEntity : class;
        Task AddOrUpdateAsync<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, CancellationToken cancellationToken = default) where TEntity : class;
        Task AddOrUpdateAsync<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr, int retryOnConflicts, CancellationToken cancellationToken = default) where TEntity : class;
        IEnumerable<TEntity> Query<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class;
        Task<IEnumerable<TEntity>> QueryAsync<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters, CancellationToken cancellationToken = default) where TEntity : class;
        long Count<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters) where TEntity : class;
        Task<long> CountAsync<TEntity>(object? baseObjectFilter, IElasticQueryParameters<TEntity>? queryParameters, CancellationToken cancellationToken = default) where TEntity : class;
        void Delete<TEntity>(TEntity inputData) where TEntity : class;
        Task DeleteAsync<TEntity>(TEntity inputData, CancellationToken cancellationToken = default) where TEntity : class;

        string GetIndexName<TEntity>(TEntity inputData, out ElasticMap<TEntity> mapInstance) where TEntity : class;
        string GetIndexNamesForQueries<TEntity>(object? baseObjectFilter) where TEntity : class;
    }
}
