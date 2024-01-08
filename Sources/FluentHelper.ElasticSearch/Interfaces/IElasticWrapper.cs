using FluentHelper.ElasticSearch.Common;
using Nest;
using System;
using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.Interfaces
{
    public interface IElasticWrapper : IDisposable
    {
        IElasticClient GetContext();
        IElasticClient CreateNewContext();

        void Add<TEntity>(TEntity inputData) where TEntity : class;
        int BulkAdd<TEntity>(IEnumerable<TEntity> inputList) where TEntity : class;
        void AddOrUpdate<TEntity>(TEntity inputData, Func<IElasticFieldUpdater<TEntity>, IElasticFieldUpdater<TEntity>> fieldUpdaterExpr) where TEntity : class;
        IEnumerable<TEntity> Query<TEntity>(object? baseObjectFilter, ElasticQueryParameters<TEntity>? queryParameters) where TEntity : class;
        long Count<TEntity>(object? baseObjectFilter, ElasticQueryParameters<TEntity>? queryParameters) where TEntity : class;
        void Delete<TEntity>(TEntity inputData) where TEntity : class;
    }
}
