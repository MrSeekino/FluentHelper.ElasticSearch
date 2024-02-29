using Elastic.Clients.Elasticsearch;
using FluentHelper.ElasticSearch.IndexCalculators;
using FluentHelper.ElasticSearch.Interfaces;
using System;
using System.Linq.Expressions;

namespace FluentHelper.ElasticSearch.Common
{
    public abstract class ElasticMap<TEntity> : IElasticMap where TEntity : class
    {
        public string BaseIndexName { get; private set; } = typeof(TEntity).Name;
        public IElasticIndexCalculator<TEntity> IndexCalculator { get; private set; }

        public string IdPropertyName { get; private set; } = string.Empty;

        protected ElasticMap()
        {
            IndexCalculator = new BasicIndexCalculator<TEntity>();
        }

        protected void SetIndexCalculator(IElasticIndexCalculator<TEntity> indexCalculator)
        {
            IndexCalculator = indexCalculator;
        }

        protected void SetBaseIndexName(string baseIndexName)
        {
            BaseIndexName = baseIndexName;
        }

        protected void Id<P>(Expression<Func<TEntity, P>> expression)
        {
            IdPropertyName = ((MemberExpression)expression.Body).Member.Name;
        }

        public Type GetMapType()
        {
            return typeof(TEntity);
        }

        public void ApplySpecialMap(ElasticsearchClientSettings esSettings)
        {
            esSettings.DefaultMappingFor<TEntity>(x =>
            {
                x.IdProperty(IdPropertyName);
            });
        }

        public void Verify()
        {
            if (string.IsNullOrWhiteSpace(IdPropertyName))
                throw new NullReferenceException($"IdProperty has not been set for {typeof(TEntity).Name}");
        }

        public abstract void Map();
    }
}
