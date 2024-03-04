using Elastic.Clients.Elasticsearch;
using FluentHelper.ElasticSearch.IndexCalculators;
using FluentHelper.ElasticSearch.Interfaces;
using System;
using System.Linq.Expressions;

namespace FluentHelper.ElasticSearch.Common
{
    public abstract class ElasticMap<TEntity> : IElasticMap where TEntity : class
    {
        public virtual string BaseIndexName { get; private set; }
        public virtual IElasticIndexCalculator<TEntity>? IndexCalculator { get; private set; }
        public virtual string IdPropertyName { get; private set; }

        protected ElasticMap()
        {
            BaseIndexName = string.Empty;
            IndexCalculator = null;
            IdPropertyName = string.Empty;
        }

        /// <summary>
        /// Set the base indexname for the type
        /// </summary>
        /// <param name="baseIndexName">the base indexname</param>
        protected void SetBaseIndexName(string baseIndexName)
        {
            BaseIndexName = baseIndexName;
        }

        /// <summary>
        /// Set a custom IndexCalculator to be used when querying elastic with the current type
        /// </summary>
        /// <param name="indexCalculator">the index calculator</param>
        protected void SetIndexCalculator(IElasticIndexCalculator<TEntity> indexCalculator)
        {
            IndexCalculator = indexCalculator;
        }

        /// <summary>
        /// Set a basic index calculator
        /// </summary>
        /// <param name="basicIndexCalculator">the action to configure the basic calculator</param>
        public void SetBasicIndexCalculator(Action<IBasicIndexCalculator<TEntity>>? basicIndexCalculator = null)
        {
            var indexCalculator = BasicIndexCalculator<TEntity>.Create();
            if (basicIndexCalculator != null)
                basicIndexCalculator(indexCalculator);

            IndexCalculator = indexCalculator;
        }

        /// <summary>
        /// Set a custom index calculator that allows a filter to be used when querying elastic with the current type
        /// </summary>
        /// <typeparam name="TFilter">the filter type</typeparam>
        /// <param name="customIndexCalculator">the action to configure the custom calculator</param>
        protected void SetCustomIndexCalculator<TFilter>(Action<ICustomIndexCalculator<TEntity, TFilter>> customIndexCalculator)
        {
            var indexCalculator = CustomIndexCalculator<TEntity, TFilter>.Create();
            customIndexCalculator(indexCalculator);

            IndexCalculator = indexCalculator;
        }

        /// <summary>
        /// Set the field considered as the Id in elasticsearch
        /// </summary>
        /// <typeparam name="P">property type</typeparam>
        /// <param name="expression">the field to be the Id</param>
        protected void Id<P>(Expression<Func<TEntity, P>> expression)
        {
            IdPropertyName = ((MemberExpression)expression.Body).Member.Name;
        }

        /// <summary>
        /// Get the current true type of the mapping
        /// </summary>
        /// <returns></returns>
        public Type GetMapType()
        {
            return typeof(TEntity);
        }

        /// <summary>
        /// Apply the map to the ElasticsearchClient settings. Automatically used when building the wrapper
        /// </summary>
        /// <param name="esSettings"></param>
        public void ApplySpecialMap(ElasticsearchClientSettings esSettings)
        {
            esSettings.DefaultMappingFor<TEntity>(x =>
            {
                x.IdProperty(IdPropertyName);
            });
        }

        /// <summary>
        /// Verify that the map is compliant with requirements
        /// </summary>
        /// <exception cref="InvalidOperationException"></exception>
        public void Verify()
        {
            if (string.IsNullOrWhiteSpace(BaseIndexName))
                throw new InvalidOperationException($"BaseIndexName has not been set for {typeof(TEntity).Name}");

            if (IndexCalculator == null)
                throw new InvalidOperationException($"IndexCalculator has not been set for {typeof(TEntity).Name}");

            if (string.IsNullOrWhiteSpace(IdPropertyName))
                throw new InvalidOperationException($"IdProperty has not been set for {typeof(TEntity).Name}");
        }

        public abstract void Map();
    }
}
