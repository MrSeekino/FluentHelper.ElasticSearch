﻿using Elastic.Clients.Elasticsearch;
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

        protected void SetBaseIndexName(string baseIndexName)
        {
            BaseIndexName = baseIndexName;
        }

        protected void SetIndexCalculator(IElasticIndexCalculator<TEntity> indexCalculator)
        {
            IndexCalculator = indexCalculator;
        }

        public void SetBasicIndexCalculator(Action<IBasicIndexCalculator<TEntity>>? basicIndexCalculator = null)
        {
            var indexCalculator = BasicIndexCalculator<TEntity>.Create();
            if (basicIndexCalculator != null)
                basicIndexCalculator(indexCalculator);

            IndexCalculator = indexCalculator;
        }

        protected void SetCustomIndexCalculator<TFilter>(Action<ICustomIndexCalculator<TEntity, TFilter>> customIndexCalculator)
        {
            var indexCalculator = CustomIndexCalculator<TEntity, TFilter>.Create();
            customIndexCalculator(indexCalculator);

            IndexCalculator = indexCalculator;
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
