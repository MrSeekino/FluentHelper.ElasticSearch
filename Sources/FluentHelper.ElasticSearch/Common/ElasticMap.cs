using FluentHelper.ElasticSearch.IndexCalculators;
using FluentHelper.ElasticSearch.Interfaces;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace FluentHelper.ElasticSearch.Common
{
    public abstract class ElasticMap<TEntity> : IElasticMap where TEntity : class
    {
        public string BaseIndexName { get; private set; } = typeof(TEntity).Name;
        public IElasticIndexCalculator<TEntity> IndexCalculator { get; private set; }

        public string IdPropertyName { get; private set; } = string.Empty;

        public List<Expression<Func<TEntity, object>>> IgnoreList { get; private set; } = new List<Expression<Func<TEntity, object>>>();
        public Dictionary<Expression<Func<TEntity, object>>, string> PropertyNameList { get; private set; } = new Dictionary<Expression<Func<TEntity, object>>, string>();

        public ElasticMap()
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

        protected void Ignore(Expression<Func<TEntity, object>> expression)
        {
            IgnoreList.Add(expression);
        }

        protected void Rename(Expression<Func<TEntity, object>> expression, string newName)
        {
            PropertyNameList.Add(expression, newName);
        }

        public Type GetMapType()
        {
            return typeof(TEntity);
        }

        public void ApplySpecialMap(ConnectionSettings esSettings)
        {
            esSettings.DefaultMappingFor<TEntity>(x =>
            {
                x.IdProperty(IdPropertyName);

                foreach (var toIgnore in IgnoreList)
                    x.Ignore(toIgnore);

                foreach (var propertyName in PropertyNameList)
                    x.PropertyName(propertyName.Key, propertyName.Value);

                return x;
            });
        }

        public void Verify()
        {
            if (string.IsNullOrWhiteSpace(IdPropertyName))
                throw new Exception($"IdProperty has not been set for {typeof(TEntity).Name}");
        }

        public abstract void Map();
    }
}
