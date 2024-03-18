using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using FluentHelper.ElasticSearch.IndexCalculators;
using FluentHelper.ElasticSearch.Interfaces;
using System;
using System.Linq.Expressions;

namespace FluentHelper.ElasticSearch.Common
{
    public abstract class ElasticMap<TEntity> : IElasticMap where TEntity : class
    {
        public virtual string BaseIndexName { get; private set; }
        public virtual IElasticIndexCalculator? IndexCalculator { get; private set; }
        public virtual string IdPropertyName { get; private set; }

        public virtual bool CreateTemplate { get; private set; }
        public virtual string TemplateName { get; private set; }

        public virtual IndexSettingsDescriptor? IndexSettings { get; private set; }
        public virtual Properties? IndexMappings { get; private set; }

        protected ElasticMap()
        {
            BaseIndexName = string.Empty;
            IndexCalculator = null;
            IdPropertyName = string.Empty;

            CreateTemplate = false;
            TemplateName = string.Empty;

            IndexSettings = null;
            IndexMappings = null;
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
        protected void SetBasicIndexCalculator(Action<IBasicIndexCalculator<TEntity>>? basicIndexCalculator = null)
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
        /// <param name="filterableIndexCalculator">the action to configure the custom calculator</param>
        protected void SetFilterableIndexCalculator<TFilter>(Action<IFilterableIndexCalculator<TEntity, TFilter>> filterableIndexCalculator)
        {
            var indexCalculator = FilterableIndexCalculator<TEntity, TFilter>.Create();
            filterableIndexCalculator(indexCalculator);

            IndexCalculator = indexCalculator;
        }

        /// <summary>
        /// Set the field considered as the Id in elasticsearch
        /// </summary>
        /// <param name="expression">the field to be the Id</param>
        protected void Id<P>(Expression<Func<TEntity, P>> expression)
        {
            IdPropertyName = ((MemberExpression)expression.Body).Member.Name;
        }

        /// <summary>
        /// Enable the automatic creation of templates when indexing data. If enabled and template is not available, indexes created will end up without mapping.
        /// </summary>
        /// <param name="templateName">The name of the template. If not specified it will be automatically deducted from defined mappings</param>
        protected void EnableTemplateCreation(string templateName = "")
        {
            CreateTemplate = true;
            TemplateName = templateName;
        }

        /// <summary>
        /// Set settings for indexes and template when creating new indexes
        /// </summary>
        /// <param name="settings">The settings to be applied to the index and/or index template</param>
        protected void Settings(Action<IndexSettingsDescriptor> settings)
        {
            IndexSettingsDescriptor settingsDescriptor = new IndexSettingsDescriptor();
            settings(settingsDescriptor);

            IndexSettings = settingsDescriptor;
        }

        /// <summary>
        /// Set property mappings when creating new indexes and templates
        /// </summary>
        /// <param name="mappings">The ammpings to be applied to the index and/or index template</param>
        public void Prop<PropertyType>(Expression<Func<TEntity, object>> expression) where PropertyType : IProperty
        {
            IndexMappings ??= new Properties();

            IProperty typeInstance = Activator.CreateInstance<PropertyType>();
            IndexMappings!.Add(expression, typeInstance);
        }

        /// <summary>
        /// Get the current true type of the mapping
        /// </summary>
        /// <returns>The type of the mapped entity</returns>
        public Type GetMappingType()
        {
            return typeof(TEntity);
        }

        /// <summary>
        /// Apply the map to the ElasticsearchClient settings. Automatically used when building the wrapper
        /// </summary>
        /// <param name="esSettings">The current elasticsearchclient settings</param>
        public void ApplyMapping(ElasticsearchClientSettings esSettings)
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

        /// <summary>
        /// Implement the method to set the needed mappings
        /// </summary>
        public abstract void Map();
    }
}
