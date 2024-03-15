using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using System;

namespace FluentHelper.ElasticSearch.Common
{
    public class ElasticIndexTemplate<TEntity> where TEntity : class
    {
        public string TemplateName { get; private set; } = string.Empty;
        public IndexSettingsDescriptor? Settings { get; private set; }
        public PropertiesDescriptor<TEntity>? Mappings { get; private set; }

        public ElasticIndexTemplate<TEntity> WithTemplateName(string templateName)
        {
            TemplateName = templateName;
            return this;
        }

        public ElasticIndexTemplate<TEntity> WithSettings(Action<IndexSettingsDescriptor> settings)
        {
            IndexSettingsDescriptor settingsDescriptor = new IndexSettingsDescriptor();
            settings(settingsDescriptor);
            Settings = settingsDescriptor;

            return this;
        }

        public ElasticIndexTemplate<TEntity> WithMappings(Action<PropertiesDescriptor<TEntity>> mappings)
        {
            PropertiesDescriptor<TEntity> mappingDescriptor = new PropertiesDescriptor<TEntity>();
            mappings(mappingDescriptor);
            Mappings = mappingDescriptor;

            return this;
        }
    }
}
