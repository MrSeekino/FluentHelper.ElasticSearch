using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using FluentHelper.ElasticSearch.IndexCalculators;
using System;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]
namespace FluentHelper.ElasticSearch.Interfaces
{
    internal interface IElasticMap<TEntity> : IElasticMap
    {
        IElasticIndexCalculator<TEntity>? IndexCalculator { get; }
        PropertiesDescriptor<TEntity>? IndexMappings { get; }
    }

    internal interface IElasticMap
    {
        string BaseIndexName { get; }
        string IdPropertyName { get; }

        bool CreateTemplate { get; }
        IndexSettingsDescriptor? IndexSettings { get; }

        Type GetMappingType();

        void Map();

        void ApplyMapping(ElasticsearchClientSettings esSettings);

        void Verify();
    }
}
