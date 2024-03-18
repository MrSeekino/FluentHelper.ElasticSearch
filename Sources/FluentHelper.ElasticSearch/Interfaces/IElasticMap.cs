using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using FluentHelper.ElasticSearch.IndexCalculators;
using System;

namespace FluentHelper.ElasticSearch.Interfaces
{
    public interface IElasticMap
    {
        string BaseIndexName { get; }
        string IdPropertyName { get; }

        IElasticIndexCalculator? IndexCalculator { get; }

        bool CreateTemplate { get; }
        string TemplateName { get; }

        IndexSettingsDescriptor? IndexSettings { get; }
        Properties? IndexMappings { get; }

        Type GetMappingType();

        void Map();

        void ApplyMapping(ElasticsearchClientSettings esSettings);

        void Verify();
    }
}
