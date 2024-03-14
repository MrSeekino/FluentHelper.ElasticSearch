using Elastic.Clients.Elasticsearch;
using FluentHelper.ElasticSearch.IndexCalculators;
using System;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]
namespace FluentHelper.ElasticSearch.Interfaces
{
    internal interface IElasticMap<TEntity> : IElasticMap
    {
        IElasticIndexCalculator<TEntity>? IndexCalculator { get; }
    }

    internal interface IElasticMap
    {
        string BaseIndexName { get; }
        string IdPropertyName { get; }

        Type GetMappingType();

        void Map();

        void ApplyMapping(ElasticsearchClientSettings esSettings);

        void Verify();
    }
}
