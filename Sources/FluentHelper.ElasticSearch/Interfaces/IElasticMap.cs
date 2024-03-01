using Elastic.Clients.Elasticsearch;
using System;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]
namespace FluentHelper.ElasticSearch.Interfaces
{
    internal interface IElasticMap
    {
        string IdPropertyName { get; }

        Type GetMapType();

        void Map();

        void ApplySpecialMap(ElasticsearchClientSettings esSettings);

        void Verify();
    }
}
