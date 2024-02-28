using Elastic.Clients.Elasticsearch;
using System;

namespace FluentHelper.ElasticSearch.Interfaces
{
    internal interface IElasticMap
    {
        Type GetMapType();

        void Map();

        void ApplySpecialMap(ElasticsearchClientSettings esSettings);

        void Verify();
    }
}
