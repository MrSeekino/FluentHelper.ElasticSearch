using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.QueryDsl;
using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.QueryParameters
{
    public interface IElasticQueryParameters<TEntity> where TEntity : class
    {
        QueryDescriptor<TEntity>? QueryDescriptor { get; }
        SourceConfig? SourceConfig { get; }
        SortOptionsDescriptor<TEntity>? SortOptionsDescriptor { get; }
        IDictionary<string, AggregationDescriptor<TEntity>>? AggregationDescriptors { get; }
        int Skip { get; }
        int Take { get; }
    }
}
