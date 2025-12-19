using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.Fluent;
using Elastic.Clients.Elasticsearch.QueryDsl;

namespace FluentHelper.ElasticSearch.QueryParameters
{
    public interface IElasticQueryParameters<TEntity> where TEntity : class
    {
        QueryDescriptor<TEntity>? QueryDescriptor { get; }
        SourceConfig? SourceConfig { get; }
        SortOptionsDescriptor<TEntity>? SortOptionsDescriptor { get; }
        FluentDescriptorDictionary<string, AggregationDescriptor<TEntity>>? AggregationDescriptors { get; }
        int Skip { get; }
        int Take { get; }
    }
}
