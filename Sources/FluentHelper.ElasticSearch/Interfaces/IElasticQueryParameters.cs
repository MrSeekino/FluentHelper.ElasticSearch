using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.QueryDsl;

namespace FluentHelper.ElasticSearch.Interfaces
{
    public interface IElasticQueryParameters<TEntity> where TEntity : class
    {
        QueryDescriptor<TEntity>? QueryDescriptor { get; }
        SourceConfig? SourceConfig { get; }
        SortOptionsDescriptor<TEntity>? SortOptionsDescriptor { get; }
        int Skip { get; }
        int Take { get; }
    }
}
