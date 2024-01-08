using Nest;

namespace FluentHelper.ElasticSearch.Common
{
    public sealed class ElasticQueryParameters<TEntity> where TEntity : class
    {
        public QueryContainer Query { get; set; }
        public SourceFilterDescriptor<TEntity>? SourceFilter { get; set; }
        public SortDescriptor<TEntity>? Sort { get; set; }

        public int Skip { get; set; }
        public int Take { get; set; }

        public ElasticQueryParameters(QueryContainer query)
            : this(query, null, null, 0, 10000) { }

        public ElasticQueryParameters(QueryContainer query, SortDescriptor<TEntity> sort)
            : this(query, null, sort, 0, 10000) { }

        public ElasticQueryParameters(QueryContainer query, int skip, int take)
            : this(query, null, null, skip, take) { }

        public ElasticQueryParameters(QueryContainer query, SortDescriptor<TEntity> sort, int skip, int take)
            : this(query, null, sort, skip, take) { }

        public ElasticQueryParameters(QueryContainer query, SourceFilterDescriptor<TEntity>? sourceFilter)
            : this(query, sourceFilter, null, 0, 10000) { }

        public ElasticQueryParameters(QueryContainer query, SourceFilterDescriptor<TEntity>? sourceFilter, int skip, int take)
            : this(query, sourceFilter, null, skip, take) { }

        public ElasticQueryParameters(QueryContainer query, SourceFilterDescriptor<TEntity>? sourceFilter, SortDescriptor<TEntity>? sort, int skip, int take)
        {
            Query = query;
            SourceFilter = sourceFilter;
            Sort = sort;

            Skip = skip;
            Take = take;
        }
    }
}
