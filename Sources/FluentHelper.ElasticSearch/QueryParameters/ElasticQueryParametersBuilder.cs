using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.QueryDsl;
using FluentHelper.ElasticSearch.Interfaces;
using System;
using System.Linq.Expressions;

namespace FluentHelper.ElasticSearch.QueryParameters
{
    public sealed class ElasticQueryParametersBuilder<TEntity> where TEntity : class
    {
        private QueryDescriptor<TEntity>? _queryDescriptor;
        private SourceFilter? _sourceFilter;
        private SortOptionsDescriptor<TEntity>? _sortOptionsDescriptor;
        private int _skip;
        private int _take;

        public static ElasticQueryParametersBuilder<TEntity> Create()
        {
            return new ElasticQueryParametersBuilder<TEntity>()
            {
                _queryDescriptor = null,
                _sourceFilter = null,
                _sortOptionsDescriptor = null,
                _skip = 0,
                _take = 10000,
            };
        }

        public ElasticQueryParametersBuilder<TEntity> Query(Action<QueryDescriptor<TEntity>> queryAction)
        {
            QueryDescriptor<TEntity> queryDescriptor = new();
            queryAction(queryDescriptor);

            _queryDescriptor = queryDescriptor;
            return this;
        }

        public ElasticQueryParametersBuilder<TEntity> SourceFilter(SourceFilter sourceFilter)
        {
            _sourceFilter = sourceFilter;
            return this;
        }

        public ElasticQueryParametersBuilder<TEntity> Exclude(Expression<Func<TEntity, object>> field)
        {
            _sourceFilter ??= new SourceFilter();

            if (_sourceFilter!.Excludes is null)
                _sourceFilter.Excludes = field;
            else
                _sourceFilter.Excludes = _sourceFilter.Excludes.And(field);

            return this;
        }

        public ElasticQueryParametersBuilder<TEntity> Include(Expression<Func<TEntity, object>> field)
        {
            _sourceFilter ??= new SourceFilter();

            if (_sourceFilter!.Includes is null)
                _sourceFilter.Includes = field;
            else
                _sourceFilter.Includes = _sourceFilter.Includes.And(field);

            return this;
        }

        public ElasticQueryParametersBuilder<TEntity> Sort(Action<SortOptionsDescriptor<TEntity>> sortAction)
        {
            SortOptionsDescriptor<TEntity> sortOptionsDescriptor = new();
            sortAction(sortOptionsDescriptor);

            _sortOptionsDescriptor = sortOptionsDescriptor;
            return this;
        }

        public ElasticQueryParametersBuilder<TEntity> Sort(Expression<Func<TEntity, object>> field, SortOrder sortOrder)
        {
            SortOptionsDescriptor<TEntity> sortOptionsDescriptor = new SortOptionsDescriptor<TEntity>().Field(field, new FieldSort
            {
                Order = sortOrder
            });

            _sortOptionsDescriptor = sortOptionsDescriptor;
            return this;
        }

        public ElasticQueryParametersBuilder<TEntity> Skip(int skipValue)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(skipValue);

            _skip = skipValue;
            return this;
        }

        public ElasticQueryParametersBuilder<TEntity> Take(int takeValue)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(1, takeValue);

            _take = takeValue;
            return this;
        }

        public IElasticQueryParameters<TEntity> Build()
        {
            return new ElasticQueryParameters<TEntity>
            {
                QueryDescriptor = _queryDescriptor,
                SourceConfig = _sourceFilter != null ? new SourceConfig(_sourceFilter) : null,
                SortOptionsDescriptor = _sortOptionsDescriptor,
                Skip = _skip,
                Take = _take
            };
        }
    }
}
