using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.QueryDsl;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace FluentHelper.ElasticSearch.QueryParameters
{
    public sealed class ElasticQueryParametersBuilder<TEntity> where TEntity : class
    {
        private QueryDescriptor<TEntity>? _queryDescriptor;
        private SourceFilter? _sourceFilter;
        private SortOptionsDescriptor<TEntity>? _sortOptionsDescriptor;
        private Dictionary<string, AggregationDescriptor<TEntity>> _aggregationDescriptors = [];
        private int _skip;
        private int _take;

        /// <summary>
        /// Create default query parameters
        /// </summary>
        /// <returns></returns>
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

        /// <summary>
        /// Add QueryDescriptor
        /// </summary>
        /// <param name="queryAction">Configuration to apply to QueryDescriptor</param>
        /// <returns></returns>
        public ElasticQueryParametersBuilder<TEntity> Query(Action<QueryDescriptor<TEntity>> queryAction)
        {
            QueryDescriptor<TEntity> queryDescriptor = new();
            queryAction(queryDescriptor);

            _queryDescriptor = queryDescriptor;
            return this;
        }

        /// <summary>
        /// Add QueryDescriptor using builder allowing to add multiple descriptor as bool query
        /// </summary>
        /// <param name="queryAction">Configuration to apply to QueryActionBuilder</param>
        /// <returns></returns>
        public ElasticQueryParametersBuilder<TEntity> Query(Action<ElasticQueryActionBuilder<TEntity>> queryAction)
        {
            ElasticQueryActionBuilder<TEntity> actionBuilder = new();
            queryAction(actionBuilder);

            _queryDescriptor = actionBuilder.BuildQueryDescriptor();
            return this;
        }

        /// <summary>
        /// Specify a custom SourceFilter to be used
        /// </summary>
        /// <param name="sourceFilter">The SourceFilter intended to be used</param>
        /// <returns></returns>
        public ElasticQueryParametersBuilder<TEntity> SourceFilter(SourceFilter sourceFilter)
        {
            _sourceFilter = sourceFilter;
            return this;
        }

        /// <summary>
        /// Exclude a field from returning values. Can be called multiple times to exclude multiple fields
        /// </summary>
        /// <param name="field">Field to be excluded</param>
        /// <returns></returns>
        public ElasticQueryParametersBuilder<TEntity> Exclude(Expression<Func<TEntity, object>> field)
        {
            _sourceFilter ??= new SourceFilter();

            if (_sourceFilter!.Excludes is null)
                _sourceFilter.Excludes = field;
            else
                _sourceFilter.Excludes = _sourceFilter.Excludes.And(field);

            return this;
        }

        /// <summary>
        /// Include a specific field to the returning values. Can be called multiple times to include multiple fields
        /// </summary>
        /// <param name="field">Field to be included</param>
        /// <returns></returns>
        public ElasticQueryParametersBuilder<TEntity> Include(Expression<Func<TEntity, object>> field)
        {
            _sourceFilter ??= new SourceFilter();

            if (_sourceFilter!.Includes is null)
                _sourceFilter.Includes = field;
            else
                _sourceFilter.Includes = _sourceFilter.Includes.And(field);

            return this;
        }

        /// <summary>
        /// Sort the result using SortOptionsDescriptor
        /// </summary>
        /// <param name="sortAction">Configuration to be applied to SortOptionsDescriptor</param>
        /// <returns></returns>
        public ElasticQueryParametersBuilder<TEntity> Sort(Action<SortOptionsDescriptor<TEntity>> sortAction)
        {
            SortOptionsDescriptor<TEntity> sortOptionsDescriptor = new();
            sortAction(sortOptionsDescriptor);

            _sortOptionsDescriptor = sortOptionsDescriptor;
            return this;
        }

        /// <summary>
        /// Sort the result on the specified field with the selected sort order
        /// </summary>
        /// <param name="field">The field to sort on</param>
        /// <param name="sortOrder">The order to sort</param>
        /// <returns></returns>
        public ElasticQueryParametersBuilder<TEntity> Sort(Expression<Func<TEntity, object>> field, SortOrder sortOrder)
        {
            SortOptionsDescriptor<TEntity> sortOptionsDescriptor = new SortOptionsDescriptor<TEntity>().Field(new FieldSort(field)
            {
                Order = sortOrder
            });

            _sortOptionsDescriptor = sortOptionsDescriptor;
            return this;
        }

        /// <summary>
        /// Add an aggregation descriptor with the specified key
        /// </summary>
        /// <param name="aggregatorKey">The key for the aggregator</param>
        /// <param name="aggregationDescriptor">The implementation of the aggregator descriptor</param>
        /// <returns></returns>
        public ElasticQueryParametersBuilder<TEntity> AddAggregation(string aggregatorKey, AggregationDescriptor<TEntity> aggregationDescriptor)
        {
            _aggregationDescriptors.Add(aggregatorKey, aggregationDescriptor);
            return this;
        }

        /// <summary>
        /// Skip the selected number of result. Throws if negative
        /// </summary>
        /// <param name="skipValue">number of elements to be skipped</param>
        /// <returns></returns>
        public ElasticQueryParametersBuilder<TEntity> Skip(int skipValue)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(skipValue);

            _skip = skipValue;
            return this;
        }

        /// <summary>
        /// Take the selected amount of result. Throws if less than 1
        /// </summary>
        /// <param name="takeValue">number of elements to be returned</param>
        /// <returns></returns>
        public ElasticQueryParametersBuilder<TEntity> Take(int takeValue)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(takeValue, 1);

            _take = takeValue;
            return this;
        }

        /// <summary>
        /// Build the query parameters
        /// </summary>
        /// <returns></returns>
        public IElasticQueryParameters<TEntity> Build()
        {
            return new ElasticQueryParameters<TEntity>
            {
                QueryDescriptor = _queryDescriptor,
                SourceConfig = _sourceFilter != null ? new SourceConfig(_sourceFilter) : null,
                SortOptionsDescriptor = _sortOptionsDescriptor,
                AggregationDescriptors = _aggregationDescriptors,
                Skip = _skip,
                Take = _take
            };
        }
    }
}
