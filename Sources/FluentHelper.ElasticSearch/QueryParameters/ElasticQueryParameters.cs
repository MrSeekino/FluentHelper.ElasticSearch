﻿using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.QueryDsl;
using FluentHelper.ElasticSearch.Interfaces;

namespace FluentHelper.ElasticSearch.QueryParameters
{
    internal sealed class ElasticQueryParameters<TEntity> : IElasticQueryParameters<TEntity> where TEntity : class
    {
        public QueryDescriptor<TEntity>? QueryDescriptor { get; set; }
        public SourceConfig? SourceConfig { get; set; }
        public SortOptionsDescriptor<TEntity>? SortOptionsDescriptor { get; set; }
        public int Skip { get; set; }
        public int Take { get; set; }
    }
}
