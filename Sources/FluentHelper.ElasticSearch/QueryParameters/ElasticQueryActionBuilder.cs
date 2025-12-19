using Elastic.Clients.Elasticsearch.QueryDsl;
using System;
using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.QueryParameters
{
    public sealed class ElasticQueryActionBuilder<TEntity> where TEntity : class
    {
        private readonly List<Action<QueryDescriptor<TEntity>>> _queryActions = [];

        public ElasticQueryActionBuilder<TEntity> AddQuery(Action<QueryDescriptor<TEntity>> queryAction)
        {
            _queryActions.Add(queryAction);
            return this;
        }

        public QueryDescriptor<TEntity> BuildQueryDescriptor()
        {
            QueryDescriptor<TEntity> queryDescriptor = new();
            queryDescriptor.Bool(b => b.Must(_queryActions.ToArray()));
            return queryDescriptor;
        }
    }
}
