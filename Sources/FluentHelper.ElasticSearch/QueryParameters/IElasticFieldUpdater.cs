using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace FluentHelper.ElasticSearch.QueryParameters
{
    public interface IElasticFieldUpdater<TEntity>
    {
        List<string> GetFieldList();
        IElasticFieldUpdater<TEntity> Update<TProperty>(Expression<Func<TEntity, TProperty>> expression);
        IElasticFieldUpdater<TEntity> UpdateAllFields();
    }
}
