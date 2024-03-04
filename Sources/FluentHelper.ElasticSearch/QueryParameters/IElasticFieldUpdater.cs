using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace FluentHelper.ElasticSearch.QueryParameters
{
    public interface IElasticFieldUpdater<TEntity>
    {
        /// <summary>
        /// Get the current list of field that will be updated
        /// </summary>
        /// <returns></returns>
        List<string> GetFieldList();
        /// <summary>
        /// Set a specific field to be updated. Can be called multiple times to specify multiple fields
        /// </summary>
        /// <typeparam name="TProperty">the type of property to be updated</typeparam>
        /// <param name="expression">field to be updated</param>
        /// <returns></returns>
        IElasticFieldUpdater<TEntity> Update<TProperty>(Expression<Func<TEntity, TProperty>> expression);
        /// <summary>
        /// Set all the fields to be updated
        /// </summary>
        /// <returns></returns>
        IElasticFieldUpdater<TEntity> UpdateAllFields();
    }
}
