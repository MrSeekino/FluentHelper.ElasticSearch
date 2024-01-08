using System.Linq.Expressions;

namespace FluentHelper.ElasticSearch.Interfaces
{
    public interface IElasticFieldUpdater<TEntity>
    {
        List<string> GetFieldList();
        IElasticFieldUpdater<TEntity> Update<TProperty>(Expression<Func<TEntity, TProperty>> expression);
        IElasticFieldUpdater<TEntity> UpdateAllFields();
    }
}
