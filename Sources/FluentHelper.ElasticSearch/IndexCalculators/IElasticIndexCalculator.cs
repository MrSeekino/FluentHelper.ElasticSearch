using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    public interface IElasticIndexCalculator<TEntity>
    {
        string GetIndexPostfixByEntity(TEntity input);

        IEnumerable<string> GetIndexPostfixByFilter(object? baseObjectFilter);
    }
}
