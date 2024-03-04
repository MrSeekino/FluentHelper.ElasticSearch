using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    public interface IElasticIndexCalculator<TEntity>
    {
        string CalcEntityIndex(TEntity input);

        IEnumerable<string> CalcQueryIndex(object? baseObjectFilter);
    }
}
