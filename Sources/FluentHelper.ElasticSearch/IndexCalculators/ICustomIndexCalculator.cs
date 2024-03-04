using System;
using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    public interface ICustomIndexCalculator<T, TFilter> : IElasticIndexCalculator<T>
    {
        void WithPostfixByEntity(Func<T, string> getIndexPostfixByEntity);
        void WithPostfixByFilter(Func<TFilter?, IEnumerable<string>?> getIndexPostfixByFilter);
    }
}
