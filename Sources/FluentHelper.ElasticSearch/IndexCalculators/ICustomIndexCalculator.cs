using System;
using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    public interface ICustomIndexCalculator<T, TFilter> : IElasticIndexCalculator<T>
    {
        /// <summary>
        /// Set the calculation function that return the postfix for the index when adding, deleting and updating
        /// </summary>
        /// <param name="getIndexPostfixByEntity"></param>
        void WithPostfixByEntity(Func<T, string> getIndexPostfixByEntity);
        /// <summary>
        /// Set the calculation function that return a set of postfixes for the index to be queries for query and count operations
        /// </summary>
        /// <param name="getIndexPostfixByFilter"></param>
        void WithPostfixByFilter(Func<TFilter?, IEnumerable<string>?> getIndexPostfixByFilter);
    }
}
