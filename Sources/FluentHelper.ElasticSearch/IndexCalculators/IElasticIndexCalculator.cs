using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    public interface IElasticIndexCalculator<TEntity>
    {
        string GetBaseIndexWildcard();

        /// <summary>
        /// Calculate the postfix to be added to index calculation for the type based on the current input
        /// </summary>
        /// <param name="input">the input to calculate the postfix index for</param>
        /// <returns></returns>
        string GetIndexPostfixByEntity(TEntity input);

        /// <summary>
        /// Calculate all the postfix indexes to be added to the index calculation for the type to be used in queries
        /// </summary>
        /// <param name="baseObjectFilter">The filter for the calculation
        /// </param>
        /// <returns></returns>
        IEnumerable<string> GetIndexPostfixByFilter(object? baseObjectFilter);
    }
}
