using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    public interface IElasticIndexCalculator
    {
        /// <summary>
        /// Get the index wildcard for index pattern usages
        /// </summary>
        /// <returns>The wildcard to be used</returns>
        string GetBaseIndexWildcard();

        /// <summary>
        /// Calculate all the postfix indexes to be added to the index calculation for the type to be used in queries
        /// </summary>
        /// <param name="baseObjectFilter">The filter for the calculation
        /// </param>
        /// <returns>All the possibile postfixes to be appended to the base index name</returns>
        IEnumerable<string> GetIndexPostfixByFilter(object? baseObjectFilter);
    }

    public interface IElasticIndexCalculator<in TEntity> : IElasticIndexCalculator where TEntity : class
    {
        /// <summary>
        /// Calculate the postfix to be added to index calculation for the type based on the current input
        /// </summary>
        /// <param name="input">the input to calculate the postfix index for</param>
        /// <returns>The postfixes to append to base index name</returns>
        string GetIndexPostfixByEntity(TEntity input);
    }
}
