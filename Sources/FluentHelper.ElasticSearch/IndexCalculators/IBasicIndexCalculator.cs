namespace FluentHelper.ElasticSearch.IndexCalculators
{
    public interface IBasicIndexCalculator<T> : IElasticIndexCalculator<T>
    {
        /// <summary>
        /// Specify that the index name is fixed. When not fixed a wildcard "*" is always applied when building indexes to be searched on
        /// </summary>
        /// <returns></returns>
        IBasicIndexCalculator<T> WithFixedIndexName();
    }
}
