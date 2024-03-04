namespace FluentHelper.ElasticSearch.IndexCalculators
{
    public interface IBasicIndexCalculator<T> : IElasticIndexCalculator<T>
    {
        IBasicIndexCalculator<T> WithFixedIndexName();
    }
}
