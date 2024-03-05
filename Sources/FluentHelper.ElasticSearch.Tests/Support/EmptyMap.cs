using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.IndexCalculators;
using System.Linq.Expressions;

namespace FluentHelper.ElasticSearch.Tests.Support
{
    public class EmptyEntity
    {
        public required string Name { get; set; }
        public required string Description { get; set; }
    }

    public class EmptyMap : ElasticMap<EmptyEntity>
    {
        public override void Map()
        { }

        public void TestSetBaseIndexName(string baseIndexName)
            => SetBaseIndexName(baseIndexName);

        public void TestSetIndexCalculator(IElasticIndexCalculator<EmptyEntity> indexCalculator)
            => SetIndexCalculator(indexCalculator);

        public void TestSetBasicIndexCalculator(Action<IBasicIndexCalculator<EmptyEntity>>? basicIndexCalculator = null)
            => SetBasicIndexCalculator(basicIndexCalculator);

        public void TestSetCustomIndexCalculator<TFilter>(Action<ICustomIndexCalculator<EmptyEntity, TFilter>> customIndexCalculator)
            => SetCustomIndexCalculator(customIndexCalculator);

        public void TestId<P>(Expression<Func<EmptyEntity, P>> expression)
            => Id(expression);
    }
}
