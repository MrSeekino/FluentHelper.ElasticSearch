using FluentHelper.ElasticSearch.Common;

namespace FluentHelper.ElasticSearch.TestsSupport
{
    public class TestSecondEntityMap : ElasticMap<TestSecondEntity>
    {
        public override void Map()
        {
            SetBaseIndexName("secondentity");

            SetBasicIndexCalculator();

            Id(e => e.Id);
        }
    }
}
