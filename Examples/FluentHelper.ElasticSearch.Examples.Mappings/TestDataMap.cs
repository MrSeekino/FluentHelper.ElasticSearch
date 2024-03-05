using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Examples.Models;

namespace FluentHelper.ElasticSearch.Examples.Mappings
{
    public class TestDataMap : ElasticMap<TestData>
    {
        public override void Map()
        {
            SetBaseIndexName("testdata");

            SetBasicIndexCalculator(x => x.WithFixedIndexName());

            Id(e => e.Id);
        }
    }
}
