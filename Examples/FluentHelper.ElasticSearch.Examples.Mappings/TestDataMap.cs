using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Examples.Models;
using FluentHelper.ElasticSearch.IndexCalculators;

namespace FluentHelper.ElasticSearch.Examples.Mappings
{
    public class TestDataMap : ElasticMap<TestData>
    {
        public override void Map()
        {
            SetBaseIndexName("testdata");

            SetIndexCalculator(new BasicIndexCalculator<TestData>(true));

            Id(e => e.Id);
        }
    }
}
