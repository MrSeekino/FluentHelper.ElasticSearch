using Elastic.Clients.Elasticsearch.Mapping;
using FluentHelper.ElasticSearch.Common;

namespace FluentHelper.ElasticSearch.TestsSupport
{
    public class TestThirdEntityMap : ElasticMap<TestThirdEntity>
    {
        public override void Map()
        {
            SetBaseIndexName("thirdentity");

            SetBasicIndexCalculator();

            Id(e => e.Id);

            Settings(x => x.NumberOfShards(1));

            Prop<KeywordProperty>(e => e.Id);
            Prop<DateProperty>(e => e.Timestamp);
            Prop<DoubleNumberProperty>(e => e.Value);
        }
    }
}
