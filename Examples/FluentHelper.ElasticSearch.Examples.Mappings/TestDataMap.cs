using Elastic.Clients.Elasticsearch.Mapping;
using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Examples.Models;

namespace FluentHelper.ElasticSearch.Examples.Mappings
{
    public class TestDataMap : ElasticMap<TestData>
    {
        public override void Map()
        {
            //EnableTemplateCreation();

            SetBaseIndexName("testdata");

            SetBasicIndexCalculator(x => x.WithFixedIndexName());

            Id(e => e.Id);

            Prop<KeywordProperty>(e => e.Id);
            Prop<TextProperty>(e => e.Name);
            Prop<DateProperty>(e => e.CreationDate);
            Prop<BooleanProperty>(e => e.Active);

            Settings(s =>
            {
                s.NumberOfShards(1).NumberOfReplicas(0);
                //s.Lifecycle(x => x.Name("OlderThan1Month"));
            });
        }
    }
}
