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
            Mappings(m =>
            {
                m.Keyword(p => p.Id);
                m.Text(p => p.Name);
                m.Date(p => p.CreationDate);
                m.Boolean(p => p.Active);
            });

            EnableTemplateCreation();
            Settings(s => s.NumberOfShards(1).NumberOfReplicas(0));
        }
    }
}
