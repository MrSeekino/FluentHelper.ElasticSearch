﻿using Elastic.Clients.Elasticsearch.Mapping;
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

            EnableTemplateCreation("secondentity_template");

            Settings(x => x.NumberOfShards(1));

            Prop<KeywordProperty>(e => e.Id);
            Prop<TextProperty>(e => e.Name);
        }
    }
}
