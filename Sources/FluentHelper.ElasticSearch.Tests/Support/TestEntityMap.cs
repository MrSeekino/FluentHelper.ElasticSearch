using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.IndexCalculators;

namespace FluentHelper.ElasticSearch.Tests.Support
{
    public class TestEntityMap : ElasticMap<TestEntity>
    {
        public override void Map()
        {
            SetBaseIndexName("testentity");

            SetIndexCalculator(new CustomIndexCalculator<TestEntity, TestFilter>(x => $"{x.GroupName}-{x.CreationTime:yyyy.MM.dd}", filter =>
            {
                if (filter == null)
                    return null;

                if (filter != null && filter.StartTime.HasValue && filter.EndTime.HasValue)
                {
                    List<string> indexNames = new();

                    int daysDelay = (int)(filter.EndTime.Value - filter.StartTime.Value).TotalDays + 1;
                    for (int i = 0; i < daysDelay; i++)
                        indexNames.Add($"{filter.GroupName}-{filter!.StartTime.Value.AddDays(i):yyyy.MM.dd}");

                    return indexNames;
                }

                return null;
            }));

            Id(e => e.Id);
        }
    }
}
