using FluentHelper.ElasticSearch.Common;

namespace FluentHelper.ElasticSearch.TestsSupport
{
    public class TestEntityMap : ElasticMap<TestEntity>
    {
        public override void Map()
        {
            SetBaseIndexName("testentity");

            SetFilterableIndexCalculator<TestFilter>(c =>
            {
                c.WithPostfixByEntity(x => $"{x.GroupName}-{x.CreationTime:yyyy.MM.dd}");

                c.WithPostfixByFilter(filter =>
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
                });
            });

            Id(e => e.Id);
        }
    }
}
