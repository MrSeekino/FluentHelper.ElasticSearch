using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using FluentHelper.ElasticSearch.Examples.Models;
using FluentHelper.ElasticSearch.Interfaces;
using FluentHelper.ElasticSearch.QueryParameters;

namespace FluentHelper.ElasticSearch.Examples.Repositories
{
    public class TestDataRepository : ITestDataRepository
    {
        private readonly IElasticWrapper _elasticWrapper;

        public TestDataRepository(IElasticWrapper elasticWrapper)
        {
            _elasticWrapper = elasticWrapper;
        }

        public void Add(TestData data)
        {
            _elasticWrapper.Add(data);
        }

        public async Task AddAsync(TestData data)
        {
            await _elasticWrapper.AddAsync(data);
        }

        public async Task Delete(TestData data)
        {
            await _elasticWrapper.DeleteAsync(data);
        }

        public async Task<IEnumerable<TestData>> GetAll()
        {
            return await _elasticWrapper.QueryAsync<TestData>(null, null);
        }

        public async Task<IEnumerable<TestData>> GetAllSortedByCreationDateDesc()
        {
            var qBuilder = ElasticQueryParametersBuilder<TestData>.Create()
                                .Sort(x => x.CreationDate, SortOrder.Desc);

            var qParams = qBuilder.Build();
            return await _elasticWrapper.QueryAsync(null, qParams);
        }

        public async Task<IEnumerable<TestData>> GetAllActiveFromDate(DateTime minDate)
        {
            var qBuilder = ElasticQueryParametersBuilder<TestData>.Create()
                                .Query(q =>
                                {
                                    q.AddQuery(x => x.Term(m => m.Field(f => f.Active).Value(true)))
                                        .AddQuery(x => x.Range(r => r.Date(x => x.Field(f => f.CreationDate).Gt(minDate))));
                                })
                                .Sort(x => x.CreationDate, SortOrder.Desc);

            var qParams = qBuilder.Build();
            return await _elasticWrapper.QueryAsync(null, qParams);
        }

        public async Task<AggregateDictionary?> GetDataGroupedByDay()
        {
            var qBuilder = ElasticQueryParametersBuilder<TestData>.Create()
                                .AddAggregation("group_by_day",
                                                x => x.DateHistogram(dh => dh
                                                        .Field(f => f.CreationDate)
                                                        .CalendarInterval(CalendarInterval.Day)
                                                        .Format("yyyy-MM-dd")
                                                        .MinDocCount(0)
                                ));

            var qParams = qBuilder.Build();
            return await _elasticWrapper.AggregateAsync(null, qParams);
        }

        public async Task<TestData?> GetById(Guid id)
        {
            var qBuilder = ElasticQueryParametersBuilder<TestData>.Create()
                               .Query(x => x.Match(x => x.Field(f => f.Id)).SimpleQueryString(c => c.Query(id.ToString())))
                               .Skip(0)
                               .Take(1);

            var qParams = qBuilder.Build();
            var dataList = await _elasticWrapper.QueryAsync(null, qParams);

            return dataList.SingleOrDefault();
        }

        public async Task<TestData?> GetByIdWithoutCreationTimeAndActive(Guid id)
        {
            var qBuilder = ElasticQueryParametersBuilder<TestData>.Create()
                .Query(q => q.Match(x => x.Field(f => f.Id)).SimpleQueryString(c => c.Query(id.ToString())))
                .Skip(0)
                .Take(1)
                .Exclude(x => x.CreationDate)
                .Exclude(x => x.Active);

            var qParams = qBuilder.Build();
            var dataList = await _elasticWrapper.QueryAsync(null, qParams);

            return dataList.SingleOrDefault();
        }

        public async Task<long> Count()
        {
            return await _elasticWrapper.CountAsync<TestData>(null, null);
        }

        public async Task Update(TestData data)
        {
            await _elasticWrapper.AddOrUpdateAsync(data, x => x.Update(f => f.Active).Update(f => f.Name), 1);
        }

        public async Task BulkAdd(IEnumerable<TestData> dataList)
        {
            await _elasticWrapper.BulkAddAsync(dataList);
        }
    }
}
