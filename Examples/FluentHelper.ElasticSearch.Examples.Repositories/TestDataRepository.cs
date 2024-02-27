using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Examples.Models;
using FluentHelper.ElasticSearch.Interfaces;
using Nest;

namespace FluentHelper.ElasticSearch.Examples.Repositories
{
    public class TestDataRepository : ITestDataRepository
    {
        private readonly IElasticWrapper _elasticWrapper;

        public TestDataRepository(IElasticWrapper elasticWrapper)
        {
            _elasticWrapper = elasticWrapper;
        }

        public async Task Add(TestData data)
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

        public async Task<TestData?> GetById(Guid id)
        {
            QueryContainer qContainer = new QueryContainer();
            var qDescriptor = new QueryContainerDescriptor<TestData>();

            qContainer = qContainer && qDescriptor.Match(x => x.Field(f => f.Id).Query(id.ToString()));

            var qParams = new ElasticQueryParameters<TestData>(qContainer);

            var dataList = await _elasticWrapper.QueryAsync(null, qParams);
            return dataList.SingleOrDefault();
        }
    }
}
