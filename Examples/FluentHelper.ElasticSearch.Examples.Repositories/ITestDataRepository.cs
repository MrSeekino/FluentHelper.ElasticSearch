using FluentHelper.ElasticSearch.Examples.Models;

namespace FluentHelper.ElasticSearch.Examples.Repositories
{
    public interface ITestDataRepository
    {
        Task<IEnumerable<TestData>> GetAll();

        Task<TestData?> GetById(Guid id);

        void Add(TestData data);
        Task AddAsync(TestData data);

        Task Delete(TestData data);

        Task<long> Count();

        Task Update(TestData data);

        Task BulkAdd(IEnumerable<TestData> dataList);

        Task<TestData?> GetByIdWithoutCreationTimeAndActive(Guid id);

        Task<IEnumerable<TestData>> GetAllSortedByCreationDateDesc();
    }
}
