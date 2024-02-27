using FluentHelper.ElasticSearch.Examples.Models;

namespace FluentHelper.ElasticSearch.Examples.Repositories
{
    public interface ITestDataRepository
    {
        Task<IEnumerable<TestData>> GetAll();

        Task<TestData?> GetById(Guid id);

        Task Add(TestData data);

        Task Delete(TestData data);
    }
}
