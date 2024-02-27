using FluentHelper.ElasticSearch.Examples.Models;
using FluentHelper.ElasticSearch.Examples.Repositories;
using Microsoft.Extensions.Hosting;

namespace FluentHelper.ElasticSearch.Examples.Runner
{
    public class ExampleService : BackgroundService
    {
        private readonly ITestDataRepository _testDataRepository;

        TestData _exampleData;

        public ExampleService(ITestDataRepository testDataRepository)
        {
            _testDataRepository = testDataRepository;

            _exampleData = new TestData
            {
                Id = Guid.NewGuid(),
                Name = "ExampleData",
                CreationDate = DateTime.UtcNow,
                Active = true
            };
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var testDataList = await _testDataRepository.GetAll();
                    Console.WriteLine($"Index contains {testDataList.Count()} documents");

                    Console.WriteLine($"Adding 1 row..");
                    PressToContinue();

                    await _testDataRepository.Add(_exampleData);

                    Console.WriteLine($"Waiting 5s to ensure ES index refreshed");
                    await Task.Delay(5000, stoppingToken);

                    testDataList = await _testDataRepository.GetAll();
                    Console.WriteLine($"Index contains {testDataList.Count()} documents");
                    Console.WriteLine($"Getting single document with id {_exampleData.Id}..");
                    PressToContinue();

                    var singleData = await _testDataRepository.GetById(_exampleData.Id);
                    if (singleData != null)
                        Console.WriteLine($"Got document with id {singleData.Id} and name {singleData.Name}, created on {singleData.CreationDate:yyyy-MM-dd HH:mm:ss}");
                    else
                        Console.WriteLine($"Could not retrieve document");

                    Console.WriteLine($"Deleting single document with id {_exampleData.Id}..");
                    PressToContinue();

                    await _testDataRepository.Delete(_exampleData);

                    Console.WriteLine($"Waiting 5s to ensure ES index refreshed");
                    await Task.Delay(5000, stoppingToken);

                    testDataList = await _testDataRepository.GetAll();
                    Console.WriteLine($"Index contains {testDataList.Count()} documents");
                    Console.WriteLine($"Deleting remaining documents..");
                    PressToContinue();

                    foreach (var testData in testDataList)
                        await _testDataRepository.Delete(testData);

                    Console.WriteLine($"Waiting 5s to ensure ES index refreshed");
                    await Task.Delay(5000, stoppingToken);

                    testDataList = await _testDataRepository.GetAll();
                    Console.WriteLine($"Index contains {testDataList.Count()} documents");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                finally
                {
                    Console.WriteLine($"ExampleService finished work. You can close the application");
                    await Task.Delay(Timeout.Infinite, stoppingToken);
                }
            }
        }

        private void PressToContinue()
        {
            Console.WriteLine("Enter any key to continue..");
            Console.ReadLine();
        }
    }
}
