using Elastic.Clients.Elasticsearch.Aggregations;
using FluentHelper.ElasticSearch.Examples.Models;
using FluentHelper.ElasticSearch.Examples.Repositories;
using FluentHelper.ElasticSearch.Interfaces;
using Microsoft.Extensions.Hosting;

namespace FluentHelper.ElasticSearch.Examples.Runner
{
    public class ExampleService : BackgroundService
    {
        const int _indexRefreshTimeMs = 5000;

        static bool _enablePressOnContinue = true;

        private readonly IElasticWrapper _elasticWrapper;
        private readonly ITestDataRepository _testDataRepository;

        private readonly TestData _exampleData;
        private readonly TestData _secondData;
        private readonly TestData _thirdData;

        public ExampleService(IElasticWrapper elasticWrapper, ITestDataRepository testDataRepository)
        {
            _elasticWrapper = elasticWrapper;
            _testDataRepository = testDataRepository;

            _exampleData = new TestData
            {
                Id = Guid.NewGuid(),
                Name = "ExampleData",
                CreationDate = DateTime.UtcNow.AddHours(-51),
                Active = true
            };

            _secondData = new TestData
            {
                Id = Guid.NewGuid(),
                Name = "SecondData",
                CreationDate = DateTime.UtcNow.AddMinutes(-3027),
                Active = false
            };

            _thirdData = new TestData
            {
                Id = Guid.NewGuid(),
                Name = "ThirdData",
                CreationDate = DateTime.UtcNow.AddMinutes(-1721),
                Active = true
            };
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    DateTime minDate = DateTime.UtcNow.Date.AddDays(-5);

                    Console.WriteLine("Enable press on continue? Y/n");
                    var key = Console.ReadLine();
                    if (!string.IsNullOrWhiteSpace(key) && key.ToLower() == "n")
                        _enablePressOnContinue = false;

                    var result = await _elasticWrapper.CreateAllMappedIndexTemplateAsync();
                    Console.WriteLine($"Template creation results: CreatedTemplates={result.CreatedTemplates}, AlreadyExistingTemplates={result.AlreadyExistingTemplates}, TotalDefinedTemplates={result.TotalDefinedTemplates}");

                    var testDataList = await _testDataRepository.GetAll();
                    Console.WriteLine($"Index contains {testDataList.Count()} documents");

                    Console.WriteLine($"Adding 1 row..");
                    PressToContinue();

                    _testDataRepository.Add(_exampleData);
                    await WaitIndexRefresh(stoppingToken);

                    testDataList = await _testDataRepository.GetAll();
                    Console.WriteLine($"Index contains {testDataList.Count()} documents");

                    Console.WriteLine($"Getting single document with id {_exampleData.Id}..");
                    PressToContinue();

                    var singleData = await _testDataRepository.GetById(_exampleData.Id);
                    if (singleData != null)
                        Console.WriteLine($"Got document with id {singleData.Id} and name {singleData.Name}, created on {singleData.CreationDate:yyyy-MM-dd HH:mm:ss}, active {singleData.Active}");
                    else
                        Console.WriteLine($"Could not retrieve document");

                    Console.WriteLine($"Adding bulk rows..");
                    PressToContinue();

                    var dataList = new List<TestData>() { _secondData, _thirdData };
                    await _testDataRepository.BulkAdd(dataList);
                    await WaitIndexRefresh(stoppingToken);

                    var totalDocs = await _testDataRepository.Count();
                    Console.WriteLine($"Index contains {totalDocs} documents");

                    Console.WriteLine($"Getting active document created in past 20minutes..");
                    PressToContinue();

                    var activeInPastTwentyMinutes = await _testDataRepository.GetAllActiveFromDate(minDate);
                    Console.WriteLine($"Retrieved {activeInPastTwentyMinutes.Count()} documents");

                    Console.WriteLine($"Updating doc with id {_exampleData.Id}..");
                    PressToContinue();

                    _exampleData.Name = "UpdatedName";
                    _exampleData.Active = false;
                    await _testDataRepository.Update(_exampleData);
                    await WaitIndexRefresh(stoppingToken);

                    var updatedData = await _testDataRepository.GetById(_exampleData.Id);
                    if (updatedData != null)
                        Console.WriteLine($"Got document with id {updatedData.Id} and name {updatedData.Name}, created on {updatedData.CreationDate:yyyy-MM-dd HH:mm:ss}, active {updatedData.Active}");
                    else
                        Console.WriteLine($"Could not retrieve document");

                    Console.WriteLine($"Getting single document with id {_thirdData.Id} without getting creationdate..");
                    PressToContinue();

                    var thirdWithNoCreationDate = await _testDataRepository.GetByIdWithoutCreationTimeAndActive(_exampleData.Id);
                    if (thirdWithNoCreationDate != null)
                        Console.WriteLine($"Got document with id {thirdWithNoCreationDate.Id} and name {thirdWithNoCreationDate.Name}, created on {thirdWithNoCreationDate.CreationDate:yyyy-MM-dd HH:mm:ss}, active {thirdWithNoCreationDate.Active}");
                    else
                        Console.WriteLine($"Could not retrieve document");

                    Console.WriteLine($"Getting all document sorted by creationdate desc..");
                    PressToContinue();

                    var sortedList = await _testDataRepository.GetAllSortedByCreationDateDesc();
                    foreach (var item in sortedList)
                        Console.WriteLine($"item {item.Name} dated {item.CreationDate:yyyy-MM-dd HH:mm:ss}");

                    Console.WriteLine($"Getting all document grouped by day..");
                    PressToContinue();

                    var groupedData = await _testDataRepository.GetDataGroupedByDay();
                    if (groupedData != null)
                    {
                        Console.WriteLine($"Got {groupedData.Count()} groups");
                        foreach (var group in groupedData)
                        {
                            DateHistogramAggregate? dateHistogramAggregate = group.Value as DateHistogramAggregate;
                            Console.WriteLine($"Group '{group.Key}' contains {dateHistogramAggregate?.Buckets.Count ?? 0} data");
                            if (dateHistogramAggregate != null)
                                foreach (var bucket in dateHistogramAggregate.Buckets)
                                    Console.WriteLine($"Bucket Key: {bucket.KeyAsString}, Doc Count: {bucket.DocCount}");
                        }
                    }
                    else
                        Console.WriteLine($"Could not retrieve grouped data");

                    Console.WriteLine($"Deleting single document with id {_exampleData.Id}..");
                    PressToContinue();

                    await _testDataRepository.Delete(_exampleData);
                    await WaitIndexRefresh(stoppingToken);

                    testDataList = await _testDataRepository.GetAll();
                    Console.WriteLine($"Index contains {testDataList.Count()} documents");

                    Console.WriteLine($"Deleting remaining documents..");
                    PressToContinue();

                    foreach (var testData in testDataList)
                        await _testDataRepository.Delete(testData);

                    await WaitIndexRefresh(stoppingToken);

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

        public static async Task WaitIndexRefresh(CancellationToken stoppingToken)
        {
            Console.WriteLine($"Waiting {_indexRefreshTimeMs}ms to ensure ES index refreshed");
            await Task.Delay(_indexRefreshTimeMs, stoppingToken);
        }

        private static void PressToContinue()
        {
            if (_enablePressOnContinue)
            {
                Console.WriteLine("Enter any key to continue..");
                Console.ReadLine();
            }
        }
    }
}
