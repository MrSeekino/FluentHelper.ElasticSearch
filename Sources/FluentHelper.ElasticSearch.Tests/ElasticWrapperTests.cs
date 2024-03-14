using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Transport;
using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Interfaces;
using FluentHelper.ElasticSearch.QueryParameters;
using FluentHelper.ElasticSearch.TestsSupport;
using NSubstitute;
using NUnit.Framework;
using System.Dynamic;

namespace FluentHelper.ElasticSearch.Tests
{
    [TestFixture]
    public class ElasticWrapperTests
    {
        [TestCase("pre", "suff")]
        [TestCase("", "suff")]
        [TestCase("pre", "")]
        [TestCase("", "")]
        public void Verify_IndexNameForEntity_IsCalculated_Correctly(string prefix, string suffix)
        {
            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.IndexPrefix.Returns(prefix);
            elasticConfig.IndexSuffix.Returns(suffix);

            var testEntityMap = new TestEntityMap();

            ElasticWrapper elasticWrapper = new(elasticConfig, [testEntityMap]);

            var testEntityInstance = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "TestName",
                GroupName = "GroupName",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            string expectedIndexName = string.Empty;
            if (!string.IsNullOrWhiteSpace(prefix))
                expectedIndexName += $"{prefix}-";

            expectedIndexName += $"{testEntityMap.BaseIndexName}-";

            if (!string.IsNullOrWhiteSpace(suffix))
                expectedIndexName += $"{suffix}-";

            expectedIndexName += testEntityMap.IndexCalculator!.GetIndexPostfixByEntity(testEntityInstance);
            expectedIndexName = expectedIndexName.ToLower();

            string indexName = elasticWrapper.GetIndexName(testEntityInstance);
            Assert.That(indexName, Is.Not.Null);
            Assert.That(indexName.Length, Is.GreaterThan(0));
            Assert.That(indexName, Is.EqualTo(expectedIndexName));
        }

        [TestCase("pre", "suf", "2023-06-08", "2023-06-08")]
        [TestCase("pre", "suf", "2023-06-08", "2023-06-10")]
        [TestCase("pre", "suf", "", "2023-06-08")]
        [TestCase("pre", "suf", "2023-06-08", "")]
        [TestCase("pre", "suf", "", "")]
        [TestCase("pre", "", "2023-06-08", "2023-06-08")]
        [TestCase("pre", "", "2023-06-08", "2023-06-10")]
        [TestCase("pre", "", "", "2023-06-08")]
        [TestCase("pre", "", "2023-06-08", "")]
        [TestCase("pre", "", "", "")]
        [TestCase("", "suf", "2023-06-08", "2023-06-08")]
        [TestCase("", "suf", "2023-06-08", "2023-06-10")]
        [TestCase("", "suf", "", "2023-06-08")]
        [TestCase("", "suf", "2023-06-08", "")]
        [TestCase("", "suf", "", "")]
        [TestCase("", "", "2023-06-08", "2023-06-08")]
        [TestCase("", "", "2023-06-08", "2023-06-10")]
        [TestCase("", "", "", "2023-06-08")]
        [TestCase("", "", "2023-06-08", "")]
        [TestCase("", "", "", "")]
        public void Verify_IndexNamesForQueries_AreCalculated_Correctly(string prefix, string suffix, string startTime, string endTime)
        {
            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.IndexPrefix.Returns(prefix);
            elasticConfig.IndexSuffix.Returns(suffix);

            var testEntityMap = new TestEntityMap();

            ElasticWrapper elasticWrapper = new(elasticConfig, [testEntityMap]);

            var testFilter = new TestFilter
            {
                GroupName = "GroupName",
                StartTime = !string.IsNullOrWhiteSpace(startTime) ? DateTime.Parse(startTime) : null,
                EndTime = !string.IsNullOrWhiteSpace(endTime) ? DateTime.Parse(endTime) : null
            };

            string fixedIndexForQuery = string.Empty;
            if (!string.IsNullOrWhiteSpace(prefix))
                fixedIndexForQuery += $"{prefix}-";

            fixedIndexForQuery += $"{testEntityMap.BaseIndexName}-";

            if (!string.IsNullOrWhiteSpace(suffix))
                fixedIndexForQuery += $"{suffix}-";

            var queryIndexes = testEntityMap.IndexCalculator!.GetIndexPostfixByFilter(testFilter);

            var indexesForQuery = $"{fixedIndexForQuery}{string.Join($",{fixedIndexForQuery}", queryIndexes)}";
            indexesForQuery = indexesForQuery.ToLower();

            string indexForQuery = elasticWrapper.GetIndexNamesForQueries<TestEntity>(testFilter);
            Assert.That(indexForQuery, Is.Not.Null);
            Assert.That(indexForQuery.Length, Is.GreaterThan(0));
            Assert.That(indexForQuery, Is.EqualTo(indexesForQuery));
        }

        [Test]
        public void Verify_ElasticWrapper_IsCreatedCorrectly()
        {
            var elasticConfig = Substitute.For<IElasticConfig>();
            var elasticMap = new TestEntityMap();

            var esWrapper = new ElasticWrapper(elasticConfig, [elasticMap]);

            Assert.That(esWrapper.MappingLength, Is.EqualTo(1));
        }

        [Test]
        public void Verify_GetOrCreateClient_WorksProperly()
        {
            bool applySpecialMapCalled = false;

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var elasticMap = Substitute.For<IElasticMap>();
            elasticMap.IdPropertyName.Returns("Id");
            elasticMap.GetMappingType().Returns(typeof(IElasticMap));
            elasticMap.When(x => x.ApplyMapping(Arg.Any<ElasticsearchClientSettings>())).Do(x =>
            {
                applySpecialMapCalled = true;
            });

            var esWrapper = new ElasticWrapper(elasticConfig, [elasticMap]);
            var elasticClient = esWrapper.GetOrCreateClient();

            Assert.That(elasticClient, Is.Not.Null);
            Assert.That(elasticClient.GetType(), Is.EqualTo(typeof(ElasticsearchClient)));
            Assert.That(applySpecialMapCalled, Is.True);
        }

        [Test]
        public void Verify_GetOrCreateClient_WorksProperly_WhenFullyConfigured()
        {
            bool applySpecialMapCalled = false;
            bool logActionCalled = false;
            (string username, string password) basicAuth = new("username", "password");

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.EnableDebug.Returns(true);
            elasticConfig.CertificateFingerprint.Returns("ABCDE");
            elasticConfig.BasicAuthentication.Returns(basicAuth);
            elasticConfig.RequestTimeout.Returns(TimeSpan.FromSeconds(60));
            elasticConfig.LogAction.Returns((x, y, z, k) => { logActionCalled = true; });

            var elasticMap = Substitute.For<IElasticMap>();
            elasticMap.IdPropertyName.Returns("Id");
            elasticMap.GetMappingType().Returns(typeof(IElasticMap));
            elasticMap.When(x => x.ApplyMapping(Arg.Any<ElasticsearchClientSettings>())).Do(x =>
            {
                applySpecialMapCalled = true;
            });

            var esWrapper = new ElasticWrapper(elasticConfig, [elasticMap]);
            var elasticClient = esWrapper.GetOrCreateClient();

            Assert.That(elasticClient, Is.Not.Null);
            Assert.That(elasticClient.GetType(), Is.EqualTo(typeof(ElasticsearchClient)));
            Assert.That(applySpecialMapCalled, Is.True);
            Assert.That(logActionCalled, Is.True);
        }

        [Test]
        public void Verify_Add_WorksCorrectly()
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test",
                GroupName = "Group",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new IndexResponse { Result = Result.Created };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);
            IndexName indexName = esWrapper.GetIndexName(testData);

            esClient.Index(Arg.Any<TestEntity>(), Arg.Any<IndexName>()).Returns(mockedResponse).AndDoes(x =>
            {
                var dataToAdd = x.Arg<TestEntity>();
                var indexUsed = x.Arg<IndexName>();

                Assert.That(dataToAdd.Id, Is.EqualTo(testData.Id));
                Assert.That(indexUsed, Is.EqualTo(indexName));
            });

            esWrapper.Add(testData);
        }

        [Test]
        public async Task Verify_AddAsync_WorksCorrectly()
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test",
                GroupName = "Group",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new IndexResponse { Result = Result.Created };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);
            IndexName indexName = esWrapper.GetIndexName(testData);

            esClient.IndexAsync(Arg.Any<TestEntity>(), Arg.Any<IndexName>()).Returns(mockedResponse).AndDoes(x =>
            {
                var dataToAdd = x.Arg<TestEntity>();
                var indexUsed = x.Arg<IndexName>();

                Assert.That(dataToAdd.Id, Is.EqualTo(testData.Id));
                Assert.That(indexUsed, Is.EqualTo(indexName));
            });

            await esWrapper.AddAsync(testData);
        }

        [Test]
        public void Verify_Add_ThrowsWithInvalidResponse()
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test",
                GroupName = "Group",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new IndexResponse { };
            var mockedResponse = TestableResponseFactory.CreateResponse(response, 400, false);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Index(Arg.Any<TestEntity>(), Arg.Any<IndexName>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            Assert.Throws<InvalidOperationException>(() => esWrapper.Add(testData));
        }

        [Test]
        public void Verify_BulkAdd_WorksCorrectly()
        {
            List<TestEntity> dataList = new()
            {
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test01",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test02",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test03",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                }
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.BulkInsertChunkSize.Returns(2);

            var response = new BulkResponse { Errors = false };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Bulk(Arg.Any<Action<BulkRequestDescriptor>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            int totalAddedElements = esWrapper.BulkAdd(dataList);
            esClient.Received(2).Bulk(Arg.Any<Action<BulkRequestDescriptor>>());
            Assert.That(totalAddedElements, Is.EqualTo(dataList.Count));
        }

        [Test]
        public async Task Verify_BulkAddAsync_WorksCorrectly()
        {
            List<TestEntity> dataList = new()
            {
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test01",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test02",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test03",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                }
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.BulkInsertChunkSize.Returns(2);

            var response = new BulkResponse { Errors = false };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.BulkAsync(Arg.Any<Action<BulkRequestDescriptor>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            int totalAddedElements = await esWrapper.BulkAddAsync(dataList);
            await esClient.Received(2).BulkAsync(Arg.Any<Action<BulkRequestDescriptor>>());
            Assert.That(totalAddedElements, Is.EqualTo(dataList.Count));
        }

        [Test]
        public void Verify_BulkAdd_DoesNothingWithEmptyList()
        {
            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.LogAction.Returns((logLevel, ex, message, args) => { });

            var response = new BulkResponse { Errors = false };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Bulk(Arg.Any<Action<BulkRequestDescriptor>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            int totalAddedElements = esWrapper.BulkAdd<TestEntity>([]);
            esClient.DidNotReceive().Bulk(Arg.Any<Action<BulkRequestDescriptor>>());
            Assert.That(totalAddedElements, Is.EqualTo(0));
        }

        [Test]
        public void Verify_BulkAdd_WorksCorrectly_WithASingleBulk()
        {
            List<TestEntity> dataList = new()
            {
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test01",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test02",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test03",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                }
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.BulkInsertChunkSize.Returns(10);

            var response = new BulkResponse { Errors = false };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Bulk(Arg.Any<Action<BulkRequestDescriptor>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            int totalAddedElements = esWrapper.BulkAdd(dataList);
            esClient.Received(1).Bulk(Arg.Any<Action<BulkRequestDescriptor>>());
            Assert.That(totalAddedElements, Is.EqualTo(dataList.Count));
        }

        [Test]
        public void Verify_BulkAdd_DoesNothingWithInvalidResponse()
        {
            List<TestEntity> dataList = new()
            {
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test01",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test02",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test03",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                }
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.BulkInsertChunkSize.Returns(2);
            elasticConfig.LogAction.Returns((logLevel, ex, message, args) => { });

            var response = new BulkResponse { Errors = true };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Bulk(Arg.Any<Action<BulkRequestDescriptor>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            int totalAddedElements = esWrapper.BulkAdd(dataList);
            elasticConfig.Received(4).LogAction!(Microsoft.Extensions.Logging.LogLevel.Error, Arg.Any<Exception?>(), Arg.Any<string>(), Arg.Any<object?[]>());
            esClient.Received(2).Bulk(Arg.Any<Action<BulkRequestDescriptor>>());
            Assert.That(totalAddedElements, Is.EqualTo(0));
        }

        [Test]
        public void Verify_Delete_WorksCorrectly()
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test",
                GroupName = "Group",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new DeleteResponse { Result = Result.Deleted };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);
            IndexName indexName = esWrapper.GetIndexName(testData);

            esClient.Delete(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<DeleteRequestDescriptor<TestEntity>>>()).Returns(mockedResponse).AndDoes(x =>
            {
                var idToDelete = x.Arg<Id>();
                var indexUsed = x.Arg<IndexName>();

                Assert.That(idToDelete.ToString(), Is.EqualTo(testData.Id.ToString()));
                Assert.That(indexUsed, Is.EqualTo(indexName));
            });

            esWrapper.Delete(testData);
        }

        [Test]
        public async Task Verify_DeleteAsync_WorksCorrectly()
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test",
                GroupName = "Group",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new DeleteResponse { Result = Result.Deleted };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);
            IndexName indexName = esWrapper.GetIndexName(testData);

            esClient.DeleteAsync(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<DeleteRequestDescriptor<TestEntity>>>()).Returns(mockedResponse).AndDoes(x =>
            {
                var idToDelete = x.Arg<Id>();
                var indexUsed = x.Arg<IndexName>();

                Assert.That(idToDelete.ToString(), Is.EqualTo(testData.Id.ToString()));
                Assert.That(indexUsed, Is.EqualTo(indexName));
            });

            await esWrapper.DeleteAsync(testData);
        }

        [Test]
        public void Verify_Delete_ThrowsWithInvalidResponse()
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test",
                GroupName = "Group",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new DeleteResponse { Result = Result.NotFound };
            var mockedResponse = TestableResponseFactory.CreateResponse(response, 404, false);

            var esClient = Substitute.For<ElasticsearchClient>();

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);
            IndexName indexName = esWrapper.GetIndexName(testData);

            esClient.Delete(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<DeleteRequestDescriptor<TestEntity>>>()).Returns(mockedResponse);

            Assert.Throws<InvalidOperationException>(() => esWrapper.Delete(testData));
        }

        [Test]
        public void Verify_AddOrUpdate_WorksCorrectly()
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test01",
                GroupName = "Group01",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new UpdateResponse<TestEntity> { Result = Result.Updated };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            IndexName indexName = esWrapper.GetIndexName(testData);

            esClient.Update(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<UpdateRequestDescriptor<TestEntity, ExpandoObject>>>()).Returns(mockedResponse).AndDoes(x =>
            {
                var idToUpdate = x.Arg<Id>();
                var indexUsed = x.Arg<IndexName>();

                Assert.That(idToUpdate.ToString(), Is.EqualTo(testData.Id.ToString()));
                Assert.That(indexUsed, Is.EqualTo(indexName));
            });

            esWrapper.AddOrUpdate(testData, x => x.Update(f => f.Name).Update(f => f.Active), 1);
        }

        [Test]
        public async Task Verify_AddOrUpdateAsync_WorksCorrectly()
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test01",
                GroupName = "Group01",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new UpdateResponse<TestEntity> { Result = Result.Updated };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            IndexName indexName = esWrapper.GetIndexName(testData);

            esClient.UpdateAsync(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<UpdateRequestDescriptor<TestEntity, ExpandoObject>>>()).Returns(mockedResponse).AndDoes(x =>
            {
                var idToUpdate = x.Arg<Id>();
                var indexUsed = x.Arg<IndexName>();

                Assert.That(idToUpdate.ToString(), Is.EqualTo(testData.Id.ToString()));
                Assert.That(indexUsed, Is.EqualTo(indexName));
            });

            await esWrapper.AddOrUpdateAsync(testData, x => x.Update(f => f.Name).Update(f => f.Active), 1);
        }

        [Test]
        public async Task Verify_AddOrUpdateAsync_WorksCorrectly_WithDefaultRetries()
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test01",
                GroupName = "Group01",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new UpdateResponse<TestEntity> { Result = Result.Updated };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            IndexName indexName = esWrapper.GetIndexName(testData);

            esClient.UpdateAsync(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<UpdateRequestDescriptor<TestEntity, ExpandoObject>>>()).Returns(mockedResponse).AndDoes(x =>
            {
                var idToUpdate = x.Arg<Id>();
                var indexUsed = x.Arg<IndexName>();

                Assert.That(idToUpdate.ToString(), Is.EqualTo(testData.Id.ToString()));
                Assert.That(indexUsed, Is.EqualTo(indexName));
            });

            await esWrapper.AddOrUpdateAsync(testData, x => x.Update(f => f.Name).Update(f => f.Active));
        }

        [Test]
        public void Verify_AddOrUpdate_ThrowsWithInvalidResponse()
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test01",
                GroupName = "Group01",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new UpdateResponse<TestEntity> { Result = Result.NoOp };
            var mockedResponse = TestableResponseFactory.CreateResponse(response, 400, false);

            var esClient = Substitute.For<ElasticsearchClient>();
            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            IndexName indexName = esWrapper.GetIndexName(testData);

            esClient.Update(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<UpdateRequestDescriptor<TestEntity, ExpandoObject>>>()).Returns(mockedResponse).AndDoes(x =>
            {
                var idToUpdate = x.Arg<Id>();
                var indexUsed = x.Arg<IndexName>();

                Assert.That(idToUpdate.ToString(), Is.EqualTo(testData.Id.ToString()));
                Assert.That(indexUsed, Is.EqualTo(indexName));
            });

            Assert.Throws<InvalidOperationException>(() => esWrapper.AddOrUpdate(testData, x => x.Update(f => f.Name).Update(f => f.Active), 1));
        }

        [Test]
        public void Verify_Query_WorksCorrectly()
        {
            List<TestEntity> dataList = new()
            {
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test01",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test02",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test03",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                }
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new SearchResponse<TestEntity>
            {
                HitsMetadata = new HitsMetadata<TestEntity>()
                {
                    Hits = dataList.Select(x => new Hit<TestEntity>
                    {
                        Id = x.Id.ToString(),
                        Source = x
                    }).ToList(),
                    Total = new TotalHits() { Value = 3 }
                }
            };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Search(Arg.Any<SearchRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            var esQueryParameters = new ElasticQueryParameters<TestEntity>
            {
                Skip = 0,
                Take = 10
            };

            var itemList = esWrapper.Query(null, esQueryParameters);
            esClient.Received(1).Search(Arg.Any<SearchRequestDescriptor<TestEntity>>());
            Assert.That(itemList.Count(), Is.EqualTo(3));
        }

        [Test]
        public void Verify_Query_WorksCorrectly_WithAllParameters()
        {
            List<TestEntity> dataList = new()
            {
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test01",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test02",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test03",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                }
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new SearchResponse<TestEntity>
            {
                HitsMetadata = new HitsMetadata<TestEntity>()
                {
                    Hits = dataList.Select(x => new Hit<TestEntity>
                    {
                        Id = x.Id.ToString(),
                        Source = x
                    }).ToList(),
                    Total = new TotalHits() { Value = 3 }
                }
            };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Search(Arg.Any<SearchRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            var esQueryParameters = new ElasticQueryParameters<TestEntity>
            {
                Skip = 0,
                Take = 10,
                QueryDescriptor = new Elastic.Clients.Elasticsearch.QueryDsl.QueryDescriptor<TestEntity>(),
                SortOptionsDescriptor = new SortOptionsDescriptor<TestEntity>(),
                SourceConfig = new SourceConfig(false)
            };

            var itemList = esWrapper.Query(null, esQueryParameters);
            esClient.Received(1).Search(Arg.Any<SearchRequestDescriptor<TestEntity>>());
            Assert.That(itemList.Count(), Is.EqualTo(3));
        }

        [Test]
        public async Task Verify_QueryAsync_WorksCorrectly()
        {
            List<TestEntity> dataList = new()
            {
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test01",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test02",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test03",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                }
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new SearchResponse<TestEntity>
            {
                HitsMetadata = new HitsMetadata<TestEntity>()
                {
                    Hits = dataList.Select(x => new Hit<TestEntity>
                    {
                        Id = x.Id.ToString(),
                        Source = x
                    }).ToList(),
                    Total = new TotalHits() { Value = 3 }
                }
            };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.SearchAsync(Arg.Any<SearchRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            var esQueryParameters = new ElasticQueryParameters<TestEntity>
            {
                Skip = 0,
                Take = 10
            };

            var itemList = await esWrapper.QueryAsync(null, esQueryParameters);
            await esClient.Received(1).SearchAsync(Arg.Any<SearchRequestDescriptor<TestEntity>>());
            Assert.That(itemList.Count(), Is.EqualTo(3));
        }

        [Test]
        public void Verify_Query_ThrowsWithInvalidResponse()
        {
            List<TestEntity> dataList = new()
            {
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test01",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test02",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test03",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow,
                    Active = true
                }
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new SearchResponse<TestEntity>
            {
                HitsMetadata = new HitsMetadata<TestEntity>()
                {
                    Hits = [],
                    Total = new TotalHits() { Value = 0 }
                }
            };
            var mockedResponse = TestableResponseFactory.CreateResponse(response, 400, false);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Search(Arg.Any<SearchRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            var esQueryParameters = new ElasticQueryParameters<TestEntity>
            {
                Skip = 0,
                Take = 10
            };

            Assert.Throws<InvalidOperationException>(() => esWrapper.Query(null, esQueryParameters));
        }

        [Test]
        public void Verify_Count_WorksCorrectly()
        {
            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new CountResponse { Count = 3 };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Count(Arg.Any<CountRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            var esQueryParameters = new ElasticQueryParameters<TestEntity>();
            var totalItems = esWrapper.Count(null, esQueryParameters);

            esClient.Received(1).Count(Arg.Any<CountRequestDescriptor<TestEntity>>());
            Assert.That(totalItems, Is.EqualTo(3));
        }

        [Test]
        public void Verify_Count_WorksCorrectly_WithQueryDescriptor()
        {
            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new CountResponse { Count = 3 };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Count(Arg.Any<CountRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            var esQueryParameters = new ElasticQueryParameters<TestEntity>()
            {
                QueryDescriptor = new Elastic.Clients.Elasticsearch.QueryDsl.QueryDescriptor<TestEntity>()
            };
            var totalItems = esWrapper.Count(null, esQueryParameters);

            esClient.Received(1).Count(Arg.Any<CountRequestDescriptor<TestEntity>>());
            Assert.That(totalItems, Is.EqualTo(3));
        }

        [Test]
        public async Task Verify_CountAsync_WorksCorrectly()
        {
            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new CountResponse { Count = 3 };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.CountAsync(Arg.Any<CountRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            var esQueryParameters = new ElasticQueryParameters<TestEntity>();
            var totalItems = await esWrapper.CountAsync(null, esQueryParameters);

            await esClient.Received(1).CountAsync(Arg.Any<CountRequestDescriptor<TestEntity>>());
            Assert.That(totalItems, Is.EqualTo(3));
        }

        [Test]
        public void Verify_Count_ThrowsWithInvalidResponse()
        {
            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new CountResponse { Count = 0 };
            var mockedResponse = TestableResponseFactory.CreateResponse(response, 400, false);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Count(Arg.Any<CountRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);
            var esQueryParameters = new ElasticQueryParameters<TestEntity>();

            Assert.Throws<InvalidOperationException>(() => esWrapper.Count(null, esQueryParameters));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void Verify_Exists_WorksCorrectly(bool itemExisting)
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test",
                GroupName = "Group",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new ExistsResponse { };
            int httpStatusCode = itemExisting ? 200 : 404;
            var mockedResponse = TestableResponseFactory.CreateResponse(response, httpStatusCode, itemExisting);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Exists(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<ExistsRequestDescriptor<TestEntity>>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            var itemExist = esWrapper.Exists(testData);

            esClient.Received(1).Exists(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<ExistsRequestDescriptor<TestEntity>>>());
            Assert.That(itemExist, Is.EqualTo(itemExisting));
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task Verify_ExistsAsync_WorksCorrectly(bool itemExisting)
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test",
                GroupName = "Group",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new ExistsResponse { };
            int httpStatusCode = itemExisting ? 200 : 404;
            var mockedResponse = TestableResponseFactory.CreateResponse(response, httpStatusCode, itemExisting);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.ExistsAsync(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<ExistsRequestDescriptor<TestEntity>>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            var itemExist = await esWrapper.ExistsAsync(testData);

            await esClient.Received(1).ExistsAsync(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<ExistsRequestDescriptor<TestEntity>>>());
            Assert.That(itemExist, Is.EqualTo(itemExisting));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void Verify_GetSource_WorksCorrectly(bool itemExisting)
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test",
                GroupName = "Group",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new GetResponse<TestEntity> { Source = itemExisting ? testData : null, Found = itemExisting };
            int httpStatusCode = itemExisting ? 200 : 404;
            var mockedResponse = TestableResponseFactory.CreateResponse(response, httpStatusCode, itemExisting);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Get<TestEntity>(Arg.Any<IndexName>(), Arg.Any<Id>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            var sourceData = esWrapper.GetSource(testData);

            esClient.Received(1).Get<TestEntity>(Arg.Any<IndexName>(), Arg.Any<Id>());

            if (!itemExisting)
            {
                Assert.That(sourceData, Is.Null);
                return;
            }

            Assert.That(sourceData, Is.Not.Null);
            Assert.That(sourceData!.Name, Is.EqualTo(testData.Name));
            Assert.That(sourceData!.GroupName, Is.EqualTo(testData.GroupName));
            Assert.That(sourceData!.CreationTime, Is.EqualTo(testData.CreationTime));
            Assert.That(sourceData!.Active, Is.EqualTo(testData.Active));
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task Verify_GetSourceAsync_WorksCorrectly(bool itemExisting)
        {
            var testData = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test",
                GroupName = "Group",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var elasticMap = new TestEntityMap();
            elasticMap.Map();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new GetResponse<TestEntity> { Source = itemExisting ? testData : null, Found = itemExisting };
            int httpStatusCode = itemExisting ? 200 : 404;
            var mockedResponse = TestableResponseFactory.CreateResponse(response, httpStatusCode, itemExisting);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.GetAsync<TestEntity>(Arg.Any<IndexName>(), Arg.Any<Id>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, elasticConfig, [elasticMap]);

            var sourceData = await esWrapper.GetSourceAsync(testData);

            await esClient.Received(1).GetAsync<TestEntity>(Arg.Any<IndexName>(), Arg.Any<Id>());

            if (!itemExisting)
            {
                Assert.That(sourceData, Is.Null);
                return;
            }

            Assert.That(sourceData, Is.Not.Null);
            Assert.That(sourceData!.Name, Is.EqualTo(testData.Name));
            Assert.That(sourceData!.GroupName, Is.EqualTo(testData.GroupName));
            Assert.That(sourceData!.CreationTime, Is.EqualTo(testData.CreationTime));
            Assert.That(sourceData!.Active, Is.EqualTo(testData.Active));
        }

        [Test]
        public void Verify_Dispose_ForceClientRecreation()
        {
            int logActionCalls = 0;

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.LogAction.Returns((loglevel, exception, message, args) =>
            {
                logActionCalls++;
            });

            var esWrapper = new ElasticWrapper(elasticConfig, []);

            var elasticClient = esWrapper.GetOrCreateClient();
            Assert.That(elasticClient, Is.Not.Null);
            Assert.That(logActionCalls, Is.EqualTo(1));

            elasticClient = esWrapper.GetOrCreateClient();
            Assert.That(elasticClient, Is.Not.Null);
            Assert.That(logActionCalls, Is.EqualTo(1));

            esWrapper.Dispose();

            elasticClient = esWrapper.GetOrCreateClient();
            Assert.That(elasticClient, Is.Not.Null);
            Assert.That(logActionCalls, Is.EqualTo(2));
        }
    }
}
