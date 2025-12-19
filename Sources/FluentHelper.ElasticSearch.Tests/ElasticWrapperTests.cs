using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.Fluent;
using Elastic.Transport;
using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.IndexCalculators;
using FluentHelper.ElasticSearch.Interfaces;
using FluentHelper.ElasticSearch.QueryParameters;
using FluentHelper.ElasticSearch.TestsSupport;
using Microsoft.Extensions.Logging;
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
            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.IndexPrefix.Returns(prefix);
            elasticConfig.IndexSuffix.Returns(suffix);

            var testEntityMap = new TestEntityMap();

            ElasticWrapper elasticWrapper = new(loggerFactory, elasticConfig, [testEntityMap]);

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

            expectedIndexName += ((IElasticIndexCalculator<TestEntity>?)testEntityMap.IndexCalculator)!.GetIndexPostfixByEntity(testEntityInstance);
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
            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.IndexPrefix.Returns(prefix);
            elasticConfig.IndexSuffix.Returns(suffix);

            var testEntityMap = new TestEntityMap();

            ElasticWrapper elasticWrapper = new(loggerFactory, elasticConfig, [testEntityMap]);

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
            var loggerFactory = Substitute.For<ILoggerFactory>();
            var elasticConfig = Substitute.For<IElasticConfig>();
            var elasticMap = new TestEntityMap();

            var esWrapper = new ElasticWrapper(loggerFactory, elasticConfig, [elasticMap]);

            Assert.That(esWrapper.MappingLength, Is.EqualTo(1));
        }

        [Test]
        public void Verify_GetOrCreateClient_WorksProperly()
        {
            bool applySpecialMapCalled = false;

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var elasticMap = Substitute.For<IElasticMap>();
            elasticMap.IdPropertyName.Returns("Id");
            elasticMap.GetMappingType().Returns(typeof(IElasticMap));
            elasticMap.When(x => x.ApplyMapping(Arg.Any<ElasticsearchClientSettings>())).Do(x =>
            {
                applySpecialMapCalled = true;
            });

            var esWrapper = new ElasticWrapper(loggerFactory, elasticConfig, [elasticMap]);
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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.DisablePing.Returns(true);
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

            var esWrapper = new ElasticWrapper(loggerFactory, elasticConfig, [elasticMap]);
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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new IndexResponse { Result = Result.Created };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.Exists(Arg.Any<Indices>()).Returns(indexExistMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);
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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new IndexResponse { Result = Result.Created };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsAsync(Arg.Any<Indices>()).Returns(indexExistMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);
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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new IndexResponse { };
            var mockedResponse = TestableResponseFactory.CreateResponse(response, 400, false);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.Exists(Arg.Any<Indices>()).Returns(indexExistMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);
            esClient.Index(Arg.Any<TestEntity>(), Arg.Any<IndexName>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.BulkInsertChunkSize.Returns(2);

            var response = new BulkResponse { Errors = false };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.Exists(Arg.Any<Indices>()).Returns(indexExistMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);
            esClient.Bulk(Arg.Any<Action<BulkRequestDescriptor>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.BulkInsertChunkSize.Returns(2);

            var response = new BulkResponse { Errors = false };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsAsync(Arg.Any<Indices>()).Returns(indexExistMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);
            esClient.BulkAsync(Arg.Any<Action<BulkRequestDescriptor>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            int totalAddedElements = await esWrapper.BulkAddAsync(dataList);
            await esClient.Received(2).BulkAsync(Arg.Any<Action<BulkRequestDescriptor>>());
            Assert.That(totalAddedElements, Is.EqualTo(dataList.Count));
        }

        [Test]
        public void Verify_BulkAdd_DoesNothingWithEmptyList()
        {
            var elasticMap = new TestEntityMap();

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.LogAction.Returns((logLevel, ex, message, args) => { });

            var response = new BulkResponse { Errors = false };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Bulk(Arg.Any<Action<BulkRequestDescriptor>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.BulkInsertChunkSize.Returns(10);

            var response = new BulkResponse { Errors = false };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.Exists(Arg.Any<Indices>()).Returns(indexExistMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);
            esClient.Bulk(Arg.Any<Action<BulkRequestDescriptor>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            int totalAddedElements = esWrapper.BulkAdd(dataList);
            esClient.Received(1).Bulk(Arg.Any<Action<BulkRequestDescriptor>>());
            Assert.That(totalAddedElements, Is.EqualTo(dataList.Count));
        }

        [Test]
        public void Verify_BulkAdd_DoesNothingWithInvalidResponse()
        {
            int totalErrorMessages = 0;

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

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.BulkInsertChunkSize.Returns(2);
            elasticConfig.LogAction.Returns((logLevel, ex, message, args) =>
            {
                if (logLevel == Microsoft.Extensions.Logging.LogLevel.Error)
                    totalErrorMessages++;
            });

            var response = new BulkResponse { Errors = true };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.Exists(Arg.Any<Indices>()).Returns(indexExistMockedResponse);

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);
            esClient.Bulk(Arg.Any<Action<BulkRequestDescriptor>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            int totalAddedElements = esWrapper.BulkAdd(dataList);
            esClient.Received(2).Bulk(Arg.Any<Action<BulkRequestDescriptor>>());
            Assert.That(totalAddedElements, Is.EqualTo(0));
            Assert.That(totalErrorMessages, Is.EqualTo(2));
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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new DeleteResponse { Result = Result.Deleted };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);
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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new DeleteResponse { Result = Result.Deleted };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);
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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new DeleteResponse { Result = Result.NotFound };
            var mockedResponse = TestableResponseFactory.CreateResponse(response, 404, false);

            var esClient = Substitute.For<ElasticsearchClient>();

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);
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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new UpdateResponse<TestEntity> { Result = Result.Updated };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.Exists(Arg.Any<Indices>()).Returns(indexExistMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new UpdateResponse<TestEntity> { Result = Result.Updated };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsAsync(Arg.Any<Indices>()).Returns(indexExistMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new UpdateResponse<TestEntity> { Result = Result.Updated };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsAsync(Arg.Any<Indices>()).Returns(indexExistMockedResponse);

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new UpdateResponse<TestEntity> { Result = Result.NoOp };
            var mockedResponse = TestableResponseFactory.CreateResponse(response, 400, false);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.Exists(Arg.Any<Indices>()).Returns(indexExistMockedResponse);

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

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

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

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

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

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

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

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

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new CountResponse { Count = 3 };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Count(Arg.Any<CountRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            var esQueryParameters = new ElasticQueryParameters<TestEntity>();
            var totalItems = esWrapper.Count(null, esQueryParameters);

            esClient.Received(1).Count(Arg.Any<CountRequestDescriptor<TestEntity>>());
            Assert.That(totalItems, Is.EqualTo(3));
        }

        [Test]
        public void Verify_Count_WorksCorrectly_WithQueryDescriptor()
        {
            var elasticMap = new TestEntityMap();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new CountResponse { Count = 3 };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Count(Arg.Any<CountRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new CountResponse { Count = 3 };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.CountAsync(Arg.Any<CountRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            var esQueryParameters = new ElasticQueryParameters<TestEntity>();
            var totalItems = await esWrapper.CountAsync(null, esQueryParameters);

            await esClient.Received(1).CountAsync(Arg.Any<CountRequestDescriptor<TestEntity>>());
            Assert.That(totalItems, Is.EqualTo(3));
        }

        [Test]
        public void Verify_Count_ThrowsWithInvalidResponse()
        {
            var elasticMap = new TestEntityMap();

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new CountResponse { Count = 0 };
            var mockedResponse = TestableResponseFactory.CreateResponse(response, 400, false);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Count(Arg.Any<CountRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);
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

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new ExistsResponse { };
            int httpStatusCode = itemExisting ? 200 : 404;
            var mockedResponse = TestableResponseFactory.CreateResponse(response, httpStatusCode, itemExisting);

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Exists(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<ExistsRequestDescriptor<TestEntity>>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new ExistsResponse { };
            int httpStatusCode = itemExisting ? 200 : 404;
            var mockedResponse = TestableResponseFactory.CreateResponse(response, httpStatusCode, itemExisting);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.ExistsAsync(Arg.Any<IndexName>(), Arg.Any<Id>(), Arg.Any<Action<ExistsRequestDescriptor<TestEntity>>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new GetResponse<TestEntity> { Source = itemExisting ? testData : null, Found = itemExisting };
            int httpStatusCode = itemExisting ? 200 : 404;
            var mockedResponse = TestableResponseFactory.CreateResponse(response, httpStatusCode, itemExisting);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Get<TestEntity>(Arg.Any<IndexName>(), Arg.Any<Id>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var response = new GetResponse<TestEntity> { Source = itemExisting ? testData : null, Found = itemExisting };
            int httpStatusCode = itemExisting ? 200 : 404;
            var mockedResponse = TestableResponseFactory.CreateResponse(response, httpStatusCode, itemExisting);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.GetAsync<TestEntity>(Arg.Any<IndexName>(), Arg.Any<Id>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

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
        public void Verify_CreateIndex_WorksCorrectly()
        {
            string indexName = "An_Index";

            var elasticMap = new TestEntityMap();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var indexExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 404, false);
            var indexCreatedMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexResponse() { Index = indexName, Acknowledged = true }, 201);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.Exists(Arg.Any<Indices>()).Returns(indexExistMockedResponse).AndDoes(x =>
            {
                var indices = x.Arg<Indices>();
                Assert.That(indices.First().ToString(), Is.EqualTo(indexName));
            });
            esIndicesClient.Create(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor>()).Returns(indexCreatedMockedResponse);

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            var result = esWrapper.CreateIndex<TestEntity>(indexName);
            Assert.That(result, Is.EqualTo(true));

            esIndicesClient.Received(1).Exists(Arg.Any<Indices>());
            esIndicesClient.Received(1).Create(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor>());
        }

        [Test]
        public void Verify_CreateIndexFromData_WorksCorrectly()
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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            string indexName = esWrapper.GetIndexName(testData);

            var indexExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 404, false);
            var indexCreatedMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexResponse() { Index = indexName, Acknowledged = true }, 201);

            esIndicesClient.Exists(Arg.Any<Indices>()).Returns(indexExistMockedResponse).AndDoes(x =>
            {
                var indices = x.Arg<Indices>();
                Assert.That(indices.First().ToString(), Is.EqualTo(indexName));
            });
            esIndicesClient.Create(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor>()).Returns(indexCreatedMockedResponse);

            var result = esWrapper.CreateIndexFromData(testData);
            Assert.That(result, Is.EqualTo(true));

            esIndicesClient.Received(1).Exists(Arg.Any<Indices>());
            esIndicesClient.Received(1).Create(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor>());
        }

        [Test]
        public void Verify_CreateIndex_ReturnsFalseWhenAlreadyExist()
        {
            string indexName = "An_Index";

            var elasticMap = new TestEntityMap();

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.Exists(Arg.Any<Indices>()).Returns(indexExistMockedResponse).AndDoes(x =>
            {
                var indices = x.Arg<Indices>();
                Assert.That(indices.First().ToString(), Is.EqualTo(indexName));
            });

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            var result = esWrapper.CreateIndex<TestEntity>(indexName);
            Assert.That(result, Is.EqualTo(false));

            esIndicesClient.Received(1).Exists(Arg.Any<Indices>());
            esIndicesClient.Received(0).Create(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor>());
        }

        [Test]
        public void Verify_CreateIndex_ThrowsOnInvalidResponse()
        {
            string indexName = "An_Index";

            var elasticMap = new TestEntityMap();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var indexExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 404, false);
            var indexCreatedMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexResponse() { Index = indexName, Acknowledged = false }, 400, false);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.Exists(Arg.Any<Indices>()).Returns(indexExistMockedResponse).AndDoes(x =>
            {
                var indices = x.Arg<Indices>();
                Assert.That(indices.First().ToString(), Is.EqualTo(indexName));
            });
            esIndicesClient.Create(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor>()).Returns(indexCreatedMockedResponse);

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            Assert.Throws<InvalidOperationException>(() => esWrapper.CreateIndex<TestEntity>(indexName));

            esIndicesClient.Received(1).Exists(Arg.Any<Indices>());
            esIndicesClient.Received(1).Create(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor>());
        }

        [Test]
        public async Task Verify_CreateIndexAsync_WorksCorrectly()
        {
            string indexName = "An_Index";

            var elasticMap = new TestThirdEntityMap();

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var indexExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 404, false);
            var indexCreatedMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexResponse() { Index = indexName, Acknowledged = true }, 201);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsAsync(Arg.Any<Indices>()).Returns(indexExistMockedResponse).AndDoes(x =>
            {
                var indices = x.Arg<Indices>();
                Assert.That(indices.First().ToString(), Is.EqualTo(indexName));
            });
            esIndicesClient.CreateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor>()).Returns(indexCreatedMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            var result = await esWrapper.CreateIndexAsync<TestThirdEntity>(indexName);
            Assert.That(result, Is.EqualTo(true));

            await esIndicesClient.Received(1).ExistsAsync(Arg.Any<Indices>());
            await esIndicesClient.Received(1).CreateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor>());
        }

        [Test]
        public async Task Verify_CreateIndexAsync_ReturnsFalseWhenAlreadyExist()
        {
            string indexName = "An_Index";

            var elasticMap = new TestEntityMap();

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var indexExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsAsync(Arg.Any<Indices>()).Returns(indexExistMockedResponse).AndDoes(x =>
            {
                var indices = x.Arg<Indices>();
                Assert.That(indices.First().ToString(), Is.EqualTo(indexName));
            });

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            var result = await esWrapper.CreateIndexAsync<TestEntity>(indexName);
            Assert.That(result, Is.EqualTo(false));

            await esIndicesClient.Received(1).ExistsAsync(Arg.Any<Indices>());
            await esIndicesClient.Received(0).CreateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor>());
        }

        [Test]
        public async Task Verify_CreateIndexFromDataAsync_WorksCorrectly()
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

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            string indexName = esWrapper.GetIndexName(testData);

            var indexExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse(), 404, false);
            var indexCreatedMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexResponse() { Index = indexName, Acknowledged = true }, 201);

            esIndicesClient.ExistsAsync(Arg.Any<Indices>()).Returns(indexExistMockedResponse).AndDoes(x =>
            {
                var indices = x.Arg<Indices>();
                Assert.That(indices.First().ToString(), Is.EqualTo(indexName));
            });
            esIndicesClient.CreateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor>()).Returns(indexCreatedMockedResponse);

            var result = await esWrapper.CreateIndexFromDataAsync(testData);
            Assert.That(result, Is.EqualTo(true));

            await esIndicesClient.Received(1).ExistsAsync(Arg.Any<Indices>());
            await esIndicesClient.Received(1).CreateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.CreateIndexRequestDescriptor>());
        }

        [Test]
        public void Verify_CreateIndexTemplate_WorksCorrectly()
        {
            var elasticMap = new TestEntityMap();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var templateExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 404, false);
            var templateCreatedMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateResponse() { Acknowledged = true }, 201);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsIndexTemplate(Arg.Any<Name>()).Returns(templateExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(elasticMap.TemplateName));
            });
            esIndicesClient.PutIndexTemplate(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>()).Returns(templateCreatedMockedResponse);

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            var result = esWrapper.CreateIndexTemplate<TestEntity>();
            Assert.That(result, Is.EqualTo(true));

            esIndicesClient.Received(1).ExistsIndexTemplate(Arg.Any<Name>());
            esIndicesClient.Received(1).PutIndexTemplate(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>());
        }

        [Test]
        public void Verify_CreateIndexTemplate_ReturnFalseWhenTemplateAlreadyExists()
        {
            var elasticMap = new TestEntityMap();

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var templateExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsIndexTemplate(Arg.Any<Name>()).Returns(templateExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(elasticMap.TemplateName));
            });

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            var result = esWrapper.CreateIndexTemplate<TestEntity>();
            Assert.That(result, Is.EqualTo(false));

            esIndicesClient.Received(1).ExistsIndexTemplate(Arg.Any<Name>());
            esIndicesClient.Received(0).PutIndexTemplate(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>());
        }

        [Test]
        public void Verify_CreateIndexTemplate_ThrowsWhenEntityTypeAndPassedMappingMismatch()
        {
            var testEntityMap = new TestEntityMap();
            var secondEntityMap = new TestSecondEntityMap();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var templateExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 404, false);
            var templateCreatedMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateResponse() { Acknowledged = true }, 201);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [testEntityMap, secondEntityMap]);

            Assert.Throws<InvalidOperationException>(() => esWrapper.CreateIndexTemplate<TestEntity>(secondEntityMap));

            esIndicesClient.Received(0).ExistsIndexTemplate(Arg.Any<Name>());
            esIndicesClient.Received(0).PutIndexTemplate(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>());
        }

        [Test]
        public void Verify_CreateIndexTemplate_ThrowsOnInvalidResponse()
        {
            var elasticMap = new TestEntityMap();

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var templateExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 404, false);
            var templateCreatedMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateResponse() { Acknowledged = false }, 400, false);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsIndexTemplate(Arg.Any<Name>()).Returns(templateExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(elasticMap.TemplateName));
            });
            esIndicesClient.PutIndexTemplate(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>()).Returns(templateCreatedMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            Assert.Throws<InvalidOperationException>(() => esWrapper.CreateIndexTemplate<TestEntity>());

            esIndicesClient.Received(1).ExistsIndexTemplate(Arg.Any<Name>());
            esIndicesClient.Received(1).PutIndexTemplate(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>());
        }

        [Test]
        public async Task Verify_CreateIndexTemplateAsync_WorksCorrectly()
        {
            var elasticMap = new TestSecondEntityMap();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var templateExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 404, false);
            var templateCreatedMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateResponse() { Acknowledged = true }, 201);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsIndexTemplateAsync(Arg.Any<Name>()).Returns(templateExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(elasticMap.TemplateName));
            });
            esIndicesClient.PutIndexTemplateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>()).Returns(templateCreatedMockedResponse);

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            var result = await esWrapper.CreateIndexTemplateAsync<TestSecondEntity>();
            Assert.That(result, Is.EqualTo(true));

            await esIndicesClient.Received(1).ExistsIndexTemplateAsync(Arg.Any<Name>());
            await esIndicesClient.Received(1).PutIndexTemplateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>());
        }

        [Test]
        public async Task Verify_CreateIndexTemplateAsync_ReturnFalseWhenTemplateAlreadyExists()
        {
            var elasticMap = new TestEntityMap();

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var templateExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 200);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsIndexTemplateAsync(Arg.Any<Name>()).Returns(templateExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(elasticMap.TemplateName));
            });

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            var result = await esWrapper.CreateIndexTemplateAsync<TestEntity>();
            Assert.That(result, Is.EqualTo(false));

            await esIndicesClient.Received(1).ExistsIndexTemplateAsync(Arg.Any<Name>());
            await esIndicesClient.Received(0).PutIndexTemplateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>());
        }

        [Test]
        public async Task Verify_CreateIndexTemplateAsync_ThrowsWhenEntityTypeAndPassedMappingMismatch()
        {
            var testEntityMap = new TestEntityMap();
            var secondEntityMap = new TestSecondEntityMap();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var templateExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 404, false);
            var templateCreatedMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateResponse() { Acknowledged = true }, 201);

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [testEntityMap, secondEntityMap]);

            Assert.ThrowsAsync<InvalidOperationException>(async () => await esWrapper.CreateIndexTemplateAsync<TestEntity>(secondEntityMap));

            await esIndicesClient.Received(0).ExistsIndexTemplateAsync(Arg.Any<Name>());
            await esIndicesClient.Received(0).PutIndexTemplateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>());
        }

        [Test]
        public void Verify_CreateAllMappedIndexTemplate_WorksCorrectly()
        {
            var loggerFactory = Substitute.For<ILoggerFactory>();
            var elasticMap = new TestEntityMap();
            var secondEntityMap = new TestSecondEntityMap();
            var thirdEntityMap = new TestThirdEntityMap();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var templateOneExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 200);
            var templateTwoExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 404, false);
            var templateCreatedMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateResponse() { Acknowledged = true }, 201);

            var templateOne = new Name("testentity_template");
            var templateTwo = new Name("secondentity_template");

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsIndexTemplate(Arg.Is(templateOne)).Returns(templateOneExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(elasticMap.TemplateName));
            });
            esIndicesClient.ExistsIndexTemplate(Arg.Is(templateTwo)).Returns(templateTwoExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(secondEntityMap.TemplateName));
            });
            esIndicesClient.PutIndexTemplate(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>()).Returns(templateCreatedMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap, secondEntityMap, thirdEntityMap]);

            var result = esWrapper.CreateAllMappedIndexTemplate();
            Assert.That(result, Is.Not.Null);
            Assert.That(result.TotalDefinedTemplates, Is.EqualTo(2));
            Assert.That(result.CreatedTemplates, Is.EqualTo(1));
            Assert.That(result.AlreadyExistingTemplates, Is.EqualTo(1));
            Assert.That(result.FailedTemplates, Is.EqualTo(0));

            esIndicesClient.Received(2).ExistsIndexTemplate(Arg.Any<Name>());
            esIndicesClient.Received(1).PutIndexTemplate(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>());
        }

        [Test]
        public void Verify_CreateAllMappedIndexTemplate_WorksCorrectlyWithFailedTemplate()
        {
            var elasticMap = new TestEntityMap();
            var secondEntityMap = new TestSecondEntityMap();
            var thirdEntityMap = new TestThirdEntityMap();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var templateOneExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 200);
            var templateTwoExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 404, false);
            var templateFailedMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateResponse() { Acknowledged = false }, 400, false);

            var templateOne = new Name("testentity_template");
            var templateTwo = new Name("secondentity_template");

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsIndexTemplate(Arg.Is(templateOne)).Returns(templateOneExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(elasticMap.TemplateName));
            });
            esIndicesClient.ExistsIndexTemplate(Arg.Is(templateTwo)).Returns(templateTwoExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(secondEntityMap.TemplateName));
            });
            esIndicesClient.PutIndexTemplate(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>()).Returns(templateFailedMockedResponse);

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap, secondEntityMap, thirdEntityMap]);

            var result = esWrapper.CreateAllMappedIndexTemplate();
            Assert.That(result, Is.Not.Null);
            Assert.That(result.TotalDefinedTemplates, Is.EqualTo(2));
            Assert.That(result.CreatedTemplates, Is.EqualTo(0));
            Assert.That(result.AlreadyExistingTemplates, Is.EqualTo(1));
            Assert.That(result.FailedTemplates, Is.EqualTo(1));

            esIndicesClient.Received(2).ExistsIndexTemplate(Arg.Any<Name>());
            esIndicesClient.Received(1).PutIndexTemplate(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>());
        }

        [Test]
        public async Task Verify_CreateAllMappedIndexTemplateAsync_WorksCorrectly()
        {
            var loggerFactory = Substitute.For<ILoggerFactory>();
            var elasticMap = new TestEntityMap();
            var secondEntityMap = new TestSecondEntityMap();
            var thirdEntityMap = new TestThirdEntityMap();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var templateOneExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 200);
            var templateTwoExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 404, false);
            var templateCreatedMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateResponse() { Acknowledged = true }, 201);

            var templateOne = new Name("testentity_template");
            var templateTwo = new Name("secondentity_template");

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsIndexTemplateAsync(Arg.Is(templateOne)).Returns(templateOneExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(elasticMap.TemplateName));
            });
            esIndicesClient.ExistsIndexTemplateAsync(Arg.Is(templateTwo)).Returns(templateTwoExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(secondEntityMap.TemplateName));
            });
            esIndicesClient.PutIndexTemplateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>()).Returns(templateCreatedMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap, secondEntityMap, thirdEntityMap]);

            var result = await esWrapper.CreateAllMappedIndexTemplateAsync();
            Assert.That(result, Is.Not.Null);
            Assert.That(result.TotalDefinedTemplates, Is.EqualTo(2));
            Assert.That(result.CreatedTemplates, Is.EqualTo(1));
            Assert.That(result.AlreadyExistingTemplates, Is.EqualTo(1));
            Assert.That(result.FailedTemplates, Is.EqualTo(0));

            await esIndicesClient.Received(2).ExistsIndexTemplateAsync(Arg.Any<Name>());
            await esIndicesClient.Received(1).PutIndexTemplateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>());
        }

        [Test]
        public async Task Verify_CreateAllMappedIndexTemplateAsync_WorksCorrectlyWithFailedTemplate()
        {
            var loggerFactory = Substitute.For<ILoggerFactory>();
            var elasticMap = new TestEntityMap();
            var secondEntityMap = new TestSecondEntityMap();
            var thirdEntityMap = new TestThirdEntityMap();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            var templateOneExistMockedResponse = TestableResponseFactory.CreateSuccessfulResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 200);
            var templateTwoExistMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsIndexTemplateResponse(), 404, false);
            var templateFailedMockedResponse = TestableResponseFactory.CreateResponse(new Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateResponse() { Acknowledged = false }, 400, false);

            var templateOne = new Name("testentity_template");
            var templateTwo = new Name("secondentity_template");

            var esIndicesClient = Substitute.For<Elastic.Clients.Elasticsearch.IndexManagement.IndicesNamespacedClient>();
            esIndicesClient.ExistsIndexTemplateAsync(Arg.Is(templateOne)).Returns(templateOneExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(elasticMap.TemplateName));
            });
            esIndicesClient.ExistsIndexTemplateAsync(Arg.Is(templateTwo)).Returns(templateTwoExistMockedResponse).AndDoes(x =>
            {
                var name = x.Arg<Name>();
                Assert.That(name.ToString(), Is.EqualTo(secondEntityMap.TemplateName));
            });
            esIndicesClient.PutIndexTemplateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>()).Returns(templateFailedMockedResponse);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Indices.Returns(esIndicesClient);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap, secondEntityMap, thirdEntityMap]);

            var result = await esWrapper.CreateAllMappedIndexTemplateAsync();
            Assert.That(result, Is.Not.Null);
            Assert.That(result.TotalDefinedTemplates, Is.EqualTo(2));
            Assert.That(result.CreatedTemplates, Is.EqualTo(0));
            Assert.That(result.AlreadyExistingTemplates, Is.EqualTo(1));
            Assert.That(result.FailedTemplates, Is.EqualTo(1));

            await esIndicesClient.Received(2).ExistsIndexTemplateAsync(Arg.Any<Name>());
            await esIndicesClient.Received(1).PutIndexTemplateAsync(Arg.Any<Elastic.Clients.Elasticsearch.IndexManagement.PutIndexTemplateRequestDescriptor>());
        }

        [Test]
        public void Verify_Dispose_ForceClientRecreation()
        {
            int logActionCalls = 0;

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);
            elasticConfig.LogAction.Returns((loglevel, exception, message, args) =>
            {
                logActionCalls++;
            });

            var esWrapper = new ElasticWrapper(loggerFactory, elasticConfig, []);

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

        [Test]
        public void Verify_Aggregations_WorksCorrectly()
        {
            List<TestEntity> dataList = new()
            {
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test01",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow.Date.AddDays(-2),
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test02",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow.Date.AddDays(-2),
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test03",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow.Date.AddDays(-1),
                    Active = true
                }
            };

            var elasticMap = new TestEntityMap();

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            Dictionary<string, IAggregate> aggregationResponse = new Dictionary<string, IAggregate>
            {
                {
                    "group_by_creation_date",
                    new DateHistogramAggregate
                    {
                        Buckets = dataList.GroupBy(x => x.CreationTime.ToString("yyyy-MM-dd")).Select(x => new DateHistogramBucket
                        {
                            KeyAsString = x.Key,
                            DocCount = x.Count()
                        }).ToList()
                    }
                }
            };
            var response = new SearchResponse<TestEntity>
            {
                Aggregations = new Elastic.Clients.Elasticsearch.Aggregations.AggregateDictionary(aggregationResponse)
            };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.Search(Arg.Any<SearchRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            var esQueryParameters = new ElasticQueryParameters<TestEntity>
            {
                AggregationDescriptors = new FluentDescriptorDictionary<string, AggregationDescriptor<TestEntity>>()
                {
                    {
                        "group_by_creation_date",
                        new AggregationDescriptor<TestEntity>()
                            .DateHistogram(h => h
                                .Field(f => f.CreationTime)
                                .CalendarInterval(CalendarInterval.Day)
                                .Format("yyyy-MM-dd")
                                .MinDocCount(0)
                            )
                    }
                },
                QueryDescriptor = new Elastic.Clients.Elasticsearch.QueryDsl.QueryDescriptor<TestEntity>(),
            };

            var itemList = esWrapper.Aggregate(null, esQueryParameters);
            esClient.Received(1).Search(Arg.Any<SearchRequestDescriptor<TestEntity>>());
            Assert.That(itemList, Is.Not.Null);
            Assert.That(itemList!.Count(), Is.EqualTo(1));

            DateHistogramAggregate dateHistogramAggregate = (DateHistogramAggregate)itemList!["group_by_creation_date"];
            Assert.That(dateHistogramAggregate.Buckets.Count, Is.EqualTo(2));
        }

        [Test]
        public async Task Verify_AggregationsAsync_WorksCorrectly()
        {
            List<TestEntity> dataList = new()
            {
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test01",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow.Date.AddDays(-2),
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test02",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow.Date.AddDays(-2),
                    Active = true
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    Name = "Test03",
                    GroupName = "Group01",
                    CreationTime = DateTime.UtcNow.Date.AddDays(-1),
                    Active = true
                }
            };

            var elasticMap = new TestEntityMap();

            var loggerFactory = Substitute.For<ILoggerFactory>();

            var elasticConfig = Substitute.For<IElasticConfig>();
            elasticConfig.ConnectionsPool.Returns([new Uri("http://localhost:9200")]);

            Dictionary<string, IAggregate> aggregationResponse = new Dictionary<string, IAggregate>
            {
                {
                    "group_by_creation_date",
                    new DateHistogramAggregate
                    {
                        Buckets = dataList.GroupBy(x => x.CreationTime.ToString("yyyy-MM-dd")).Select(x => new DateHistogramBucket
                        {
                            KeyAsString = x.Key,
                            DocCount = x.Count()
                        }).ToList()
                    }
                }
            };
            var response = new SearchResponse<TestEntity>
            {
                Aggregations = new Elastic.Clients.Elasticsearch.Aggregations.AggregateDictionary(aggregationResponse)
            };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(response, 201);

            var esClient = Substitute.For<ElasticsearchClient>();
            esClient.SearchAsync(Arg.Any<SearchRequestDescriptor<TestEntity>>()).Returns(mockedResponse);

            var esWrapper = new ElasticWrapper(esClient, loggerFactory, elasticConfig, [elasticMap]);

            var esQueryParameters = new ElasticQueryParameters<TestEntity>
            {
                AggregationDescriptors = new FluentDescriptorDictionary<string, AggregationDescriptor<TestEntity>>()
                {
                    {
                        "group_by_creation_date",
                        new AggregationDescriptor<TestEntity>()
                            .DateHistogram(h => h
                                .Field(f => f.CreationTime)
                                .CalendarInterval(CalendarInterval.Day)
                                .Format("yyyy-MM-dd")
                                .MinDocCount(0)
                            )
                    }
                },
                QueryDescriptor = new Elastic.Clients.Elasticsearch.QueryDsl.QueryDescriptor<TestEntity>(),
            };

            var itemList = await esWrapper.AggregateAsync(null, esQueryParameters);
            await esClient.Received(1).SearchAsync(Arg.Any<SearchRequestDescriptor<TestEntity>>());
            Assert.That(itemList, Is.Not.Null);
            Assert.That(itemList!.Count(), Is.EqualTo(1));

            DateHistogramAggregate dateHistogramAggregate = (DateHistogramAggregate)itemList!["group_by_creation_date"];
            Assert.That(dateHistogramAggregate.Buckets.Count, Is.EqualTo(2));
        }
    }
}
