using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Interfaces;
using FluentHelper.ElasticSearch.Tests.Support;
using NSubstitute;
using NUnit.Framework;

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

            expectedIndexName += testEntityMap.IndexCalculator.CalcEntityIndex(testEntityInstance);
            expectedIndexName = expectedIndexName.ToLower();

            string indexName = elasticWrapper.GetIndexName(testEntityInstance);
            Assert.That(indexName, Is.Not.Null);
            Assert.That(indexName.Length, Is.GreaterThan(0));
            Assert.That(indexName, Is.EqualTo(expectedIndexName));
        }

        [TestCase("2023-06-08", "2023-06-08")]
        [TestCase("2023-06-08", "2023-06-10")]
        [TestCase("", "2023-06-08")]
        [TestCase("2023-06-08", "")]
        [TestCase("", "")]
        public void Verify_IndexNamesForQueries_AreCalculated_Correctly(string startTime, string endTime)
        {
            string prefix = "pre";
            string suffix = "suf";

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

            var queryIndexes = testEntityMap.IndexCalculator.CalcQueryIndex(testFilter);

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
            elasticMap.GetMapType().Returns(typeof(IElasticMap));
            elasticMap.When(x => x.ApplySpecialMap(Arg.Any<ElasticsearchClientSettings>())).Do(x =>
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
            elasticConfig.CertificateFingerprint.Returns("");
            elasticConfig.BasicAuthentication.Returns(basicAuth);
            elasticConfig.RequestTimeout.Returns(TimeSpan.FromSeconds(60));
            elasticConfig.LogAction.Returns((x, y, z, k) => { logActionCalled = true; });

            var elasticMap = Substitute.For<IElasticMap>();
            elasticMap.IdPropertyName.Returns("Id");
            elasticMap.GetMapType().Returns(typeof(IElasticMap));
            elasticMap.When(x => x.ApplySpecialMap(Arg.Any<ElasticsearchClientSettings>())).Do(x =>
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

            var createResponse = new IndexResponse { Result = Result.Created };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(createResponse, 201);

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

            var createResponse = new IndexResponse { Result = Result.Created };
            var mockedResponse = TestableResponseFactory.CreateSuccessfulResponse(createResponse, 201);

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
    }
}
