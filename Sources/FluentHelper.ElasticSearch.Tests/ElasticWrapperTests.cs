using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Interfaces;
using FluentHelper.ElasticSearch.Tests.Support;
using Moq;
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
            Mock<IElasticConfig> elasticConfigMock = new Mock<IElasticConfig>();
            elasticConfigMock.Setup(x => x.IndexPrefix).Returns(prefix);
            elasticConfigMock.Setup(x => x.IndexSuffix).Returns(suffix);

            var testEntityMap = new TestEntityMap();

            ElasticWrapper elasticWrapper = new ElasticWrapper(elasticConfigMock.Object, new List<IElasticMap>() { testEntityMap });

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

            string indexName = elasticWrapper.GetIndexName(testEntityMap, testEntityInstance);
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

            Mock<IElasticConfig> elasticConfigMock = new Mock<IElasticConfig>();
            elasticConfigMock.Setup(x => x.IndexPrefix).Returns(prefix);
            elasticConfigMock.Setup(x => x.IndexSuffix).Returns(suffix);

            var testEntityMap = new TestEntityMap();

            ElasticWrapper elasticWrapper = new ElasticWrapper(elasticConfigMock.Object, new List<IElasticMap>() { testEntityMap });

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

            string indexForQuery = elasticWrapper.GetIndexNamesForQueries(testEntityMap, testFilter);
            Assert.That(indexForQuery, Is.Not.Null);
            Assert.That(indexForQuery.Length, Is.GreaterThan(0));
            Assert.That(indexForQuery, Is.EqualTo(indexesForQuery));
        }
    }
}
