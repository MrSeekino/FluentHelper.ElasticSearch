using FluentHelper.ElasticSearch.IndexCalculators;
using FluentHelper.ElasticSearch.Tests.Support;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace FluentHelper.ElasticSearch.Tests
{
    [TestFixture]
    public class BasicIndexCalculatorTests
    {
        [Test]
        public void CalcEntityIndex_Returns_EmptyString()
        {
            var testEntity = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test",
                GroupName = "Group",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            var basicIndexCalculator = new BasicIndexCalculator<TestEntity>();

            string entityIndex = basicIndexCalculator.CalcEntityIndex(testEntity);
            ClassicAssert.AreEqual(string.Empty, entityIndex);
        }

        [Test]
        public void CalcQueryIndex_Returns_EmptyString_WhenFixedIndex()
        {
            var basicIndexCalculator = new BasicIndexCalculator<TestEntity>(true);

            var queryIndexes = basicIndexCalculator.CalcQueryIndex(null);
            ClassicAssert.IsNotNull(queryIndexes);
            ClassicAssert.AreEqual(0, queryIndexes.Count());
        }

        [Test]
        public void CalcQueryIndex_Returns_Start_WhenNotFixedIndex()
        {
            var basicIndexCalculator = new BasicIndexCalculator<TestEntity>(false);

            var queryIndexes = basicIndexCalculator.CalcQueryIndex(null);
            ClassicAssert.IsNotNull(queryIndexes);
            ClassicAssert.AreEqual(1, queryIndexes.Count());
            ClassicAssert.AreEqual("*", queryIndexes.First());
        }
    }
}
