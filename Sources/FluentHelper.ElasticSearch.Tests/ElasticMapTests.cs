using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.IndexCalculators;
using FluentHelper.ElasticSearch.Tests.Support;
using NSubstitute;
using NUnit.Framework;

namespace FluentHelper.ElasticSearch.Tests
{
    [TestFixture]
    public class ElasticMapTests
    {
        [Test]
        public void Verify_MapVerification_ThrowsWhen_BaseIndexName_IsNotSet()
        {
            var indexCalculator = Substitute.For<IElasticIndexCalculator<TestEntity>>();

            var elasticMap = Substitute.For<ElasticMap<TestEntity>>();
            elasticMap.IdPropertyName.Returns("Id");
            elasticMap.IndexCalculator.Returns(indexCalculator);

            Assert.Throws<InvalidOperationException>(elasticMap.Verify);
        }

        [Test]
        public void Verify_MapVerification_ThrowsWhen_IndexCalculator_IsNotSet()
        {
            IElasticIndexCalculator<TestEntity>? indexCalculator = null;

            var elasticMap = Substitute.For<ElasticMap<TestEntity>>();
            elasticMap.IdPropertyName.Returns("Id");
            elasticMap.IndexCalculator.Returns(indexCalculator);
            elasticMap.BaseIndexName.Returns("BasicIndexName");

            Assert.Throws<InvalidOperationException>(elasticMap.Verify);
        }

        [Test]
        public void Verify_MapVerification_ThrowsWhen_IdProperty_IsNotSet()
        {
            var indexCalculator = Substitute.For<IElasticIndexCalculator<TestEntity>>();

            var elasticMap = Substitute.For<ElasticMap<TestEntity>>();
            elasticMap.BaseIndexName.Returns("BasicIndexName");
            elasticMap.IndexCalculator.Returns(indexCalculator);

            Assert.Throws<InvalidOperationException>(elasticMap.Verify);
        }
    }
}
