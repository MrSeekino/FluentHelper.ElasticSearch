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
        public void Verify_SetBaseIndexName_WorksCorrectly()
        {
            string baseIndexName = "TestBaseIndexName";

            EmptyMap emptyMap = new EmptyMap();
            Assert.That(emptyMap.BaseIndexName, Is.EqualTo(string.Empty));

            emptyMap.TestSetBaseIndexName(baseIndexName);
            Assert.That(emptyMap.BaseIndexName, Is.EqualTo(baseIndexName));
        }

        [Test]
        public void Verify_SetIndexCalculator_WorksCorrectly()
        {
            var indexCalculator = Substitute.For<IElasticIndexCalculator<EmptyEntity>>();

            EmptyMap emptyMap = new EmptyMap();
            Assert.That(emptyMap.IndexCalculator, Is.Null);

            emptyMap.TestSetIndexCalculator(indexCalculator);
            Assert.That(emptyMap.IndexCalculator, Is.Not.Null);
            Assert.That(emptyMap.IndexCalculator, Is.EqualTo(indexCalculator));
        }

        [Test]
        public void Verify_SetBasicIndexCalculator_WorksCorrectly()
        {
            EmptyMap emptyMap = new EmptyMap();
            Assert.That(emptyMap.IndexCalculator, Is.Null);

            emptyMap.TestSetBasicIndexCalculator();
            Assert.That(emptyMap.IndexCalculator, Is.Not.Null);
        }

        [Test]
        public void Verify_SetCustomIndexCalculator_WorksCorrectly()
        {
            EmptyMap emptyMap = new EmptyMap();
            Assert.That(emptyMap.IndexCalculator, Is.Null);

            emptyMap.TestSetCustomIndexCalculator<string>(x =>
            {
                x.WithPostfixByEntity(p => string.Empty);
                x.WithPostfixByFilter(f => Array.Empty<string>());
            });
            Assert.That(emptyMap.IndexCalculator, Is.Not.Null);
        }

        [Test]
        public void Verify_Id_WorksCorrectly()
        {
            EmptyMap emptyMap = new EmptyMap();
            Assert.That(emptyMap.IdPropertyName, Is.EqualTo(string.Empty));

            emptyMap.TestId(x => x.Name);
            Assert.That(emptyMap.IdPropertyName, Is.EqualTo("Name"));
        }

        [Test]
        public void Verify_GetMapType_WorksCorrectly()
        {
            EmptyMap emptyMap = new EmptyMap();
            Assert.That(emptyMap.GetMappingType(), Is.EqualTo(typeof(EmptyEntity)));
        }

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
