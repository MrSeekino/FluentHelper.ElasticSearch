using Elastic.Clients.Elasticsearch.Mapping;
using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.IndexCalculators;
using FluentHelper.ElasticSearch.Tests.Support;
using FluentHelper.ElasticSearch.TestsSupport;
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

            var indexPostfixByFilter = emptyMap.IndexCalculator!.GetIndexPostfixByFilter(null!);
            Assert.That(indexPostfixByFilter.Count(), Is.EqualTo(1));
            Assert.That(indexPostfixByFilter.First(), Is.EqualTo("*"));
        }

        [Test]
        public void Verify_SetBasicIndexCalculator_WorksCorrectly_WithSpecificConfiguration()
        {
            EmptyMap emptyMap = new EmptyMap();
            Assert.That(emptyMap.IndexCalculator, Is.Null);

            emptyMap.TestSetBasicIndexCalculator(x => x.WithFixedIndexName());
            Assert.That(emptyMap.IndexCalculator, Is.Not.Null);

            var indexPostfixByFilter = emptyMap.IndexCalculator!.GetIndexPostfixByFilter(null!);
            Assert.That(indexPostfixByFilter.Count(), Is.EqualTo(0));
        }

        [Test]
        public void Verify_SetFilterableIndexCalculator_WorksCorrectly()
        {
            EmptyMap emptyMap = new EmptyMap();
            Assert.That(emptyMap.IndexCalculator, Is.Null);

            emptyMap.TestSetFilterableIndexCalculator<string>(x =>
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

        [Test]
        public void Verify_EnableTemplateCreation_WorksCorrectly()
        {
            string templateName = "testTemplateName";

            EmptyMap emptyMap = new EmptyMap();
            Assert.That(emptyMap.CreateTemplate, Is.EqualTo(false));
            Assert.That(emptyMap.TemplateName, Is.EqualTo(string.Empty));

            emptyMap.TestEnableTemplateCreation(templateName);
            Assert.That(emptyMap.CreateTemplate, Is.EqualTo(true));
            Assert.That(emptyMap.TemplateName, Is.EqualTo(templateName));
        }

        [Test]
        public void Verify_Settings_WorksCorrectly()
        {
            EmptyMap emptyMap = new EmptyMap();
            Assert.That(emptyMap.IndexSettings, Is.Null);

            emptyMap.TestSettings(x => x.NumberOfShards(1).NumberOfReplicas(0));
            Assert.That(emptyMap.IndexSettings, Is.Not.Null);
        }

        [Test]
        public void Verify_Prop_WorksCorrectly()
        {
            EmptyMap emptyMap = new EmptyMap();
            Assert.That(emptyMap.IndexMappings, Is.Null);

            emptyMap.TestProp<KeywordProperty>(e => e.Name);
            emptyMap.TestProp<TextProperty>(e => e.Description);

            Assert.That(emptyMap.IndexMappings, Is.Not.Null);
            Assert.That(emptyMap.IndexMappings!.Count(), Is.EqualTo(2));

            var mapDictionary = emptyMap.IndexMappings!.ToDictionary(x => x.Key, y => y.Value);
            Assert.That(mapDictionary.Keys.Any(k => k.Expression.ToString() == "e => e.Name"), Is.EqualTo(true));
            Assert.That(mapDictionary.Keys.Any(k => k.Expression.ToString() == "e => e.Description"), Is.EqualTo(true));
            Assert.That(mapDictionary.SingleOrDefault(k => k.Key.Expression.ToString() == "e => e.Name").Value.Type, Is.EqualTo("keyword"));
            Assert.That(mapDictionary.SingleOrDefault(k => k.Key.Expression.ToString() == "e => e.Description").Value.Type, Is.EqualTo("text"));
        }
    }
}
