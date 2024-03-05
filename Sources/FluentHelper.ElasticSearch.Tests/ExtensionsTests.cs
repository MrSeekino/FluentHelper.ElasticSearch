using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.QueryParameters;
using FluentHelper.ElasticSearch.TestsSupport;
using Microsoft.CSharp.RuntimeBinder;
using NUnit.Framework;

namespace FluentHelper.ElasticSearch.Tests
{
    [TestFixture]
    public class ExtensionsTests
    {
        [TestCase(false)]
        [TestCase(true)]
        public void Verify_ThrowIfIndexInvalid_Throws_WhenIndexIsTooLong(bool isRetrieveQuery)
        {
            string alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

            string indexName = string.Empty;
            for (int i = 0; i < 260; i++)
                indexName += alphabet[Random.Shared.Next(0, alphabet.Length)];

            Assert.Throws<ArgumentOutOfRangeException>(() => indexName.ThrowIfIndexInvalid(false));
        }

        [TestCase("testdata*", false)]
        [TestCase("*testdata", false)]
        [TestCase("*test*data", false)]
        [TestCase(".", false)]
        [TestCase("..", false)]
        [TestCase("-testdata", false)]
        [TestCase("_testdata", false)]
        [TestCase("+testdata", false)]
        [TestCase("test\\data", false)]
        [TestCase("\\testdata", false)]
        [TestCase("testdata\\", false)]
        [TestCase("test/data", false)]
        [TestCase("/testdata", false)]
        [TestCase("testdata/", false)]
        [TestCase("test?data", false)]
        [TestCase("?testdata", false)]
        [TestCase("testdata?", false)]
        [TestCase("test\"data", false)]
        [TestCase("\"testdata", false)]
        [TestCase("testdata\"", false)]
        [TestCase("test<data", false)]
        [TestCase("<testdata", false)]
        [TestCase("testdata<", false)]
        [TestCase("test>data", false)]
        [TestCase(">testdata", false)]
        [TestCase("testdata>", false)]
        [TestCase("test|data", false)]
        [TestCase("|testdata", false)]
        [TestCase("testdata|", false)]
        [TestCase("test data", false)]
        [TestCase(" testdata", false)]
        [TestCase("testdata ", false)]
        [TestCase("test#data", false)]
        [TestCase("#testdata", false)]
        [TestCase("testdata#", false)]
        [TestCase(".", true)]
        [TestCase("..", true)]
        [TestCase("-testdata", true)]
        [TestCase("_testdata", true)]
        [TestCase("+testdata", true)]
        [TestCase("test\\data", true)]
        [TestCase("\\testdata", true)]
        [TestCase("testdata\\", true)]
        [TestCase("test/data", true)]
        [TestCase("/testdata", true)]
        [TestCase("testdata/", true)]
        [TestCase("test?data", true)]
        [TestCase("?testdata", true)]
        [TestCase("testdata?", true)]
        [TestCase("test\"data", true)]
        [TestCase("\"testdata", true)]
        [TestCase("testdata\"", true)]
        [TestCase("test<data", true)]
        [TestCase("<testdata", true)]
        [TestCase("testdata<", true)]
        [TestCase("test>data", true)]
        [TestCase(">testdata", true)]
        [TestCase("testdata>", true)]
        [TestCase("test|data", true)]
        [TestCase("|testdata", true)]
        [TestCase("testdata|", true)]
        [TestCase("test data", true)]
        [TestCase(" testdata", true)]
        [TestCase("testdata ", true)]
        [TestCase("test#data", true)]
        [TestCase("#testdata", true)]
        [TestCase("testdata#", true)]
        public void Verify_ThrowIfIndexInvalid_Throws_WhenIndexIsNotValid(string indexName, bool isRetrieveQuery)
        {
            Assert.Throws<FormatException>(() => indexName.ThrowIfIndexInvalid(isRetrieveQuery));
        }

        [TestCase("Description", null)]
        [TestCase("Name", "TestName")]
        public void Verify_GetFieldValue_ReturnCorrectValue(string fieldName, object? expectedValue)
        {
            var testEntity = new TestEntity
            {
                Active = true,
                Id = Guid.NewGuid(),
                Name = "TestName",
                CreationTime = DateTime.UtcNow,
                GroupName = "TestGroup"
            };

            object? fieldValue = testEntity.GetFieldValue(fieldName);
            Assert.That(expectedValue, Is.EqualTo(fieldValue));
        }

        [Test]
        public void Verify_GetFieldValue_ReturnNullWhenInputIsNull()
        {
            TestEntity testEntity = null!;

            object? fieldValue = testEntity.GetFieldValue("Name");
            Assert.That(fieldValue, Is.Null);
        }

        [Test]
        public void Verify_GetExpandoObject_ThrowsWhenElasticInputUpdaterIsNull()
        {
            var testEntity = new TestEntity
            {
                Active = true,
                Id = Guid.NewGuid(),
                Name = "TestName",
                CreationTime = DateTime.UtcNow,
                GroupName = "TestGroup"
            };

            Assert.Throws<NullReferenceException>(() => testEntity.GetExpandoObject(null!));
        }

        [Test]
        public void Verify_GetExpandoObject_ReturnsValidValue()
        {
            var testEntity = new TestEntity
            {
                Active = true,
                Id = Guid.NewGuid(),
                Name = "TestName",
                CreationTime = DateTime.UtcNow,
                GroupName = "TestGroup"
            };

            var elasticFieldUpdater = new ElasticFieldUpdater<TestEntity>("Id")
                                            .Update(x => x.Name)
                                            .Update(x => x.CreationTime);

            dynamic expandoData = testEntity.GetExpandoObject(elasticFieldUpdater!);
            Assert.That(expandoData, Is.Not.Null);

            Assert.DoesNotThrow(() =>
            {
                string name = expandoData.name;
                Assert.That(name, Is.EqualTo(testEntity.Name));
            });

            Assert.DoesNotThrow(() =>
            {
                DateTime creationTime = expandoData.creationTime;
                Assert.That(creationTime, Is.EqualTo(testEntity.CreationTime));
            });

            Assert.Throws<RuntimeBinderException>(() => { var groupName = expandoData.groupName; });
            Assert.Throws<RuntimeBinderException>(() => { var active = expandoData.active; });
        }
    }
}
