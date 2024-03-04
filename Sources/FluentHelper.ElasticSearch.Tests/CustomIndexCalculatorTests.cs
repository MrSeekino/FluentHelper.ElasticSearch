using FluentHelper.ElasticSearch.IndexCalculators;
using FluentHelper.ElasticSearch.Tests.Support;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace FluentHelper.ElasticSearch.Tests
{
    [TestFixture]
    public class CustomIndexCalculatorTests
    {
        readonly ICustomIndexCalculator<TestEntity, DateTime[]> _customIndexCalculator;

        public CustomIndexCalculatorTests()
        {
            _customIndexCalculator = CustomIndexCalculator<TestEntity, DateTime[]>.Create();

            _customIndexCalculator.WithPostfixByEntity(entity => $"{entity.CreationTime:yyyy.MM.dd}");

            _customIndexCalculator.WithPostfixByFilter(filter =>
            {
                if (filter == null)
                    return null;

                List<string> postFixIndexes = new List<string>();
                foreach (var date in filter)
                    postFixIndexes.Add($"{date:yyyy.MM.dd}");

                return postFixIndexes;
            });
        }

        [Test]
        public void CalcEntityIndex_Returns_CorrectIndex()
        {
            var testEntity = new TestEntity
            {
                Id = Guid.NewGuid(),
                Name = "Test",
                GroupName = "Group",
                CreationTime = DateTime.UtcNow,
                Active = true
            };

            string expectedEntityIndex = $"{testEntity.CreationTime:yyyy.MM.dd}";

            string entityIndex = _customIndexCalculator.CalcEntityIndex(testEntity);
            ClassicAssert.AreEqual(expectedEntityIndex, entityIndex);
        }

        [Test]
        public void CalcQueryIndex_When_FilterIsValid_Returns_ValidListOfIndexes()
        {
            DateTime[] filter = [new DateTime(2023, 12, 1), new DateTime(2024, 1, 1)];

            var queryIndexes = _customIndexCalculator.CalcQueryIndex(filter);
            ClassicAssert.IsNotNull(queryIndexes);
            ClassicAssert.AreEqual(2, queryIndexes.Count());

            ClassicAssert.True(queryIndexes.Contains("2023.12.01"));
            ClassicAssert.True(queryIndexes.Contains("2024.01.01"));
        }

        [Test]
        public void CalcQueryIndex_When_FilterIsNull_Returns_Star()
        {
            var queryIndexes = _customIndexCalculator.CalcQueryIndex(null);
            ClassicAssert.IsNotNull(queryIndexes);
            ClassicAssert.AreEqual(1, queryIndexes.Count());
            ClassicAssert.AreEqual("*", queryIndexes.First());
        }

        [Test]
        public void CalcQueryIndex_When_FilterIsInvalid_Throws()
        {
            int[] badFilter = [12];

            Assert.Throws<InvalidCastException>(() => _customIndexCalculator.CalcQueryIndex(badFilter));
        }
    }
}
