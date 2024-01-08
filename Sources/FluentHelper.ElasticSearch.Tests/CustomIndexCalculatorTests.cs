using FluentHelper.ElasticSearch.IndexCalculators;
using FluentHelper.ElasticSearch.Tests.Support;
using NUnit.Framework;

namespace FluentHelper.ElasticSearch.Tests
{
    [TestFixture]
    public class CustomIndexCalculatorTests
    {
        readonly CustomIndexCalculator<TestEntity, DateTime[]> _customIndexCalculator;

        public CustomIndexCalculatorTests()
        {
            _customIndexCalculator = new CustomIndexCalculator<TestEntity, DateTime[]>(entity => $"{entity.CreationTime:yyyy.MM.dd}", filter =>
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
            Assert.AreEqual(expectedEntityIndex, entityIndex);
        }

        [Test]
        public void CalcQueryIndex_When_FilterIsValid_Returns_ValidListOfIndexes()
        {
            DateTime[] filter = new DateTime[2] { new DateTime(2023, 12, 1), new DateTime(2024, 1, 1) };

            var queryIndexes = _customIndexCalculator.CalcQueryIndex(filter);
            Assert.IsNotNull(queryIndexes);
            Assert.AreEqual(2, queryIndexes.Count());

            Assert.True(queryIndexes.Contains("2023.12.01"));
            Assert.True(queryIndexes.Contains("2024.01.01"));
        }

        [Test]
        public void CalcQueryIndex_When_FilterIsNull_Returns_Star()
        {
            var queryIndexes = _customIndexCalculator.CalcQueryIndex(null);
            Assert.IsNotNull(queryIndexes);
            Assert.AreEqual(1, queryIndexes.Count());
            Assert.AreEqual("*", queryIndexes.First());
        }

        [Test]
        public void CalcQueryIndex_When_FilterIsInvalid_Throws()
        {
            int[] badFilter = new int[1] { 12 };

            Assert.Throws<InvalidCastException>(() => _customIndexCalculator.CalcQueryIndex(badFilter));
        }
    }
}
