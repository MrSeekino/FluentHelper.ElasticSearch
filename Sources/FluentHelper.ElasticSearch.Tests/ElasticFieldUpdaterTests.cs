using FluentHelper.ElasticSearch.QueryParameters;
using FluentHelper.ElasticSearch.TestsSupport;
using NUnit.Framework;

namespace FluentHelper.ElasticSearch.Tests
{
    [TestFixture]
    public class ElasticFieldUpdaterTests
    {
        [Test]
        public void Verify_Update_ThrowsIfCalledAfterUpdateAllFields()
        {
            IElasticFieldUpdater<TestEntity> elasticFieldUpdater = new ElasticFieldUpdater<TestEntity>("Id").UpdateAllFields();
            Assert.Throws<InvalidOperationException>(() => elasticFieldUpdater.Update(x => x.Name));
        }

        [Test]
        public void Verify_UpdateAllFields_AddAllFields()
        {
            IElasticFieldUpdater<TestEntity> elasticFieldUpdater = new ElasticFieldUpdater<TestEntity>("Id").UpdateAllFields();

            var fieldList = elasticFieldUpdater.GetFieldList();
            Assert.That(fieldList.Count, Is.EqualTo(4));
            Assert.That(fieldList.Contains("GroupName"), Is.True);
            Assert.That(fieldList.Contains("Name"), Is.True);
            Assert.That(fieldList.Contains("CreationTime"), Is.True);
            Assert.That(fieldList.Contains("Active"), Is.True);
        }
    }
}
