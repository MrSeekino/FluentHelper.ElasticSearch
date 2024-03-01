using FluentHelper.ElasticSearch.Tests.Support;
using NUnit.Framework;

namespace FluentHelper.ElasticSearch.Tests
{
    [TestFixture]
    public class ElasticMapTests
    {
        [Test]
        public void Verify_MapVerificationThrowsWhenIdPropertyIsNotSet()
        {
            var testEntityMap = new TestEntityMap();
            Assert.Throws<InvalidOperationException>(testEntityMap.Verify);
        }
    }
}
