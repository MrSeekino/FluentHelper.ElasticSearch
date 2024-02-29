using Elastic.Clients.Elasticsearch.Core.Search;
using FluentHelper.ElasticSearch.QueryParameters;
using FluentHelper.ElasticSearch.Tests.Support;
using NUnit.Framework;

namespace FluentHelper.ElasticSearch.Tests
{
    [TestFixture]
    public class ElasticQueryParametersBuilderTests
    {
        [Test]
        public void Verify_Params_AreCorrectWhenOnDefault()
        {
            var builder = ElasticQueryParametersBuilder<TestEntity>.Create();

            var queryParameters = builder.Build();
            Assert.That(queryParameters.QueryDescriptor, Is.Null);
            Assert.That(queryParameters.SourceConfig, Is.Null);
            Assert.That(queryParameters.SortOptionsDescriptor, Is.Null);
            Assert.That(queryParameters.Skip, Is.EqualTo(0));
            Assert.That(queryParameters.Take, Is.EqualTo(10000));
        }

        [Test]
        public void Verify_Skip_IsCorrectlyApplied()
        {
            var builder = ElasticQueryParametersBuilder<TestEntity>.Create()
                            .Skip(10);

            var queryParameters = builder.Build();
            Assert.That(queryParameters.Skip, Is.EqualTo(10));
        }

        [Test]
        public void Verify_Take_IsCorrectlyApplied()
        {
            var builder = ElasticQueryParametersBuilder<TestEntity>.Create()
                            .Take(10);

            var queryParameters = builder.Build();
            Assert.That(queryParameters.Take, Is.EqualTo(10));
        }

        [Test]
        public void Verify_QueryDescriptor_IsCorrectlyApplied()
        {
            var builder = ElasticQueryParametersBuilder<TestEntity>.Create()
                            .Query(x => x.Match(m => m.Query("test")));

            var queryParameters = builder.Build();
            Assert.That(queryParameters.QueryDescriptor, Is.Not.Null);
        }

        [Test]
        public void Verify_SortDescriptor_IsCorrectlyApplied()
        {
            var builder = ElasticQueryParametersBuilder<TestEntity>.Create()
                            .Sort(x => x.Name, Elastic.Clients.Elasticsearch.SortOrder.Asc);

            var queryParameters = builder.Build();
            Assert.That(queryParameters.SortOptionsDescriptor, Is.Not.Null);
        }

        [Test]
        public void Verify_SortDescriptor_IsCorrectlyApplied_WithExpression()
        {
            var builder = ElasticQueryParametersBuilder<TestEntity>.Create()
                            .Sort(x =>
                            {
                                x.Field(e => e.Name);
                            });

            var queryParameters = builder.Build();
            Assert.That(queryParameters.SortOptionsDescriptor, Is.Not.Null);
        }

        [Test]
        public void Verify_SourceFilter_IsCorrectlyApplied()
        {
            var builder = ElasticQueryParametersBuilder<TestEntity>.Create()
                            .SourceFilter(new SourceFilter());

            var queryParameters = builder.Build();
            Assert.That(queryParameters.SourceConfig, Is.Not.Null);
        }

        [Test]
        public void Verify_SourceFilter_IsCorrectlyApplied_WhenUsingInclude()
        {
            var builder = ElasticQueryParametersBuilder<TestEntity>.Create()
                            .Include(f => f.Name);

            var queryParameters = builder.Build();
            Assert.That(queryParameters.SourceConfig, Is.Not.Null);
        }

        [Test]
        public void Verify_SourceFilter_IsCorrectlyApplied_WhenUsingExclude()
        {
            var builder = ElasticQueryParametersBuilder<TestEntity>.Create()
                            .Exclude(f => f.Name);

            var queryParameters = builder.Build();
            Assert.That(queryParameters.SourceConfig, Is.Not.Null);
        }
    }
}
