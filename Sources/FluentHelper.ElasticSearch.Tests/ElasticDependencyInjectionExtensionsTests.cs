using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Interfaces;
using FluentHelper.ElasticSearch.TestsSupport;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace FluentHelper.ElasticSearch.Tests
{
    [TestFixture]
    public class ElasticDependencyInjectionExtensionsTests
    {
        [Test]
        public void Verify_AddFluentElasticWrapper_WorksCorrectly()
        {
            string url = "http://localhost:9200";
            string username = "username";
            string password = "password";
            (string username, string password) basicAuth = new(username, password);
            string certFingerPrint = "abcdef";

            IServiceCollection serviceCollection = new ServiceCollection();
            serviceCollection.AddFluentElasticWrapper(esConfigBuilder =>
            {
                esConfigBuilder
                    .WithConnectionUri(url)
                    .WithAuthorization(certFingerPrint, basicAuth)
                    .WithDebugEnabled()
                    .WithMappingFromAssemblyOf<TestEntityMap>();
            });

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var elasticConfig = serviceProvider.GetRequiredService<IElasticConfig>();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.ConnectionsPool.Length, Is.EqualTo(1));
            Assert.That(elasticConfig.ConnectionsPool[0], Is.EqualTo(new Uri("http://localhost:9200")));
            Assert.That(elasticConfig.CertificateFingerprint, Is.EqualTo(certFingerPrint));
            Assert.That(elasticConfig.BasicAuthentication, Is.Not.Null);
            Assert.That(elasticConfig.BasicAuthentication!.Value.Username, Is.EqualTo(username));
            Assert.That(elasticConfig.BasicAuthentication!.Value.Password, Is.EqualTo(password));
            Assert.That(elasticConfig.EnableDebug, Is.True);
            Assert.That(elasticConfig.MappingAssemblies.Count, Is.EqualTo(1));

            var elasticMaps = serviceProvider.GetServices<IElasticMap>();
            Assert.That(elasticMaps, Is.Not.Null);
            Assert.That(elasticMaps.Count(), Is.EqualTo(3));
            Assert.That(elasticMaps.Any(x => x.GetType() == typeof(TestEntityMap)), Is.EqualTo(true));
            Assert.That(elasticMaps.Any(x => x.GetType() == typeof(TestSecondEntityMap)), Is.EqualTo(true));
            Assert.That(elasticMaps.Any(x => x.GetType() == typeof(TestThirdEntityMap)), Is.EqualTo(true));

            var elasticWrapper = serviceProvider.GetRequiredService<IElasticWrapper>();
            Assert.That(elasticWrapper, Is.Not.Null);
        }
    }
}
