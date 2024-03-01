using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Tests.Support;
using NUnit.Framework;

namespace FluentHelper.ElasticSearch.Tests
{
    [TestFixture]
    public class ElasticConfigBuilderTests
    {
        [Test]
        public void Verify_Configs_AreCorrectWhenOnDefault()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create();

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.ConnectionsPool.Length, Is.EqualTo(0));
            Assert.That(elasticConfig.CertificateFingerprint, Is.EqualTo(string.Empty));
            Assert.That(elasticConfig.EnableDebug, Is.False);
            Assert.That(elasticConfig.RequestCompleted, Is.Null);
            Assert.That(elasticConfig.RequestTimeout, Is.Null);
            Assert.That(elasticConfig.BulkInsertChunkSize, Is.EqualTo(50));
            Assert.That(elasticConfig.IndexPrefix, Is.EqualTo(string.Empty));
            Assert.That(elasticConfig.IndexSuffix, Is.EqualTo(string.Empty));
            Assert.That(elasticConfig.LogAction, Is.Null);
            Assert.That(elasticConfig.MappingAssemblies.Count, Is.EqualTo(0));
        }

        [Test]
        public void Verify_WithConnectionUri_WorksCorrectly()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithConnectionUri("http://localhost:9200");

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.ConnectionsPool.Length, Is.EqualTo(1));
            Assert.That(elasticConfig.ConnectionsPool[0], Is.EqualTo(new Uri("http://localhost:9200")));
        }

        [Test]
        public void Verify_WithConnectionUri_WorksCorrectlyWithMultipleAdd()
        {
            List<string> connectionsPool = ["http://192.168.1.1:9200", "http://192.168.1.2:9200"];

            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithConnectionUri(connectionsPool[0])
                                            .WithConnectionUri(connectionsPool[1]);

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.ConnectionsPool.Length, Is.EqualTo(2));
            Assert.That(elasticConfig.ConnectionsPool.Contains(new Uri(connectionsPool[0])), Is.True);
            Assert.That(elasticConfig.ConnectionsPool.Contains(new Uri(connectionsPool[1])), Is.True);
        }

        [Test]
        public void Verify_WithConnectionsPool_WorksCorrectly()
        {
            List<string> connectionsPool = ["http://192.168.1.1:9200", "http://192.168.1.2:9200"];

            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithConnectionsPool(connectionsPool);

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.ConnectionsPool.Length, Is.EqualTo(2));
            Assert.That(elasticConfig.ConnectionsPool.Contains(new Uri(connectionsPool[0])), Is.True);
            Assert.That(elasticConfig.ConnectionsPool.Contains(new Uri(connectionsPool[1])), Is.True);
        }

        [Test]
        public void Verify_WithAuthorization_WorksCorrectly_WithFingerPrintOnly()
        {
            string certFingerPrint = "abcdef";

            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithAuthorization(certFingerPrint);

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.CertificateFingerprint, Is.EqualTo(certFingerPrint));
            Assert.That(elasticConfig.BasicAuthentication, Is.Null);
        }

        [Test]
        public void Verify_WithAuthorization_WorksCorrectly_WithBasicAuthOnly()
        {
            string username = "username";
            string password = "password";
            (string username, string password) basicAuth = new(username, password);

            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithAuthorization(basicAuth);

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.CertificateFingerprint, Is.EqualTo(string.Empty));
            Assert.That(elasticConfig.BasicAuthentication, Is.Not.Null);
            Assert.That(elasticConfig.BasicAuthentication!.Value.Username, Is.EqualTo(username));
            Assert.That(elasticConfig.BasicAuthentication!.Value.Password, Is.EqualTo(password));
        }

        [Test]
        public void Verify_WithAuthorization_WorksCorrectly()
        {
            string username = "username";
            string password = "password";
            (string username, string password) basicAuth = new(username, password);

            string certFingerPrint = "abcdef";

            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithAuthorization(certFingerPrint, basicAuth);

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.CertificateFingerprint, Is.EqualTo(certFingerPrint));
            Assert.That(elasticConfig.BasicAuthentication, Is.Not.Null);
            Assert.That(elasticConfig.BasicAuthentication!.Value.Username, Is.EqualTo(username));
            Assert.That(elasticConfig.BasicAuthentication!.Value.Password, Is.EqualTo(password));
        }

        [Test]
        public void Verify_WithRequestTimeout_WorksCorrectly()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithRequestTimeout(TimeSpan.FromSeconds(60));

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.RequestTimeout, Is.Not.Null);
            Assert.That(elasticConfig.RequestTimeout, Is.EqualTo(TimeSpan.FromSeconds(60)));
        }

        [Test]
        public void Verify_WithDebugEnabled_WorksCorrectly()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithDebugEnabled();

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.EnableDebug, Is.True);
        }

        [Test]
        public void Verify_WithOnRequestCompleted_WorksCorrectly()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithOnRequestCompleted(x => { });

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.EnableDebug, Is.True);
            Assert.That(elasticConfig.RequestCompleted, Is.Not.Null);
        }

        [Test]
        public void Verify_WithBulkInsertChunkSize_WorksCorrectly()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithBulkInsertChunkSize(200);

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.BulkInsertChunkSize, Is.Not.Null);
            Assert.That(elasticConfig.BulkInsertChunkSize, Is.EqualTo(200));
        }

        [Test]
        public void Verify_WithIndexPrefix_WorksCorrectly()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithIndexPrefix("prefix");

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.IndexPrefix, Is.EqualTo("prefix"));
        }

        [Test]
        public void Verify_WithIndexSuffix_WorksCorrectly()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithIndexSuffix("suffix");

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.IndexSuffix, Is.EqualTo("suffix"));
        }

        [Test]
        public void Verify_WithLogAction_WorksCorrectly()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithLogAction((x, y, z, k) => { });

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.LogAction, Is.Not.Null);
        }

        [Test]
        public void Verify_WithMappingFromAssemblyOf_WorksCorrectly()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithMappingFromAssemblyOf<TestEntityMap>();

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.MappingAssemblies.Count, Is.EqualTo(1));
        }
    }
}
