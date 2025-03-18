using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.TestsSupport;
using NUnit.Framework;
using System.Security.Cryptography.X509Certificates;

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
        public void Verify_WithoutCertificateValidation_WorksCorrectly()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithoutCertificateValidation();

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.CertificateFingerprint, Is.EqualTo(string.Empty));
            Assert.That(elasticConfig.SkipCertificateValidation, Is.True);
            Assert.That(elasticConfig.CertificateFile, Is.Null);
        }

        [Test]
        public void Verify_WithCertificate_WorksCorrectly_WithFingerPrint()
        {
            string certFingerPrint = "abcdef";

            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithCertificate(certFingerPrint);

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.CertificateFingerprint, Is.EqualTo(certFingerPrint));
            Assert.That(elasticConfig.SkipCertificateValidation, Is.False);
            Assert.That(elasticConfig.CertificateFile, Is.Null);
        }

        [Test]
        public void Verify_WithCertificate_WorksCorrectly_WithFile()
        {
            byte[] certData = Convert.FromBase64String("LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tDQpNSUlDYkRDQ0FkV2dBd0lCQWdJQkFEQU5CZ2txaGtpRzl3MEJBUTBGQURCVE1Rc3dDUVlEVlFRR0V3SnBkREVMDQpNQWtHQTFVRUNBd0NTVlF4RFRBTEJnTlZCQW9NQkZSbGMzUXhLREFtQmdOVkJBTU1IMlpzZFdWdWRHaGxiSEJsDQpjaTVsYkdGemRHbGpjMlZoY21Ob0xuUmxjM1F3SGhjTk1qVXdNekU0TVRZeE5UQTVXaGNOTWpZd016RTRNVFl4DQpOVEE1V2pCVE1Rc3dDUVlEVlFRR0V3SnBkREVMTUFrR0ExVUVDQXdDU1ZReERUQUxCZ05WQkFvTUJGUmxjM1F4DQpLREFtQmdOVkJBTU1IMlpzZFdWdWRHaGxiSEJsY2k1bGJHRnpkR2xqYzJWaGNtTm9MblJsYzNRd2daOHdEUVlKDQpLb1pJaHZjTkFRRUJCUUFEZ1kwQU1JR0pBb0dCQU14OG54T2tsbVhTWDh6d0hpdlVDM0dQK29KazdsMllCci9CDQowemtLdTNxdDVTV3ZtK1I5Ym5wUjdvMTFnbE5IakFQUjJGY2k4MGxQQklIWWJZcm16QSthMUwxdmJaMFpERXlXDQpjZ1YvYk1Xcmw2Rml5OWpRaVJ6RFRCWmN1VHcvQW55c3RaclpTVlRlbm1pNUxUOVVXWnQvSnpPM3FrR1U3TE5JDQpySXVOdVRmM0FnTUJBQUdqVURCT01CMEdBMVVkRGdRV0JCUlkrdlQwSmxiQTVZS2RBb2E5MlVKWnVaWS8rREFmDQpCZ05WSFNNRUdEQVdnQlJZK3ZUMEpsYkE1WUtkQW9hOTJVSlp1WlkvK0RBTUJnTlZIUk1FQlRBREFRSC9NQTBHDQpDU3FHU0liM0RRRUJEUVVBQTRHQkFENys4cllQUWd6WlFXNjNzemtJMDRFa3JySHZFWTUxc3NwS252Q0swelhvDQpVbXhGdFBNTFFld3oxdHE3bjVORitSYjVKS3V2OGpPWVhKVVMzbE1lK2gwV0k5eWdGSmhzTHFSTytCUnBadTRBDQoyWDI1K3FKajB1MXhCUG9haDRBMjBuMkp3L1FJUHN6djRTOWN6SVZZSFRXWlN3N0JRQlVoS2dWeFpnaXNlcGgyDQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0t");
            var certFile = new X509Certificate2(certData);

            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithCertificate(certFile);

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.CertificateFingerprint, Is.EqualTo(string.Empty));
            Assert.That(elasticConfig.SkipCertificateValidation, Is.False);
            Assert.That(elasticConfig.CertificateFile, Is.Not.Null);
            Assert.That(elasticConfig.CertificateFile!.RawData, Is.EqualTo(certFile.RawData));
        }

        [Test]
        public void Verify_WithAuthorization_WorksCorrectly()
        {
            string username = "username";
            string password = "password";
            (string username, string password) basicAuth = new(username, password);

            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithAuthorization(basicAuth);

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
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
        public void Verify_WithRequestTimeout_ThrowsWhenChunkIsLessThanOneSecond()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create();
            Assert.Throws<ArgumentOutOfRangeException>(() => elasticConfigBuilder.WithRequestTimeout(TimeSpan.FromMilliseconds(10)));
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
        public void Verify_WithBulkInsertChunkSize_ThrowsWhenChunkIsLessThanOne()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create();
            Assert.Throws<ArgumentOutOfRangeException>(() => elasticConfigBuilder.WithBulkInsertChunkSize(0));
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

        [Test]
        public void Verify_WithDisablePing_WorksCorrectly()
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create()
                                            .WithDisablePing();

            var elasticConfig = elasticConfigBuilder.Build();
            Assert.That(elasticConfig, Is.Not.Null);
            Assert.That(elasticConfig.DisablePing, Is.True);
        }
    }
}
