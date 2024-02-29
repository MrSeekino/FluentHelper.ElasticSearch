using Elastic.Transport;
using FluentHelper.ElasticSearch.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace FluentHelper.ElasticSearch.Common
{
    public sealed class ElasticConfigBuilder
    {
        private List<Uri> _connectionPool = [];
        private string _certificateFingerprint = string.Empty;
        private (string Username, string Password)? _basicAuthentication;

        private bool _enableDebug = false;
        private Action<ApiCallDetails>? _requestCompleted;

        private TimeSpan? _requestTimeout;
        private int _bulkInsertChunkSize = 50;

        private string _indexPrefix = string.Empty;
        private string _indexSuffix = string.Empty;

        private Action<Microsoft.Extensions.Logging.LogLevel, Exception?, string, string?[]>? _logAction;

        private readonly List<Assembly> _mappingAssemblies = [];

        public ElasticConfigBuilder WithConnectionUri(string connectionUri)
            => WithConnectionUri(new Uri(connectionUri));

        public ElasticConfigBuilder WithConnectionUri(Uri connectionUri)
        {
            _connectionPool.Add(connectionUri);
            return this;
        }

        public ElasticConfigBuilder WithConnectionsPool(IEnumerable<string> connectionPool)
            => WithConnectionsPool(connectionPool.Select(c => new Uri(c)));

        public ElasticConfigBuilder WithConnectionsPool(IEnumerable<Uri> connectionPool)
        {
            _connectionPool = connectionPool.ToList();
            return this;
        }

        public ElasticConfigBuilder WithAuthorization(string? certificateFingerprint = null, (string username, string password)? basicAuthentication = null)
        {
            if (!string.IsNullOrEmpty(certificateFingerprint))
                _certificateFingerprint = certificateFingerprint;

            if (basicAuthentication != null)
                _basicAuthentication = basicAuthentication;

            return this;
        }

        public ElasticConfigBuilder WithRequestTimeout(TimeSpan timeoutValue)
        {
            _requestTimeout = timeoutValue;
            return this;
        }

        public ElasticConfigBuilder WithDebugEnabled()
        {
            _enableDebug = true;
            return this;
        }

        public ElasticConfigBuilder WithOnRequestCompleted(Action<ApiCallDetails> requestCompleted)
        {
            _enableDebug = true;
            _requestCompleted = requestCompleted;
            return this;
        }

        public ElasticConfigBuilder WithBulkInsertChunkSize(int chunkSize)
        {
            _bulkInsertChunkSize = chunkSize;
            return this;
        }

        public ElasticConfigBuilder WithIndexPrefix(string indexPrefix)
        {
            _indexPrefix = indexPrefix.ToLower();
            return this;
        }

        public ElasticConfigBuilder WithIndexSuffix(string indexSuffix)
        {
            _indexSuffix = indexSuffix.ToLower();
            return this;
        }

        public ElasticConfigBuilder WithMappingFromAssemblyOf<T>()
        {
            var mappingAssembly = Assembly.GetAssembly(typeof(T)) ?? throw new ArgumentException($"Could not find assembly with {typeof(T).Name}");

            _mappingAssemblies.Add(mappingAssembly!);
            return this;
        }

        public ElasticConfigBuilder WithLogAction(Action<Microsoft.Extensions.Logging.LogLevel, Exception?, string, string?[]> logAction)
        {
            _logAction = logAction;
            return this;
        }

        public IElasticConfig Build()
        {
            return new ElasticConfig
            {
                ConnectionsPool = _connectionPool.ToArray(),
                CertificateFingerprint = _certificateFingerprint,
                BasicAuthentication = _basicAuthentication,
                EnableDebug = _enableDebug,
                RequestCompleted = _requestCompleted,
                RequestTimeout = _requestTimeout,
                BulkInsertChunkSize = _bulkInsertChunkSize,
                IndexPrefix = _indexPrefix,
                IndexSuffix = _indexSuffix,
                LogAction = _logAction,
                MappingAssemblies = _mappingAssemblies
            };
        }
    }
}
