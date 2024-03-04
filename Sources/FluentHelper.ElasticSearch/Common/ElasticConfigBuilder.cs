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
        private List<Uri> _connectionPool;
        private string _certificateFingerprint;
        private (string Username, string Password)? _basicAuthentication;

        private bool _enableDebug;
        private Action<ApiCallDetails>? _requestCompleted;

        private TimeSpan? _requestTimeout;
        private int _bulkInsertChunkSize;

        private string _indexPrefix;
        private string _indexSuffix;

        private Action<Microsoft.Extensions.Logging.LogLevel, Exception?, string, object?[]>? _logAction;

        private readonly List<Assembly> _mappingAssemblies;

        private ElasticConfigBuilder()
        {
            _connectionPool = [];
            _certificateFingerprint = string.Empty;
            _basicAuthentication = null;
            _enableDebug = false;
            _requestCompleted = null;
            _requestTimeout = null;
            _bulkInsertChunkSize = 50;
            _indexPrefix = string.Empty;
            _indexSuffix = string.Empty;
            _logAction = null;
            _mappingAssemblies = [];
        }

        public static ElasticConfigBuilder Create()
            => new ElasticConfigBuilder();

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

        public ElasticConfigBuilder WithAuthorization(string certificateFingerprint)
            => WithAuthorization(certificateFingerprint, null);

        public ElasticConfigBuilder WithAuthorization((string username, string password) basicAuthentication)
            => WithAuthorization(null, basicAuthentication);

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
            ArgumentOutOfRangeException.ThrowIfLessThan(timeoutValue.TotalSeconds, 1);

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
            ArgumentOutOfRangeException.ThrowIfLessThan(chunkSize, 1);

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

        public ElasticConfigBuilder WithLogAction(Action<Microsoft.Extensions.Logging.LogLevel, Exception?, string, object?[]> logAction)
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
