using Elastic.Transport;
using FluentHelper.ElasticSearch.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;

namespace FluentHelper.ElasticSearch.Common
{
    public sealed class ElasticConfigBuilder
    {
        private List<Uri> _connectionPool;

        private bool _skipCertificateValidation;
        private string _certificateFingerprint;
        private X509Certificate2? _certificateFile;

        private (string Username, string Password)? _basicAuthentication;

        private bool _disablePing;

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
            _skipCertificateValidation = false;
            _certificateFingerprint = string.Empty;
            _certificateFile = null;
            _basicAuthentication = null;
            _disablePing = false;
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

        /// <summary>
        /// Specify a single connection url to elastic
        /// </summary>
        /// <param name="connectionUri">the connection url</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithConnectionUri(string connectionUri)
            => WithConnectionUri(new Uri(connectionUri));

        /// <summary>
        /// Specify a single connection uri to elastic
        /// </summary>
        /// <param name="connectionUri">the connection uri</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithConnectionUri(Uri connectionUri)
        {
            _connectionPool.Add(connectionUri);
            return this;
        }

        /// <summary>
        /// Specify a pool of connections for an elastic cluster
        /// </summary>
        /// <param name="connectionPool">the connection pool</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithConnectionsPool(IEnumerable<string> connectionPool)
            => WithConnectionsPool(connectionPool.Select(c => new Uri(c)));

        /// <summary>
        /// Specify a pool of connections for an elastic cluster
        /// </summary>
        /// <param name="connectionPool">the connection pool</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithConnectionsPool(IEnumerable<Uri> connectionPool)
        {
            _connectionPool = connectionPool.ToList();
            return this;
        }

        /// <summary>
        /// Skip certificate validation when connecting to elasticsearch
        /// </summary>
        /// <returns></returns>
        public ElasticConfigBuilder WithoutCertificateValidation()
        {
            _skipCertificateValidation = true;
            _certificateFingerprint = string.Empty;
            _certificateFile = null;
            return this;
        }

        /// <summary>
        /// Specify the certificate fingerprint to be used when connecting to elasticsearch
        /// </summary>
        /// <param name="certificateFingerprint">SHA256 certificate fingerprint</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithCertificate(string certificateFingerprint)
        {
            _skipCertificateValidation = false;
            _certificateFingerprint = certificateFingerprint;
            _certificateFile = null;
            return this;
        }

        /// <summary>
        /// Specify the certificate file to be used when connecting to elasticsearch
        /// </summary>
        /// <param name="certificateFile">Certificate file in x509 format</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithCertificate(X509Certificate2 certificateFile)
        {
            _skipCertificateValidation = false;
            _certificateFingerprint = string.Empty;
            _certificateFile = certificateFile;
            return this;
        }

        /// <summary>
        /// Specify user account to be used when connecting to elasticsearch
        /// </summary>
        /// <param name="basicAuthentication">Username and Password to be used</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithAuthorization((string username, string password) basicAuthentication)
        {
            _basicAuthentication = basicAuthentication;
            return this;
        }

        /// <summary>
        /// Disable Ping to Elasticsearch
        /// </summary>
        /// <returns></returns>
        public ElasticConfigBuilder WithDisablePing()
        {
            _disablePing = true;
            return this;
        }

        /// <summary>
        /// Specify the timeout for all elasticsearch requests. Throws if less than 1second
        /// </summary>
        /// <param name="timeoutValue">timeout value</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithRequestTimeout(TimeSpan timeoutValue)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(timeoutValue.TotalSeconds, 1);

            _requestTimeout = timeoutValue;
            return this;
        }

        /// <summary>
        /// Enable the debug mode
        /// </summary>
        /// <returns></returns>
        public ElasticConfigBuilder WithDebugEnabled()
        {
            _enableDebug = true;
            return this;
        }

        /// <summary>
        /// Set an action to be executed when a request is completed. Also enable debug mode
        /// </summary>
        /// <param name="requestCompleted">the action to be performed</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithOnRequestCompleted(Action<ApiCallDetails> requestCompleted)
        {
            _enableDebug = true;
            _requestCompleted = requestCompleted;
            return this;
        }

        /// <summary>
        /// Set the size of the chunks when using Bulk function. Throws if less than 1
        /// </summary>
        /// <param name="chunkSize">chunk size to be used</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithBulkInsertChunkSize(int chunkSize)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(chunkSize, 1);

            _bulkInsertChunkSize = chunkSize;
            return this;
        }

        /// <summary>
        /// Set the prefix for all the indexes
        /// </summary>
        /// <param name="indexPrefix">prefix for all indexes</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithIndexPrefix(string indexPrefix)
        {
            _indexPrefix = indexPrefix.ToLower();
            return this;
        }

        /// <summary>
        /// Set the sufic for all the indexes
        /// </summary>
        /// <param name="indexSuffix">suffix for all indexes</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithIndexSuffix(string indexSuffix)
        {
            _indexSuffix = indexSuffix.ToLower();
            return this;
        }

        /// <summary>
        /// Add all IElasticMap defined in the assembly that contains the type. Throws if assembly is not found
        /// </summary>
        /// <typeparam name="T">the type to search the assembly</typeparam>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public ElasticConfigBuilder WithMappingFromAssemblyOf<T>()
        {
            var mappingAssembly = Assembly.GetAssembly(typeof(T)) ?? throw new ArgumentException($"Could not find assembly with {typeof(T).Name}");

            _mappingAssemblies.Add(mappingAssembly!);
            return this;
        }

        /// <summary>
        /// Add an action to log the internal operatons
        /// </summary>
        /// <param name="logAction">the action to be performed</param>
        /// <returns></returns>
        public ElasticConfigBuilder WithLogAction(Action<Microsoft.Extensions.Logging.LogLevel, Exception?, string, object?[]> logAction)
        {
            _logAction = logAction;
            return this;
        }

        /// <summary>
        /// Build the ElasticConfig
        /// </summary>
        /// <returns></returns>
        public IElasticConfig Build()
        {
            return new ElasticConfig
            {
                ConnectionsPool = _connectionPool.ToArray(),
                SkipCertificateValidation = _skipCertificateValidation,
                CertificateFingerprint = _certificateFingerprint,
                CertificateFile = _certificateFile,
                BasicAuthentication = _basicAuthentication,
                DisablePing = _disablePing,
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
