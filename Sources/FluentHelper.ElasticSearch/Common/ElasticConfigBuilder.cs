using FluentHelper.ElasticSearch.Interfaces;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace FluentHelper.ElasticSearch.Common
{
    public sealed class ElasticConfigBuilder
    {
        internal string ConnectionUrl { get; private set; } = string.Empty;
        internal string CertificateFingerprint { get; private set; } = string.Empty;
        internal (string Username, string Password)? BasicAuthentication { get; private set; }

        internal bool EnableApiVersioningHeader { get; private set; } = false;

        internal TimeSpan? RequestTimeout { get; private set; }
        internal bool DebugQuery { get; private set; } = false;
        internal int BulkInsertChunkSize { get; private set; } = 50;

        internal string IndexPrefix { get; private set; } = string.Empty;
        internal string IndexSuffix { get; private set; } = string.Empty;

        internal Action<Microsoft.Extensions.Logging.LogLevel, Exception?, string, string?[]>? LogAction { get; private set; }

        internal List<Assembly> MappingAssemblies { get; private set; } = new List<Assembly>();

        public ElasticConfigBuilder WithConnectionUrl(string connectionUrl)
        {
            ConnectionUrl = connectionUrl;
            return this;
        }

        public ElasticConfigBuilder WithAuthorization(string? certificateFingerprint = null, (string username, string password)? basicAuthentication = null)
        {
            if (!string.IsNullOrEmpty(certificateFingerprint))
                CertificateFingerprint = certificateFingerprint;

            if (basicAuthentication != null)
                BasicAuthentication = basicAuthentication;

            return this;
        }

        public ElasticConfigBuilder WithRequestTimeout(TimeSpan timeoutValue)
        {
            RequestTimeout = timeoutValue;
            return this;
        }

        public ElasticConfigBuilder WithDebugQuery(bool debugQuery)
        {
            DebugQuery = debugQuery;
            return this;
        }

        public ElasticConfigBuilder WithBulkInsertChunkSize(int chunkSize)
        {
            BulkInsertChunkSize = chunkSize;
            return this;
        }

        public ElasticConfigBuilder WithIndexPrefix(string indexPrefix)
        {
            IndexPrefix = indexPrefix.ToLower();
            return this;
        }

        public ElasticConfigBuilder WithIndexSuffix(string indexSuffix)
        {
            IndexSuffix = indexSuffix.ToLower();
            return this;
        }

        public ElasticConfigBuilder WithMappingFromAssemblyOf<T>()
        {
            var mappingAssembly = Assembly.GetAssembly(typeof(T)) ?? throw new ArgumentException($"Could not find assembly with {typeof(T).Name}");

            MappingAssemblies.Add(mappingAssembly!);
            return this;
        }

        public ElasticConfigBuilder WithLogAction(Action<Microsoft.Extensions.Logging.LogLevel, Exception?, string, string?[]> logAction)
        {
            LogAction = logAction;
            return this;
        }

        public ElasticConfigBuilder WithApiVersioningHeader(bool enableApiVersioningHeader = true)
        {
            EnableApiVersioningHeader = enableApiVersioningHeader;
            return this;
        }

        public IElasticConfig Build()
        {
            return new ElasticConfig
            {
                ConnectionUrl = ConnectionUrl,
                CertificateFingerprint = CertificateFingerprint,
                BasicAuthentication = BasicAuthentication,
                EnableApiVersioningHeader = EnableApiVersioningHeader,
                RequestTimeout = RequestTimeout,
                DebugQuery = DebugQuery,
                BulkInsertChunkSize = BulkInsertChunkSize,
                IndexPrefix = IndexPrefix,
                IndexSuffix = IndexSuffix,
                LogAction = LogAction,
                MappingAssemblies = MappingAssemblies
            };
        }
    }
}
