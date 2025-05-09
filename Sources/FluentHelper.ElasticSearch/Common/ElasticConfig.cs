﻿using Elastic.Transport;
using FluentHelper.ElasticSearch.Interfaces;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;

namespace FluentHelper.ElasticSearch.Common
{
    internal sealed class ElasticConfig : IElasticConfig
    {
        public Uri[] ConnectionsPool { get; set; } = [];

        public bool SkipCertificateValidation { get; set; }
        public string? CertificateFingerprint { get; set; }
        public X509Certificate2? CertificateFile { get; set; }

        public (string Username, string Password)? BasicAuthentication { get; set; }

        public bool DisablePing { get; set; }

        public bool EnableDebug { get; set; }
        public Action<ApiCallDetails>? RequestCompleted { get; set; }

        public TimeSpan? RequestTimeout { get; set; }
        public int BulkInsertChunkSize { get; set; }

        public string IndexPrefix { get; set; } = string.Empty;
        public string IndexSuffix { get; set; } = string.Empty;

        public Action<Microsoft.Extensions.Logging.LogLevel, Exception?, string, object?[]>? LogAction { get; set; }

        public List<Assembly> MappingAssemblies { get; set; } = [];
    }
}
