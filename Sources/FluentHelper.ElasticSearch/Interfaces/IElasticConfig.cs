using Elastic.Transport;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;

namespace FluentHelper.ElasticSearch.Interfaces
{
    public interface IElasticConfig
    {
        Uri[] ConnectionsPool { get; }

        bool SkipCertificateValidation { get; }
        string? CertificateFingerprint { get; }
        X509Certificate2? CertificateFile { get; }

        (string Username, string Password)? BasicAuthentication { get; }

        bool DisablePing { get; }

        bool EnableDebug { get; }
        Action<ApiCallDetails>? RequestCompleted { get; }

        TimeSpan? RequestTimeout { get; }
        int BulkInsertChunkSize { get; }

        string IndexPrefix { get; }
        string IndexSuffix { get; }

        Action<Microsoft.Extensions.Logging.LogLevel, Exception?, string, object?[]>? LogAction { get; }

        List<Assembly> MappingAssemblies { get; }
    }
}
