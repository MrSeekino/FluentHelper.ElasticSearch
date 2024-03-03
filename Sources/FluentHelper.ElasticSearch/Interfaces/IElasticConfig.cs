using Elastic.Transport;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace FluentHelper.ElasticSearch.Interfaces
{
    public interface IElasticConfig
    {
        Uri[] ConnectionsPool { get; }
        string? CertificateFingerprint { get; }
        (string Username, string Password)? BasicAuthentication { get; }

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
