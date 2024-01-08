using System.Reflection;

namespace FluentHelper.ElasticSearch.Interfaces
{
    public interface IElasticConfig
    {
        string ConnectionUrl { get; }
        string? CertificateFingerprint { get; }
        (string Username, string Password)? BasicAuthentication { get; }

        bool EnableApiVersioningHeader { get; }

        TimeSpan? RequestTimeout { get; }
        bool DebugQuery { get; }
        int BulkInsertChunkSize { get; }

        string IndexPrefix { get; }
        string IndexSuffix { get; }

        Action<Microsoft.Extensions.Logging.LogLevel, string?, Exception?>? LogAction { get; }

        List<Assembly> MappingAssemblies { get; }
    }
}
