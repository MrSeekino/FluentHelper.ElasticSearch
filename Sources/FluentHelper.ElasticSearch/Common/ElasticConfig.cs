using FluentHelper.ElasticSearch.Interfaces;
using System.Reflection;

namespace FluentHelper.ElasticSearch.Common
{
    internal sealed class ElasticConfig : IElasticConfig
    {
        public string ConnectionUrl { get; set; } = string.Empty;
        public string? CertificateFingerprint { get; set; }
        public (string Username, string Password)? BasicAuthentication { get; set; }

        public bool EnableApiVersioningHeader { get; set; }

        public TimeSpan? RequestTimeout { get; set; }
        public bool DebugQuery { get; set; }
        public int BulkInsertChunkSize { get; set; }

        public string IndexPrefix { get; set; } = string.Empty;
        public string IndexSuffix { get; set; } = string.Empty;

        public Action<Microsoft.Extensions.Logging.LogLevel, string?, Exception?>? LogAction { get; set; }

        public List<Assembly> MappingAssemblies { get; set; } = new List<Assembly>();
    }
}
