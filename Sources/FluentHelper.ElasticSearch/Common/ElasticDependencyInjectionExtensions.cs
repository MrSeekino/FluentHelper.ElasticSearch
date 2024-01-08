using FluentHelper.ElasticSearch.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace FluentHelper.ElasticSearch.Common
{
    public static class ElasticDependencyInjectionExtensions
    {
        public static void AddFluentElasticWrapper(this IServiceCollection serviceCollection, Action<ElasticConfigBuilder> elasticConfigBuilderFunc, ServiceLifetime serviceLifetime = ServiceLifetime.Singleton)
        {
            ElasticConfigBuilder elasticConfigBuilder = new ElasticConfigBuilder();
            elasticConfigBuilderFunc(elasticConfigBuilder);

            IElasticConfig elasticConfig = elasticConfigBuilder.Build();
            serviceCollection.AddSingleton(x => elasticConfig);

            var mappingTypes = elasticConfig.MappingAssemblies.SelectMany(m => m.GetTypes()).Where(p => p.IsClass && typeof(IElasticMap).IsAssignableFrom(p) && !p.IsAbstract).ToList();
            foreach (var mappingType in mappingTypes)
                serviceCollection.AddSingleton(typeof(IElasticMap), mappingType!);

            serviceCollection.Add(new ServiceDescriptor(typeof(IElasticWrapper), typeof(ElasticWrapper), serviceLifetime));
        }
    }
}
