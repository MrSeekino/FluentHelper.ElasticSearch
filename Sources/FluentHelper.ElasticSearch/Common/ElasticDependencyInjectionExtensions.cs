﻿using FluentHelper.ElasticSearch.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Linq;

namespace FluentHelper.ElasticSearch.Common
{
    public static class ElasticDependencyInjectionExtensions
    {
        /// <summary>
        /// Add IElasticWrapper as dependency
        /// </summary>
        /// <param name="serviceCollection">the current servicecollection</param>
        /// <param name="elasticConfigBuilderFunc">the action to build the elastic configuations</param>
        /// <param name="serviceLifetime">the lifetime of IElasticWrapper. default to Singleton</param>
        public static void AddFluentElasticWrapper(this IServiceCollection serviceCollection, Action<ElasticConfigBuilder> elasticConfigBuilderFunc, ServiceLifetime serviceLifetime = ServiceLifetime.Singleton)
        {
            var elasticConfigBuilder = ElasticConfigBuilder.Create();
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
