using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Examples.Mappings;
using FluentHelper.ElasticSearch.Examples.Repositories;
using FluentHelper.ElasticSearch.Examples.Runner;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

string elasticUrl = "https://localhost:9200";
string certSha256 = "3e5b838059e988dae8b13891de0f9bf513d22d83768a909b2c2dda77e2f3da29";
string username = "elastic";
string password = "IowPZTzYRNrRu1_CO-QH";

var builder = Host.CreateDefaultBuilder();

builder.ConfigureServices(services =>
{
    services.AddSingleton<ITestDataRepository, TestDataRepository>();

    services.AddFluentElasticWrapper(esConfigBuilder =>
    {
        esConfigBuilder.WithConnectionUrl(elasticUrl)
               .WithAuthorization(certSha256, new(username, password))
               .WithMappingFromAssemblyOf<TestDataMap>();
    });

    services.AddHostedService<ExampleService>();
});

var host = builder.Build();
host.Run();