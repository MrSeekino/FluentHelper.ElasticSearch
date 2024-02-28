using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Examples.Mappings;
using FluentHelper.ElasticSearch.Examples.Repositories;
using FluentHelper.ElasticSearch.Examples.Runner;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

string elasticUrl = "https://localhost:9200";
string certSha256 = "cada384305553e4758f9f821096fd91513cea094d48826f5702b1fcb39fd8001";
string username = "elastic";
string password = "U55Tz=xZSINciZeacUKy";

var builder = Host.CreateDefaultBuilder();

builder.ConfigureServices(services =>
{
    services.AddSingleton<ITestDataRepository, TestDataRepository>();

    services.AddFluentElasticWrapper(esConfigBuilder =>
    {
        esConfigBuilder.WithConnectionUri(elasticUrl)
               .WithAuthorization(certSha256, new(username, password))
               .WithDebugEnabled()
               //.WithOnRequestCompleted(x => Console.WriteLine($"DebugInfo: {x.DebugInformation}"))
               .WithMappingFromAssemblyOf<TestDataMap>();
    });

    services.AddHostedService<ExampleService>();
});

var host = builder.Build();
host.Run();