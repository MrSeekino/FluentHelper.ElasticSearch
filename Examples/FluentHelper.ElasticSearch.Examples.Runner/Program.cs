using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Examples.Mappings;
using FluentHelper.ElasticSearch.Examples.Repositories;
using FluentHelper.ElasticSearch.Examples.Runner;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

string elasticUrl = "https://localhost:9200";
string certSha256 = "44D6D42CD8E5F4AAAF3D310B1E3315109D575398BFCE81F7EE72EE1906978302";
string username = "elastic";
string password = "x4MlkRnVbjsLdWXVY7lS";

var builder = Host.CreateDefaultBuilder();

builder.ConfigureServices(services =>
{
    services.AddSingleton<ITestDataRepository, TestDataRepository>();

    services.AddFluentElasticWrapper(esConfigBuilder =>
    {
        esConfigBuilder.WithConnectionUri(elasticUrl)
               .WithAuthorization(new(username, password))
               .WithCertificate(certSha256)
               .WithDebugEnabled()
               //.WithOnRequestCompleted(x => Console.WriteLine($"DebugInfo: {x.DebugInformation}"))
               .WithMappingFromAssemblyOf<TestDataMap>();
    });

    services.AddHostedService<ExampleService>();
});

var host = builder.Build();
host.Run();