using FluentHelper.ElasticSearch.Common;
using FluentHelper.ElasticSearch.Examples.Mappings;
using FluentHelper.ElasticSearch.Examples.Repositories;
using FluentHelper.ElasticSearch.Examples.Runner;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

string elasticUrl = "https://localhost:9200";
string certSha256 = "DB2514B5BD82D0331C97C85C60FCF35CDD23A45F2EFB49F19FAE76FDDFC3A271";
string username = "elastic";
string password = "ppY4hN+Fv28ca-O+F7tp";

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