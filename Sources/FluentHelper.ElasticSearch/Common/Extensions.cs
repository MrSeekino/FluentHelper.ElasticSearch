using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using FluentHelper.ElasticSearch.Interfaces;
using FluentHelper.ElasticSearch.QueryParameters;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace FluentHelper.ElasticSearch.Common
{
    internal static class Extensions
    {
        public static ElasticsearchClientSettings AddCertificateValidation(this ElasticsearchClientSettings esSettings, IElasticConfig elasticConfig)
        {
            if (!elasticConfig.SkipCertificateValidation)
            {
                if (elasticConfig.CertificateFile != null)
                {
                    esSettings.ServerCertificateValidationCallback((message, certificate, chain, sslErrors) =>
                    {
                        if (sslErrors == SslPolicyErrors.None)
                            return true;

                        if (certificate == null)
                            return false;

                        if (chain == null)
                            return false;

                        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                        chain.ChainPolicy.CustomTrustStore.Add(elasticConfig.CertificateFile);

                        return chain.Build(new X509Certificate2(certificate));
                    });
                }
                else if (!string.IsNullOrWhiteSpace(elasticConfig.CertificateFingerprint))
                    esSettings.CertificateFingerprint(elasticConfig.CertificateFingerprint);
            }
            else
                esSettings.ServerCertificateValidationCallback(CertificateValidations.AllowAll);

            return esSettings;
        }

        public static void ThrowIfIndexInvalid(this string indexNames, bool isRetrieveQuery)
        {
            string[] indexList = indexNames.Split(',');
            foreach (string indexName in indexList)
            {
                if (indexName.Length > 255)
                    throw new ArgumentOutOfRangeException($"Index name {indexName} exceeded maximum length of 255 characters");

                if (indexName == "." || indexName == "..")
                    throw new FormatException($"Index name cannot be '.' or '..'");

                if (indexName.StartsWith('-') || indexName.StartsWith('_') || indexName.StartsWith('+'))
                    throw new FormatException($"Index name {indexName} cannot start with '-' or '_' or '+'");

                if (indexName.Contains('\\') || indexName.Contains('/') || indexName.Contains('?') || indexName.Contains('"') ||
                        indexName.Contains('\'') || indexName.Contains('<') || indexName.Contains('>') || indexName.Contains('|') ||
                        indexName.Contains(' ') || indexName.Contains('#') || (indexName.Contains('*') && !isRetrieveQuery))
                    throw new FormatException($"Index name {indexName} cannot contain '\', '/', '*', '?', '\"', '<', '>', '|', ' ', '#'");
            }
        }

        public static object? GetFieldValue<TEntity>(this TEntity inputData, string fieldName) where TEntity : class
        {
            if (inputData == null)
                return null;

            var propertyData = TypeDescriptor.GetProperties(inputData.GetType()).Find(fieldName, true);
            return propertyData?.GetValue(inputData);
        }

        public static ExpandoObject GetExpandoObject<TEntity>(this TEntity inputData, IElasticFieldUpdater<TEntity> elasticFieldUpdater) where TEntity : class
        {
            var expandoInstance = new ExpandoObject();

            var fieldProperties = TypeDescriptor.GetProperties(inputData.GetType());
            foreach (var field in elasticFieldUpdater.GetFieldList())
            {
                var property = fieldProperties.Find(field, true);
                if (property != null)
                    expandoInstance.TryAdd($"{char.ToLower(property.Name[0]) + property.Name.Substring(1)}", property.GetValue(inputData));
            }

            return expandoInstance!;
        }
    }
}
