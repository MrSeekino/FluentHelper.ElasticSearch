﻿using FluentHelper.ElasticSearch.Interfaces;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;

namespace FluentHelper.ElasticSearch.Common
{
    internal static class Extensions
    {
        public static void ThrowIfIndexInvalid(this string indexNames, bool isRetrieveQuery)
        {
            string[] indexList = indexNames.Split(',');
            foreach (string indexName in indexList)
            {
                if (indexName.Length > 255)
                    throw new Exception($"Index name {indexName} exceeded maximum length of 255 characters");

                if (indexName == "." || indexName == "..")
                    throw new Exception($"Index name cannot be '.' or '..'");

                if (indexName.StartsWith('-') || indexName.StartsWith('_') || indexName.StartsWith('+'))
                    throw new Exception($"Index name {indexName} cannot start with '-' or '_' or '+'");

                if (indexName.Contains('\\') || indexName.Contains('/') || indexName.Contains('?') ||
                        indexName.Contains('\'') || indexName.Contains('<') || indexName.Contains('>') || indexName.Contains('|') ||
                        indexName.Contains(' ') || indexName.Contains(',') || indexName.Contains('#') || (indexName.Contains('*') && !isRetrieveQuery))
                    throw new Exception($"Index name {indexName} cannot contain '\', '/', '*', '?', '\"', '<', '>', '|', ' ', ',', '#'");
            }
        }

        public static object? GetFieldValue<TEntity>(this TEntity inputData, string fieldName) where TEntity : class
        {
            if (inputData == null)
                return null;

            foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(inputData.GetType()))
                if (property.Name == fieldName)
                    return property.GetValue(inputData);

            return null;
        }

        public static ExpandoObject GetExpandoObject<TEntity>(this TEntity inputData, IElasticFieldUpdater<TEntity>? elasticFieldUpdater) where TEntity : class
        {
            if (elasticFieldUpdater == null)
                throw new Exception("ElasticFieldUpdater cannot be null");

            var expandoInstance = new ExpandoObject();

            foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(inputData.GetType()))
                if (elasticFieldUpdater.GetFieldList().Contains(property.Name))
                    expandoInstance.TryAdd($"{char.ToLower(property.Name[0]) + property.Name.Substring(1)}", property.GetValue(inputData));

            return expandoInstance!;
        }
    }
}