using FluentHelper.ElasticSearch.Interfaces;
using System;
using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    public sealed class CustomIndexCalculator<T, TFilter> : IElasticIndexCalculator<T>
    {
        internal Func<T, string> GetIndexPostfixByEntity { get; set; }
        internal Func<TFilter?, IEnumerable<string>?> GetIndexPostfixByFilter { get; set; }

        public CustomIndexCalculator(Func<T, string> getIndexPostfixByEntity, Func<TFilter?, IEnumerable<string>?> getIndexPostfixByFilter)
        {
            GetIndexPostfixByEntity = getIndexPostfixByEntity;
            GetIndexPostfixByFilter = getIndexPostfixByFilter;
        }

        public string CalcEntityIndex(T input)
        {
            string indexByEntity = GetIndexPostfixByEntity(input);
            return indexByEntity;
        }

        public IEnumerable<string> CalcQueryIndex(object? baseObjectFilter)
        {
            var postFixFilter = GetIndexPostfixByFilter((TFilter?)baseObjectFilter);
            if (postFixFilter == null)
                return new List<string>() { "*" };

            return postFixFilter;
        }
    }
}
