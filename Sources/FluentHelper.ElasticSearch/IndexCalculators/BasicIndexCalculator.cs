using FluentHelper.ElasticSearch.Interfaces;
using System;
using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    public sealed class BasicIndexCalculator<T> : IElasticIndexCalculator<T>
    {
        readonly bool _fixedIndex;

        public BasicIndexCalculator(bool fixedIndex = false)
        {
            _fixedIndex = fixedIndex;
        }

        public string CalcEntityIndex(T input)
        {
            return string.Empty;
        }

        public IEnumerable<string> CalcQueryIndex(object? baseObjectFilter)
        {
            if (_fixedIndex)
                return Array.Empty<string>();

            return new List<string>() { "*" };
        }
    }
}
