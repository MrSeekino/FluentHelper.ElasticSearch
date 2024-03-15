using System;
using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    internal sealed class FilterableIndexCalculator<T, TFilter> : IFilterableIndexCalculator<T, TFilter>
    {
        private Func<T, string>? _getIndexPostfixByEntity;
        private Func<TFilter?, IEnumerable<string>?>? _getIndexPostfixByFilter;

        private FilterableIndexCalculator()
        { }

        public static IFilterableIndexCalculator<T, TFilter> Create()
        {
            return new FilterableIndexCalculator<T, TFilter>();
        }

        public void WithPostfixByEntity(Func<T, string> getIndexPostfixByEntity)
        {
            _getIndexPostfixByEntity = getIndexPostfixByEntity;
        }

        public void WithPostfixByFilter(Func<TFilter?, IEnumerable<string>?> getIndexPostfixByFilter)
        {
            _getIndexPostfixByFilter = getIndexPostfixByFilter;
        }

        public string GetBaseIndexWildcard()
        {
            if (_getIndexPostfixByFilter == null)
                return "*";

            return "-*";
        }

        public string GetIndexPostfixByEntity(T input)
        {
            if (_getIndexPostfixByEntity == null)
                return string.Empty;

            string indexByEntity = _getIndexPostfixByEntity(input);
            return indexByEntity;
        }

        public IEnumerable<string> GetIndexPostfixByFilter(object? baseObjectFilter)
        {
            if (_getIndexPostfixByFilter == null)
                return ["*"];

            var postFixFilter = _getIndexPostfixByFilter((TFilter?)baseObjectFilter);
            if (postFixFilter == null)
                return ["*"];

            return postFixFilter;
        }
    }
}
