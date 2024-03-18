using System;
using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    internal sealed class FilterableIndexCalculator<TEntity, TFilter> : IFilterableIndexCalculator<TEntity, TFilter> where TEntity : class
    {
        private Func<TEntity, string>? _getIndexPostfixByEntity;
        private Func<TFilter?, IEnumerable<string>?>? _getIndexPostfixByFilter;

        private FilterableIndexCalculator()
        { }

        public static IFilterableIndexCalculator<TEntity, TFilter> Create()
        {
            return new FilterableIndexCalculator<TEntity, TFilter>();
        }

        public void WithPostfixByEntity(Func<TEntity, string> getIndexPostfixByEntity)
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

        public string GetIndexPostfixByEntity(TEntity input)
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
