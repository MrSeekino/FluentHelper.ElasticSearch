using System;
using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    internal sealed class CustomIndexCalculator<T, TFilter> : ICustomIndexCalculator<T, TFilter>
    {
        internal Func<T, string>? GetIndexPostfixByEntity { get; set; }
        internal Func<TFilter?, IEnumerable<string>?>? GetIndexPostfixByFilter { get; set; }

        private CustomIndexCalculator()
        { }

        public static ICustomIndexCalculator<T, TFilter> Create()
        {
            return new CustomIndexCalculator<T, TFilter>();
        }

        public void WithPostfixByEntity(Func<T, string> getIndexPostfixByEntity)
        {
            GetIndexPostfixByEntity = getIndexPostfixByEntity;
        }

        public void WithPostfixByFilter(Func<TFilter?, IEnumerable<string>?> getIndexPostfixByFilter)
        {
            GetIndexPostfixByFilter = getIndexPostfixByFilter;
        }

        public string CalcEntityIndex(T input)
        {
            if (GetIndexPostfixByEntity == null)
                return string.Empty;

            string indexByEntity = GetIndexPostfixByEntity(input);
            return indexByEntity;
        }

        public IEnumerable<string> CalcQueryIndex(object? baseObjectFilter)
        {
            if (GetIndexPostfixByFilter == null)
                return ["*"];

            var postFixFilter = GetIndexPostfixByFilter((TFilter?)baseObjectFilter);
            if (postFixFilter == null)
                return ["*"];

            return postFixFilter;
        }
    }
}
