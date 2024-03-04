using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    internal sealed class BasicIndexCalculator<T> : IBasicIndexCalculator<T>
    {
        bool _fixedIndex;

        private BasicIndexCalculator()
        { }

        public static IBasicIndexCalculator<T> Create()
        {
            return new BasicIndexCalculator<T>()
            {
                _fixedIndex = false
            };
        }

        public IBasicIndexCalculator<T> WithFixedIndexName()
        {
            _fixedIndex = true;
            return this;
        }

        public string GetIndexPostfixByEntity(T input)
        {
            return string.Empty;
        }

        public IEnumerable<string> GetIndexPostfixByFilter(object? baseObjectFilter)
        {
            if (_fixedIndex)
                return [];

            return ["*"];
        }
    }
}
