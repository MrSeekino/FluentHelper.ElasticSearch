using System.Collections.Generic;

namespace FluentHelper.ElasticSearch.IndexCalculators
{
    internal sealed class BasicIndexCalculator<TEntity> : IBasicIndexCalculator<TEntity> where TEntity : class
    {
        bool _fixedIndex;

        private BasicIndexCalculator()
        { }

        public static IBasicIndexCalculator<TEntity> Create()
        {
            return new BasicIndexCalculator<TEntity>()
            {
                _fixedIndex = false
            };
        }

        public IBasicIndexCalculator<TEntity> WithFixedIndexName()
        {
            _fixedIndex = true;
            return this;
        }

        public string GetBaseIndexWildcard()
        {
            if (_fixedIndex)
                return string.Empty;

            return "*";
        }

        public string GetIndexPostfixByEntity(TEntity input)
        {
            return string.Empty;
        }

        public IEnumerable<string> GetIndexPostfixByFilter(object? baseObjectFilter)
        {
            string baseWildcard = GetBaseIndexWildcard();

            if (string.IsNullOrWhiteSpace(baseWildcard))
                return [];

            return [baseWildcard];
        }
    }
}
