using Nest;

namespace FluentHelper.ElasticSearch.Interfaces
{
    internal interface IElasticMap
    {
        Type GetMapType();

        void Map();

        void ApplySpecialMap(ConnectionSettings esSettings);

        void Verify();
    }
}
