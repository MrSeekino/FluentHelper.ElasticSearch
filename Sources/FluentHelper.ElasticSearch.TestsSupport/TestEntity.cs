namespace FluentHelper.ElasticSearch.TestsSupport
{
    public class TestEntity
    {
        public Guid Id { get; set; }

        public string GroupName { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public DateTime CreationTime { get; set; }
        public bool Active { get; set; }
    }
}
