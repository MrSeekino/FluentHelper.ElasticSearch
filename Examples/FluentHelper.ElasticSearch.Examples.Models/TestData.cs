namespace FluentHelper.ElasticSearch.Examples.Models
{
    public class TestData
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime CreationDate { get; set; }
        public bool Active { get; set; }
    }
}
