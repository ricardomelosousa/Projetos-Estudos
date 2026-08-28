namespace Trading.Orders.Api.Configuration
{
    public sealed class KafkaOptions
    {
        public const string SectionName = "Kafka";

        public string BootstrapServers { get; init; } = string.Empty;

        public KafkaTopicsOptions Topics { get; init; }  = new();
    }

    public sealed class KafkaTopicsOptions
    {
        public string OrderCreated { get; set; } = string.Empty;
    }
}
