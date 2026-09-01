namespace Trading.Wallet.Api.Domain.Entities
{
    public class ProcessedMessage
    {
        public Guid Id { get; set; }

        public Guid MessageId { get; set; }

        public string Consumer { get; set; } = string.Empty;

        public DateTime ProcessedAt { get; set; }
    }
}
