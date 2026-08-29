namespace Trading.Wallet.Api.Domain.Entities
{
    public  class Wallet
    {
        public Guid Id { get; set; }

        public Guid InvestorId { get; set; }

        public decimal Balance { get; set; }

        public decimal ReservedBalance { get; set; }

        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }
}
