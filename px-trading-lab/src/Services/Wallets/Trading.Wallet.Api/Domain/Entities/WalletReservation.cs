namespace Trading.Wallet.Api.Domain.Entities
{
    public sealed class WalletReservation
    {
        public Guid Id { get; set; }

        public Guid OrderId { get; set; }

        public Guid WalletId { get; set; }

        public decimal Amount { get; set; }

        public ReservationStatus Status { get; set; }

        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }

    public enum ReservationStatus
    {
        Reserved = 1,
        Released = 2,
        Confirmed = 3
    }
}
