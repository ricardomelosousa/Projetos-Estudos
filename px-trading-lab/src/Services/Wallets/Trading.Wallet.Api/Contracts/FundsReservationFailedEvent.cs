namespace Trading.Wallet.Api.Contracts
{
    public record FundsReservationFailedEvent(
    Guid OrderId,
    Guid InvestorId,
    decimal RequiredAmount,
    string Reason,
    DateTime FailedAt);
}
