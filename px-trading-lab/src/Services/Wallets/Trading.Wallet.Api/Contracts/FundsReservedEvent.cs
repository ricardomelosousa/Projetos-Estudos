namespace Trading.Wallet.Api.Contracts
{
    public record FundsReservedEvent(
        Guid OrdeId,
        Guid InvestorId,
        Guid ReservationId,
        decimal Amount,
        DateTime ReservedAt);
}
