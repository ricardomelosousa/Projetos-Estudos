namespace Trading.Wallet.Api.Contracts
{
    public record OrderCreatedEvent(
        Guid OrderId,
        Guid InvestorId,
        string Symbol,
        int Quantity,
        decimal Price,
        string Side);
}
