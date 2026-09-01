using Trading.Orders.Api.Contracts;

public sealed record OrderCreatedEvent(Guid MessageId, Guid OrderId, Guid InvestorId, string AccountId, string Symbol, string Side, int Quantity, decimal Price, DateTimeOffset CreatedAt);