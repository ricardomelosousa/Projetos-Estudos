using Trading.Orders.Api.Contracts;

public sealed record OrderCreatedEvent(Guid OrderId, string AccountId, string Symbol, string Side, int Quantity, decimal Price, DateTimeOffset CreatedAt);