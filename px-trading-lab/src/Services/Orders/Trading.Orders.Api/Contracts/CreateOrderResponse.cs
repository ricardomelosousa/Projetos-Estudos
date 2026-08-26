using Trading.Orders.Api.Models;
namespace Trading.Orders.Api.Contracts;

public sealed record CreateOrderResponse(Guid OrderId, string Status);