using Trading.Orders.Api.Models;
namespace Trading.Orders.Api.Contracts;

public sealed record CreateOrderRequest(string AccountId, string Symbol, OrderSide Side, int Quantity, decimal Price);