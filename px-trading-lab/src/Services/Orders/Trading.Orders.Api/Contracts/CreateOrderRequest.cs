using Trading.Orders.Api.Models;
namespace Trading.Orders.Api.Contracts;

public sealed record CreateOrderRequest(Guid InvestorId, string AccountId, string Symbol, OrderSide Side, int Quantity, decimal Price);