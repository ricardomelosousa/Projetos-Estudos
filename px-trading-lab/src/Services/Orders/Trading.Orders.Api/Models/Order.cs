namespace Trading.Orders.Api.Models;

public sealed class Order
{
    public Guid Id {get; init;}
    public string AccountId {get; init;} = string.Empty;
    public string Symbol {get;init;} = string.Empty;
    public OrderSide Side {get;init;}
    public int Quantity {get;init;}
    public decimal Price {get;init;}
    public OrderStatus Status {get;set;}
    public DateTimeOffset CreatedAt {get;init;}

}

public enum OrderSide
{
    Buy = 1,
    Sell = 2
}

public enum OrderStatus
{
    Received = 1,
    Processing = 2,
    Executed = 3,
    Rejected = 4,
    Cancelled = 5
}
