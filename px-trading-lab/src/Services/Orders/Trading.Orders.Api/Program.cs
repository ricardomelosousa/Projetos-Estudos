using System.Collections.Concurrent;
using Trading.Orders.Api.Contracts;
using Trading.Orders.Api.Models;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

var orders = new ConcurrentDictionary<Guid, Order>();

app.MapPost("/api/orders", (CreateOrderRequest request) =>
{
    if (string.IsNullOrWhiteSpace(request.AccountId))
    {
        return Results.BadRequest(new {error = "AccountId is required."});
    }
    if (string.IsNullOrWhiteSpace(request.Symbol))
    {
        return Results.BadRequest(new {error = "Symbol is required."});
    }
    if(request.Quantity <= 0)
    {
        return Results.BadRequest(new {error = "Quantity must be greater than zero."});
    }
    if(request.Price <= 0)
    {
        return Results.BadRequest(new {error = "Price must be greater than zero."});
    }

    var order = new Order
    {
        Id = Guid.NewGuid(),
        AccountId = request.AccountId,
        Symbol = request.Symbol.ToUpperInvariant(),
        Side = request.Side,
        Quantity = request.Quantity,
        Price = request.Price,
        Status = OrderStatus.Received,
        CreatedAt = DateTimeOffset.UtcNow
    };

    orders[order.Id] = order;

    return Results.Created($"/api/orders/{order.Id}", new CreateOrderResponse(order.Id, order.Status.ToString()));
});

app.MapGet("/api/orders/{id:guid}", (Guid id) =>
{
   return orders.TryGetValue(id, out var order) ? Results.Ok(order) : Results.NotFound();
});

app.MapGet("/healh/live", ()=> Results.Ok(new {status = "Healthy", service = "Trading.Orders.Api", data = DateTime.Now}));

app.Run();


