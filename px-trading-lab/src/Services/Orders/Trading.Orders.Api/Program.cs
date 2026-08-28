using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Text.Json;
using Trading.Orders.Api.BackgroundServices;
using Trading.Orders.Api.Configuration;
using Trading.Orders.Api.Contracts;
using Trading.Orders.Api.Data;
using Trading.Orders.Api.Models;


var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("OrdersDatabase")
?? throw new InvalidOperationException("Connection string 'OrdersDatabase' not found.");

builder.Services.AddDbContext<OrdersDbContext>(options =>
{
    options.UseNpgsql(connectionString);
});

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));
builder.Services.AddSingleton(sp =>
    sp.GetRequiredService<IOptions<KafkaOptions>>().Value);
builder.Services.AddHostedService<OutboxPublisherService>();

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}



app.MapPost("/api/orders", async (CreateOrderRequest request, OrdersDbContext dbContext, CancellationToken cancellationToken) =>
{
    if (string.IsNullOrWhiteSpace(request.AccountId))
    {
        return Results.BadRequest(new { error = "AccountId is required." });
    }
    if (string.IsNullOrWhiteSpace(request.Symbol))
    {
        return Results.BadRequest(new { error = "Symbol is required." });
    }
    if (request.Quantity <= 0)
    {
        return Results.BadRequest(new { error = "Quantity must be greater than zero." });
    }
    if (request.Price <= 0)
    {
        return Results.BadRequest(new { error = "Price must be greater than zero." });
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
    var @event = new OrderCreatedEvent(order.Id,
        order.AccountId,
        order.Symbol,
        order.Side.ToString(),
        order.Quantity,
        order.Price,
        order.CreatedAt);

    var payload = JsonSerializer.Serialize(@event);

    var outboxMessage = new OutboxMessage
    {
        Id = Guid.NewGuid(),
        Type = nameof(OrderCreatedEvent),
        Payload = payload,
        OccurredAt = DateTimeOffset.UtcNow,
        ProcessedAt = null,
        RetryCount = 0
    };

    dbContext.Orders.Add(order);
    dbContext.OutboxMessages.Add(outboxMessage);
    await dbContext.SaveChangesAsync(cancellationToken);

    return Results.Created($"/api/orders/{order.Id}", new CreateOrderResponse(order.Id, order.Status.ToString()));
});

app.MapGet("/api/orders/{id:guid}", async (Guid id, OrdersDbContext dbContext, CancellationToken cancellationToken) =>
{
    var order = await dbContext.Orders.AsNoTracking().FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
    return order is not null ? Results.Ok(order) : Results.NotFound();
});

app.MapGet("/healh/live", () => Results.Ok(new { status = "Healthy", service = "Trading.Orders.Api", data = DateTime.Now }));

app.Run();



