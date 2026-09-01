using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Trading.Orders.Api.BackgroundServices;
using Trading.Orders.Api.Configuration;
using Trading.Orders.Api.Contracts;
using Trading.Orders.Api.Data;
using Trading.Orders.Api.Models;
using Trading.Orders.Api.Observability;
using OpenTelemetry.Metrics;


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
builder.Services
    .AddOpenTelemetry()
    .ConfigureResource(resource =>
    {
        resource.AddService(
            serviceName: "trading-orders-api",
            serviceVersion: "1.0.0");
    })
    .WithTracing(tracing =>
    {
        tracing
            .SetSampler(new AlwaysOnSampler())
            .AddSource(
                OrdersTelemetry.ActivitySourceName)
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddEntityFrameworkCoreInstrumentation()
            .AddOtlpExporter();
    })
    .WithMetrics(metrics =>
    {
        metrics
            .AddMeter(OrdersMetrics.MeterName)
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddRuntimeInstrumentation()
            .AddOtlpExporter();
    });
builder.Services.AddControllers();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}



app.MapPost("/api/orders", async (CreateOrderRequest request, OrdersDbContext dbContext, ILogger<Program> logger, CancellationToken cancellationToken) =>
{
    if (request.InvestorId == Guid.Empty)
    {
        return Results.BadRequest(new
        {
            error = "InvestorId is required."
        });
    }
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

    // =========================================================
    // OBSERVABILIDADE
    // =========================================================

    logger.LogInformation(
    "OTel diagnóstico: Source={Source} HasListeners={HasListeners} " +
    "ActivityCurrent={ActivityCurrent} TraceIdCurrent={TraceIdCurrent} Recorded={Recorded}",
    OrdersTelemetry.ActivitySource.Name,
    OrdersTelemetry.ActivitySource.HasListeners(),
    Activity.Current?.DisplayName,
    Activity.Current?.TraceId,
    Activity.Current?.Recorded);

    using var activity =
        OrdersTelemetry.ActivitySource.StartActivity(
            "orders.create",
            ActivityKind.Internal);

    logger.LogInformation(
        "Orders Activity criada? {Created} TraceId={TraceId} SpanId={SpanId}",
        activity is not null,
        activity?.TraceId,
        activity?.SpanId);

    // =========================================================


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

    activity?.SetTag("trading.order.id", order.Id);
    activity?.SetTag("trading.order.symbol", order.Symbol);
    activity?.SetTag("trading.order.quantity", order.Quantity);
    activity?.SetTag("trading.order.price", order.Price);

    var @event = new OrderCreatedEvent(Guid.NewGuid(), order.Id, request.InvestorId,
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
        RetryCount = 0,
        TraceParent = Activity.Current?.Id,
        TraceState = Activity.Current?.TraceStateString
    };

    logger.LogInformation(
    "Outbox criada. MessageId={MessageId} TraceParent={TraceParent}",
    outboxMessage.Id,
    outboxMessage.TraceParent);

    dbContext.Orders.Add(order);
    dbContext.OutboxMessages.Add(outboxMessage);
    await dbContext.SaveChangesAsync(cancellationToken);

    logger.LogInformation(
    "METRIC DEBUG - MeterName={MeterName} Enabled={Enabled}",
    OrdersMetrics.Meter.Name,
    OrdersMetrics.OrdersCreated.Enabled);

    OrdersMetrics.OrdersCreated.Add(1, new KeyValuePair<string, object?>("symbol", order.Symbol), new KeyValuePair<string, object?>("side", order.Side.ToString()));

    activity?.SetStatus(ActivityStatusCode.Ok);


    return Results.Created($"/api/orders/{order.Id}", new CreateOrderResponse(order.Id, order.Status.ToString()));
});

app.MapGet("/api/orders/{id:guid}", async (Guid id, OrdersDbContext dbContext, CancellationToken cancellationToken) =>
{
    var order = await dbContext.Orders.AsNoTracking().FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
    return order is not null ? Results.Ok(order) : Results.NotFound();
});

app.MapGet("/healh/live", () => Results.Ok(new { status = "Healthy", service = "Trading.Orders.Api", data = DateTime.Now }));

using (var scope = app.Services.CreateScope())
{
    var dbContext = scope.ServiceProvider
        .GetRequiredService<OrdersDbContext>();

    dbContext.Database.Migrate();
}

app.Run();



