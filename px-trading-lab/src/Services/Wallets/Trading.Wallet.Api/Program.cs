using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Trading.Wallet.Api.Consumers;
using Trading.Wallet.Api.Infrastructure.Persistence;
using Trading.Wallet.Api.Observability;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<WalletDbContext>(options =>
    options.UseNpgsql(
        builder.Configuration.GetConnectionString(
            "WalletDatabase")));

builder.Services.AddHostedService<OrderCreatedConsumer>();

builder.Services
    .AddOpenTelemetry()
    .ConfigureResource(resource =>
    {
        resource.AddService(
            serviceName: "trading-wallet-api",
            serviceVersion: "1.0.0");
    })
    .WithTracing(tracing =>
    {
        tracing
            .SetSampler(new AlwaysOnSampler())
            .AddSource(WalletTelemetry.ActivitySourceName)
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddEntityFrameworkCoreInstrumentation()
            .AddOtlpExporter();
    })
    .WithMetrics(metrics =>
    {
        metrics
            .AddMeter(WalletMetrics.MeterName)
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddRuntimeInstrumentation()
            .AddOtlpExporter();
    });

builder.Services.AddControllers();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var dbContext = scope.ServiceProvider
        .GetRequiredService<WalletDbContext>();

    dbContext.Database.Migrate();
}

app.MapGet("/health", () =>
    Results.Ok(new
    {
        service = "wallet-service",
        status = "healthy"
    }));

app.MapPost(
    "/wallets",
    async (
        CreateWalletRequest request,
        WalletDbContext db) =>
    {
        var wallet =
            new Trading.Wallet.Api.Domain.Entities.Wallet
            {
                Id = Guid.NewGuid(),
                InvestorId = request.InvestorId,
                Balance = request.Balance,
                ReservedBalance = 0
            };

        db.Wallets.Add(wallet);

        await db.SaveChangesAsync();

        return Results.Created(
            $"/wallets/{wallet.Id}",
            wallet);
    });



app.Run();

public record CreateWalletRequest(
    Guid InvestorId,
    decimal Balance);