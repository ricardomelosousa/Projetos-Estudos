using Microsoft.EntityFrameworkCore;
using Trading.Wallet.Api.Consumers;
using Trading.Wallet.Api.Infrastructure.Persistence;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<WalletDbContext>(options =>
    options.UseNpgsql(
        builder.Configuration.GetConnectionString(
            "WalletDatabase")));

builder.Services.AddHostedService<OrderCreatedConsumer>();

var app = builder.Build();

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

using (var scope = app.Services.CreateScope())
{
    var dbContext = scope.ServiceProvider
        .GetRequiredService<WalletDbContext>();

    dbContext.Database.Migrate();
}

app.Run();

public record CreateWalletRequest(
    Guid InvestorId,
    decimal Balance);