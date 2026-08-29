using System.Text.Json;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Trading.Wallet.Api.Contracts;
using Trading.Wallet.Api.Domain.Entities;
using Trading.Wallet.Api.Infrastructure.Persistence;

namespace Trading.Wallet.Api.Consumers;

public class OrderCreatedConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OrderCreatedConsumer> _logger;
    private readonly IConfiguration _configuration;

    public OrderCreatedConsumer(
        IServiceScopeFactory scopeFactory,
        ILogger<OrderCreatedConsumer> logger,
        IConfiguration configuration)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers =
                _configuration["Kafka:BootstrapServers"],

            GroupId = "wallet-service",

            AutoOffsetReset = AutoOffsetReset.Earliest,

            EnableAutoCommit = false
        };

        using var consumer =
            new ConsumerBuilder<string, string>(config)
                .Build();

        consumer.Subscribe(
            _configuration["Kafka:Topics:OrderCreated"]);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult =
                    consumer.Consume(stoppingToken);

                var order =
                    JsonSerializer.Deserialize<OrderCreatedEvent>(
                        consumeResult.Message.Value);

                if (order is null)
                    continue;

                await ProcessOrderAsync(
                    order,
                    stoppingToken);

                consumer.Commit(consumeResult);
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(
                    ex,
                    "Erro consumindo OrderCreated");
            }
        }
    }
    private async Task ProcessOrderAsync(
    OrderCreatedEvent order,
    CancellationToken cancellationToken)
    {
        await using var scope =
            _scopeFactory.CreateAsyncScope();

        var db =
            scope.ServiceProvider
                .GetRequiredService<WalletDbContext>();

        var alreadyProcessed =
            await db.Reservations
                .AnyAsync(
                    x => x.OrderId == order.OrderId,
                    cancellationToken);

        if (alreadyProcessed)
        {
            _logger.LogInformation(
                "Order {OrderId} já processada.",
                order.OrderId);

            return;
        }

        var wallet =
            await db.Wallets
                .SingleOrDefaultAsync(
                    x => x.InvestorId == order.InvestorId,
                    cancellationToken);

        if (wallet is null)
        {
            await CreateFailureEvent(
                db,
                order,
                0,
                "Wallet não encontrada",
                cancellationToken);

            return;
        }

        var amount =
            order.Quantity * order.Price;

        var availableBalance =
            wallet.Balance - wallet.ReservedBalance;

        if (availableBalance < amount)
        {
            await CreateFailureEvent(
                db,
                order,
                amount,
                "Saldo insuficiente",
                cancellationToken);

            return;
        }

        var reservation =
            new WalletReservation
            {
                Id = Guid.NewGuid(),
                OrderId = order.OrderId,
                WalletId = wallet.Id,
                Amount = amount,
                Status = ReservationStatus.Reserved
            };

        wallet.ReservedBalance += amount;

        db.Reservations.Add(reservation);

        var @event =
            new FundsReservedEvent(
                order.OrderId,
                order.InvestorId,
                reservation.Id,
                amount,
                DateTime.UtcNow);

        db.OutboxMessages.Add(
            new OutboxMessage
            {
                Id = Guid.NewGuid(),
                Type = "FundsReserved",
                Payload =
                    JsonSerializer.Serialize(@event),
                OccurredOnUtc = DateTime.UtcNow
            });

        await db.SaveChangesAsync(cancellationToken);

        _logger.LogInformation(
            "R$ {Amount} reservado para Order {OrderId}",
            amount,
            order.OrderId);
    }

    private static async Task CreateFailureEvent(
    WalletDbContext db,
    OrderCreatedEvent order,
    decimal amount,
    string reason,
    CancellationToken cancellationToken)
    {
        var @event =
            new FundsReservationFailedEvent(
                order.OrderId,
                order.InvestorId,
                amount,
                reason,
                DateTime.UtcNow);

        db.OutboxMessages.Add(
            new OutboxMessage
            {
                Id = Guid.NewGuid(),
                Type = "FundsReservationFailed",
                Payload =
                    JsonSerializer.Serialize(@event),
                OccurredOnUtc = DateTime.UtcNow
            });

        await db.SaveChangesAsync(cancellationToken);
    }
}