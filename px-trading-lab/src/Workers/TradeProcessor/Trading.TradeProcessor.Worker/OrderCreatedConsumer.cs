using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System.Text.Json;
using Trading.TradeProcessor.Worker.Configuration;
using Trading.TradeProcessor.Worker.Contracts;
using Trading.TradeProcessor.Worker.Domain;
using Trading.TradeProcessor.Worker.Infrastructure;
using Microsoft.EntityFrameworkCore;

namespace Trading.TradeProcessor.Worker;

public sealed class OrderCreatedConsumer : BackgroundService
{
    private readonly KafkaOptions _kafkaOptions;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OrderCreatedConsumer> _logger;

    public OrderCreatedConsumer(
        IOptions<KafkaOptions> kafkaOptions,
        IServiceScopeFactory scopeFactory,
        ILogger<OrderCreatedConsumer> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,

            GroupId = _kafkaOptions.GroupId,

            AutoOffsetReset = AutoOffsetReset.Earliest,

            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();

        consumer.Subscribe(_kafkaOptions.Topics.OrderCreated);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);

                var message = JsonSerializer.Deserialize<OrderCreatedEvent>(
                        result.Message.Value,
                        new JsonSerializerOptions
                        {
                            PropertyNameCaseInsensitive = true
                        });

                if (message is null)
                {
                    continue;
                }

                await ProcessMessageAsync(message, stoppingToken);

                consumer.Commit(result);
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Erro ao consumir mensagem Kafka");
            }
        }
    }

    private async Task ProcessMessageAsync(
        OrderCreatedEvent message,
        CancellationToken cancellationToken)
    {
        await using var scope =
            _scopeFactory.CreateAsyncScope();

        var dbContext =
            scope.ServiceProvider
                .GetRequiredService<TradeDbContext>();

        var alreadyProcessed =  await dbContext.ProcessedMessages.AnyAsync(
                    x => x.MessageId == message.MessageId,
                    cancellationToken);

        if (alreadyProcessed)
        {
            _logger.LogInformation(
                "Mensagem {MessageId} já processada.",
                message.MessageId);

            return;
        }

        await using var transaction =
            await dbContext.Database
                .BeginTransactionAsync(cancellationToken);

        var trade = new Trade
        {
            Id = Guid.NewGuid(),

            OrderId = message.OrderId,

            CustomerId = message.CustomerId,

            Symbol = message.Symbol,

            Side = message.Side,

            Quantity = message.Quantity,

            RequestedPrice = message.Price,

            ExecutedPrice = message.Price,

            ExecutedAt = DateTime.UtcNow
        };

        dbContext.Trades.Add(trade);

        dbContext.ProcessedMessages.Add(
            new ProcessedMessage
            {
                MessageId = message.MessageId,

                ProcessedAt = DateTime.UtcNow
            });

        await dbContext.SaveChangesAsync(
            cancellationToken);

        await transaction.CommitAsync(
            cancellationToken);

        _logger.LogInformation(
            "Trade executado. OrderId: {OrderId} TradeId: {TradeId}",
            trade.OrderId,
            trade.Id);
    }
}
