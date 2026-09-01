using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Trading.TradeProcessor.Worker.Configuration;
using Trading.TradeProcessor.Worker.Contracts;
using Trading.TradeProcessor.Worker.Domain;
using Trading.TradeProcessor.Worker.Infrastructure;
using Trading.TradeProcessor.Worker.Observability;

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
                #region Observabilidade 

                var traceParent = GetMessageBrokerHeader(result.Message.Headers, "traceparent");

                var traceState = GetMessageBrokerHeader(result.Message.Headers, "tracestate");

                _logger.LogInformation("TRADE KAFKA HEADER - TraceParent={TraceParent} TraceState={TraceState}",
                                        traceParent,
                                        traceState);

                ActivityContext parentContext = default;

                if (!string.IsNullOrWhiteSpace(traceParent))
                {
                    ActivityContext.TryParse(
                        traceParent,
                        traceState,
                        out parentContext);
                }

                _logger.LogInformation("TRADE PARENT CONTEXT - Valid={Valid} TraceId={TraceId} SpanId={SpanId}",
                                         parentContext != default,
                                         parentContext.TraceId,
                                         parentContext.SpanId);

                using var activity = TradeProcessorTelemetry.ActivitySource.StartActivity("trade.consume.order-created", ActivityKind.Consumer, parentContext);

                _logger.LogInformation("TRADE ACTIVITY - Created={Created} TraceId={TraceId} SpanId={SpanId} ParentSpanId={ParentSpanId}",
                                        activity is not null,
                                        activity?.TraceId,
                                        activity?.SpanId,
                                        activity?.ParentSpanId);

                activity?.SetTag(
                    "messaging.system",
                    "kafka");

                activity?.SetTag(
                    "messaging.destination.name",
                    result.Topic);

                activity?.SetTag(
                    "messaging.kafka.partition",
                    result.Partition.Value);

                activity?.SetTag(
                    "messaging.kafka.offset",
                    result.Offset.Value);

                activity?.SetTag(
                    "trading.order.id",
                    message.OrderId);

                activity?.SetTag(
                    "trading.message.id",
                    message.MessageId);

                activity?.SetTag(
                    "trading.order.symbol",
                    message.Symbol);

                #endregion

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

        var alreadyProcessed = await dbContext.ProcessedMessages.AnyAsync(
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

    private static string? GetMessageBrokerHeader(
Headers headers,
string key)
    {
        var header =
            headers.LastOrDefault(
                x => x.Key == key);

        if (header is null)
            return null;

        return Encoding.UTF8.GetString(
            header.GetValueBytes());
    }
}
