using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Trading.Wallet.Api.Contracts;
using Trading.Wallet.Api.Domain.Entities;
using Trading.Wallet.Api.Infrastructure.Persistence;
using Trading.Wallet.Api.Observability;

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
            ConsumeResult<string, string>? consumeResult = null;
            Activity? activity = null;

            try
            {
                consumeResult =
                    consumer.Consume(stoppingToken);

                var order =
                    JsonSerializer.Deserialize<OrderCreatedEvent>(
                        consumeResult.Message.Value);

                if (order is null)
                {
                    throw new InvalidOperationException(
                        "OrderCreated inválido.");
                }
                if (order.Symbol == "ERRO")
                {
                    throw new Exception("Falha simulada para testar Retry e DLQ.");
                }
                #region Observabilidade

                var traceParent = GetMessageBrokerHeader(consumeResult.Message.Headers, "traceparent");

                var traceState = GetMessageBrokerHeader(consumeResult.Message.Headers, "tracestate");

                _logger.LogInformation("WALLET KAFKA HEADER - TraceParent={TraceParent} TraceState={TraceState}",
                                       traceParent,
                                       traceState);

                ActivityContext parentContext = default;

                if (!string.IsNullOrWhiteSpace(traceParent))
                {
                    ActivityContext.TryParse(traceParent, traceState, out parentContext);
                }
                _logger.LogInformation("WALLET PARENT CONTEXT - Valid={Valid} TraceId={TraceId} SpanId={SpanId}",
                                        parentContext != default,
                                        parentContext.TraceId,
                                        parentContext.SpanId);

                activity = WalletTelemetry.ActivitySource.StartActivity("wallet.consume.order-created", ActivityKind.Consumer, parentContext);

                _logger.LogInformation("WALLET ACTIVITY - Created={Created} TraceId={TraceId} SpanId={SpanId} ParentSpanId={ParentSpanId}",
                                        activity is not null,
                                        activity?.TraceId,
                                        activity?.SpanId,
                                        activity?.ParentSpanId);

                activity?.SetTag(
                    "messaging.system",
                    "kafka");

                activity?.SetTag(
                    "messaging.destination.name",
                    consumeResult.Topic);

                activity?.SetTag(
                    "messaging.kafka.partition",
                    consumeResult.Partition.Value);

                activity?.SetTag(
                    "messaging.kafka.offset",
                    consumeResult.Offset.Value);

                activity?.SetTag(
                    "trading.order.id",
                    order.OrderId);

                activity?.SetTag(
                    "trading.message.id",
                    order.MessageId);

                activity?.SetTag(
                    "trading.order.symbol",
                    order.Symbol);

                activity?.SetTag(
                    "trading.order.quantity",
                    order.Quantity);

                activity?.SetTag(
                    "trading.order.price",
                    order.Price);

                #endregion

                _logger.LogInformation("ANTES PROCESSAMENTO - Activity.Current={Name} TraceId={TraceId} SpanId={SpanId}",
                                        Activity.Current?.DisplayName,
                                        Activity.Current?.TraceId,
                                        Activity.Current?.SpanId);

                await ProcessOrderWithRetryAsync(
                    order,
                    stoppingToken);

                activity?.SetStatus(ActivityStatusCode.Ok);

                consumer.Commit(consumeResult);

                _logger.LogInformation(
                    "Mensagem processada e offset confirmado. Offset={Offset}",
                    consumeResult.Offset.Value);
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(
                    ex,
                    "Erro consumindo mensagem do Kafka.");
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Falha definitiva no processamento de OrderCreated.");

                if (consumeResult is null)
                    continue;

                try
                {
                    Activity.Current?.SetStatus(ActivityStatusCode.Error, ex.Message);

                    Activity.Current?.SetTag(
                        "error.type",
                        ex.GetType().FullName);

                    Activity.Current?.SetTag(
                        "error.message",
                        ex.Message);

                    await SendToDlqAsync(
                        consumeResult,
                        ex,
                        stoppingToken);

                    consumer.Commit(consumeResult);

                    _logger.LogWarning(
                        "Mensagem enviada para DLQ e offset {Offset} confirmado.",
                        consumeResult.Offset.Value);
                }
                catch (Exception dlqException)
                {
                    _logger.LogCritical(
                        dlqException,
                        "Falha ao enviar mensagem para DLQ. Offset NÃO será confirmado.");

                    /*
                     * Não fazemos Commit aqui.
                     *
                     * A mensagem poderá ser entregue novamente.
                     */
                }
            }
            finally
            {
                activity?.Dispose();
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

        var consumerName =
    nameof(OrderCreatedConsumer);

        _logger.LogInformation("DENTRO PROCESSAMENTO - Activity.Current={Name} TraceId={TraceId} SpanId={SpanId}",
                                Activity.Current?.DisplayName,
                                Activity.Current?.TraceId,
                                Activity.Current?.SpanId);
        var alreadyProcessed2 =
            await db.ProcessedMessages
                .AnyAsync(
                    x =>
                        x.MessageId == order.MessageId
                        &&
                        x.Consumer == consumerName,
                    cancellationToken);

        if (alreadyProcessed)
        {
            _logger.LogInformation(
                "Mensagem {MessageId} da Order {OrderId} já processada.",
                order.MessageId,
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

            WalletMetrics.ReservationFailures.Add(1, new KeyValuePair<string, object?>("reason", "wallet_not_found"), new KeyValuePair<string, object?>("symbol", order.Symbol));

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

            WalletMetrics.ReservationFailures.Add(1, new KeyValuePair<string, object?>("reason", "insufficient_balance"), new KeyValuePair<string, object?>("symbol", order.Symbol));

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

        db.ProcessedMessages.Add(
            new ProcessedMessage
            {
                Id = Guid.NewGuid(),
                MessageId = order.MessageId,
                Consumer = nameof(OrderCreatedConsumer),
                ProcessedAt = DateTime.UtcNow
            });

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

        WalletMetrics.Reservations.Add(1, new KeyValuePair<string, object?>("symbol", order.Symbol));

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

        db.ProcessedMessages.Add(
            new ProcessedMessage
            {
                Id = Guid.NewGuid(),
                MessageId = order.MessageId,
                Consumer = nameof(OrderCreatedConsumer),
                ProcessedAt = DateTime.UtcNow
            });

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

    private async Task ProcessOrderWithRetryAsync(
        OrderCreatedEvent order,
        CancellationToken cancellationToken)
    {
        const int maxAttempts = 3;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await ProcessOrderAsync(
                    order,
                    cancellationToken);

                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Erro processando Order {OrderId}. Tentativa {Attempt}/{MaxAttempts}",
                    order.OrderId,
                    attempt,
                    maxAttempts);

                if (attempt == maxAttempts)
                {
                    throw;
                }

                var delay =
                    TimeSpan.FromSeconds(
                        Math.Pow(2, attempt));

                await Task.Delay(
                    delay,
                    cancellationToken);
            }
        }
    }


    private async Task SendToDlqAsync(
    ConsumeResult<string, string> consumeResult,
    Exception exception,
    CancellationToken cancellationToken)
    {
        var dlqTopic =
            _configuration["Kafka:Topics:OrderCreatedDlq"];

        using var producer = CreateProducer();

        var headers = new Headers
    {
        {
            "original-topic",
            System.Text.Encoding.UTF8.GetBytes(
                consumeResult.Topic)
        },
        {
            "original-partition",
            System.Text.Encoding.UTF8.GetBytes(
                consumeResult.Partition.Value.ToString())
        },
        {
            "original-offset",
            System.Text.Encoding.UTF8.GetBytes(
                consumeResult.Offset.Value.ToString())
        },
        {
            "error-message",
            System.Text.Encoding.UTF8.GetBytes(
                exception.Message)
        }
    };

        await producer.ProduceAsync(
            dlqTopic,
            new Message<string, string>
            {
                Key = consumeResult.Message.Key,
                Value = consumeResult.Message.Value,
                Headers = headers
            },
            cancellationToken);

        _logger.LogError(
            exception,
            "Mensagem enviada para DLQ {DlqTopic}. Topic original={Topic}, Partition={Partition}, Offset={Offset}",
            dlqTopic,
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value);
    }

    private IProducer<string, string> CreateProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers =
                _configuration["Kafka:BootstrapServers"]
        };

        return new ProducerBuilder<string, string>(config)
            .Build();
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