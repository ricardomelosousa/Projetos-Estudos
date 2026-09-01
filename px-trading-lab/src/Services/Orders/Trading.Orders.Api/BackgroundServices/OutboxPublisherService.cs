using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using System.Diagnostics;
using System.Text;
using Trading.Orders.Api.Configuration;
using Trading.Orders.Api.Data;
using Trading.Orders.Api.Observability;

namespace Trading.Orders.Api.BackgroundServices
{
    public sealed class OutboxPublisherService : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly KafkaOptions _kafkaOptions;
        private readonly ILogger<OutboxPublisherService> _logger;

        public OutboxPublisherService(IServiceScopeFactory scopeFactory, KafkaOptions kafkaOptions, ILogger<OutboxPublisherService> logger)
        {
            _scopeFactory = scopeFactory;
            _kafkaOptions = kafkaOptions;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _kafkaOptions.BootstrapServers,
                Acks = Acks.All
            };

            using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

            while (!stoppingToken.IsCancellationRequested)
            {

                try
                {
                    await PublishPendingMessage(producer, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing outbox messages");

                }

                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        private async Task PublishPendingMessage(IProducer<string, string> producer, CancellationToken cancellation)
        {
            using var scope = _scopeFactory.CreateScope();

            var dbContext = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();

            var messages = await dbContext.OutboxMessages.Where(x => x.ProcessedAt == null).OrderBy(x => x.OccurredAt).Take(20).ToListAsync();

            foreach (var message in messages)
            {
                try
                {
                    var headers = new Headers();

                    if (!string.IsNullOrWhiteSpace(
                        message.TraceParent))
                    {
                        headers.Add(
                            "traceparent",
                            Encoding.UTF8.GetBytes(
                                message.TraceParent));
                    }

                    if (!string.IsNullOrWhiteSpace(
                        message.TraceState))
                    {
                        headers.Add(
                            "tracestate",
                            Encoding.UTF8.GetBytes(
                                message.TraceState));
                    }


                    var message_broker =
                                 new Message<string, string>
                                 {
                                     Key = message.Id.ToString(),
                                     Value = message.Payload,
                                     Headers = headers
                                 };



                    var result = await producer.ProduceAsync(_kafkaOptions.Topics.OrderCreated, message_broker, cancellation);
                    message.ProcessedAt = DateTimeOffset.UtcNow;
                    message.Error = null;
                    _logger.LogInformation("Outbox message {MessageId} published to topic {Topic}, partition {Partition}, offeset {Offset}",
                        message.Id,
                        result.Topic,
                        result.Partition.Value,
                        result.Offset.Value);
                    ActivityContext parentContext = default;
                    if (!string.IsNullOrWhiteSpace(message.TraceParent))
                    {
                        ActivityContext.TryParse(
                            message.TraceParent,
                            message.TraceState,
                            out parentContext);
                    }
                    _logger.LogInformation("OUTBOX TRACE - MessageId={MessageId} ParentValid={ParentValid} ParentTraceId={ParentTraceId} ParentSpanId={ParentSpanId}",
                                            message.Id,
                                            parentContext != default,
                                            parentContext.TraceId,
                                            parentContext.SpanId);

                    using var activity = OrdersTelemetry.ActivitySource.StartActivity("kafka.publish.orders-created", ActivityKind.Producer, parentContext);

                    _logger.LogInformation("KAFKA PRODUCER TRACE - ActivityCreated={Created} TraceId={TraceId} SpanId={SpanId} ParentSpanId={ParentSpanId}",
                                            activity is not null,
                                            activity?.TraceId,
                                            activity?.SpanId,
                                            activity?.ParentSpanId);

                    activity?.SetTag(
                         "messaging.system",
                         "kafka");

                    activity?.SetTag(
                        "messaging.destination.name",
                        _kafkaOptions.Topics.OrderCreated);

                    activity?.SetTag(
                        "messaging.operation",
                        "publish");

                    activity?.SetTag(
                        "messaging.message.id",
                        message.Id);

                }
                catch (Exception ex)
                {
                    message.RetryCount++;
                    message.Error = ex.Message;

                    _logger.LogError(ex, "Failed to publish outbox message {MessageId}", message.Id);
                }
            }
            await dbContext.SaveChangesAsync(cancellation);

        }

    }
}
