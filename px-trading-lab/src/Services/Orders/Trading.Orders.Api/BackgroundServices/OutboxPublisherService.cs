using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Trading.Orders.Api.Configuration;
using Trading.Orders.Api.Data;

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
                    var result = await producer.ProduceAsync(_kafkaOptions.Topics.OrderCreated, new Message<string, string> { Key = message.Id.ToString(), Value = message.Payload }, cancellation);
                    message.ProcessedAt = DateTimeOffset.UtcNow;
                    message.Error = null;
                    _logger.LogInformation("Outbox message {MessageId} published to topic {Topic}, partition {Partition}, offeset {Offset}",
                        message.Id,
                        result.Topic,
                        result.Partition.Value,
                        result.Offset.Value);
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
