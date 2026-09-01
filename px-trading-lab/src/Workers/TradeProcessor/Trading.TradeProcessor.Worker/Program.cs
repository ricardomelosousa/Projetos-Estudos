using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Trading.TradeProcessor.Worker;
using Trading.TradeProcessor.Worker.Configuration;
using Trading.TradeProcessor.Worker.Infrastructure;
using Trading.TradeProcessor.Worker.Observability;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<KafkaOptions>(
    builder.Configuration.GetSection("Kafka"));

builder.Services.AddDbContext<TradeDbContext>(options =>
{
    options.UseNpgsql(
        builder.Configuration.GetConnectionString("Postgres"));
});

builder.Services.AddHostedService<OrderCreatedConsumer>();

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
                TradeProcessorTelemetry.ActivitySourceName)           
            .AddHttpClientInstrumentation()
            .AddEntityFrameworkCoreInstrumentation()
            .AddOtlpExporter();
    });

var host = builder.Build();

using (var scope = host.Services.CreateScope())
{
    var dbContext =
        scope.ServiceProvider.GetRequiredService<TradeDbContext>();

    dbContext.Database.Migrate();
}

host.Run();