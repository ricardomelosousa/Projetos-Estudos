using Microsoft.EntityFrameworkCore;
using Trading.TradeProcessor.Worker;
using Trading.TradeProcessor.Worker.Configuration;
using Trading.TradeProcessor.Worker.Infrastructure;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<KafkaOptions>(
    builder.Configuration.GetSection("Kafka"));

builder.Services.AddDbContext<TradeDbContext>(options =>
{
    options.UseNpgsql(
        builder.Configuration.GetConnectionString("Postgres"));
});

builder.Services.AddHostedService<OrderCreatedConsumer>();

var host = builder.Build();

using (var scope = host.Services.CreateScope())
{
    var dbContext =
        scope.ServiceProvider.GetRequiredService<TradeDbContext>();

    dbContext.Database.Migrate();
}

host.Run();