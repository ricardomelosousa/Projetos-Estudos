using System.Diagnostics.Metrics;

namespace Trading.Orders.Api.Observability
{
    public static class OrdersMetrics
    {
        public const string MeterName = "Trading.Orders.Api.Metrics";

        public static readonly Meter Meter =
            new(MeterName);

        public static readonly Counter<long> OrdersCreated =
            Meter.CreateCounter<long>(
                name: "orders.created.total",
                unit: "{order}",
                description: "Total de ordens criadas com sucesso.");
    }
}
