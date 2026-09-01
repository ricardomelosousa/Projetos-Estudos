using System.Diagnostics;

namespace Trading.Orders.Api.Observability
{
    public static class OrdersTelemetry
    {
        public const string ActivitySourceName =
            "Trading.Orders.Api";

        public static readonly ActivitySource ActivitySource =
            new(ActivitySourceName);
    }
}
