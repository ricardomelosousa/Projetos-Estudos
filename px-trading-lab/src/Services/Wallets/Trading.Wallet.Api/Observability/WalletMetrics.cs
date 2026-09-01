using System.Diagnostics.Metrics;

namespace Trading.Wallet.Api.Observability
{
    public static class WalletMetrics
    {
        public const string MeterName = "Trading.Wallet.Api.Metrics";

        public static readonly Meter Meter =
            new(MeterName);

        public static readonly Counter<long> Reservations =
            Meter.CreateCounter<long>(
                "wallet.reservations.total",
                unit: "{reservation}",
                description: "Total de reservas de saldo realizadas com sucesso.");

        public static readonly Counter<long> ReservationFailures =
            Meter.CreateCounter<long>(
                "wallet.reservation.failures.total",
                unit: "{failure}",
                description: "Total de falhas de reserva de saldo.");
    }
}
