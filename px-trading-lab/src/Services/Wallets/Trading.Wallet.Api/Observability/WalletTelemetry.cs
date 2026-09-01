using System.Diagnostics;

namespace Trading.Wallet.Api.Observability
{
    public static class WalletTelemetry
    {
        public const string ActivitySourceName =
            "Trading.Wallet.Api";

        public static readonly ActivitySource ActivitySource =
            new(ActivitySourceName);
    }
}
