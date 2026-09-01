using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Trading.TradeProcessor.Worker.Observability
{
    public static class TradeProcessorTelemetry
    {
        public const string ActivitySourceName =
            "Trading.TradeProcessor.Worker";

        public static readonly ActivitySource ActivitySource =
            new(ActivitySourceName);
    }
}
