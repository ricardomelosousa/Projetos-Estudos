using System;
using System.Collections.Generic;
using System.Text;

namespace Trading.TradeProcessor.Worker.Domain
{
    public sealed class ProcessedMessage
    {
        public Guid MessageId { get; set; }

        public DateTime ProcessedAt { get; set; }
    }
}
