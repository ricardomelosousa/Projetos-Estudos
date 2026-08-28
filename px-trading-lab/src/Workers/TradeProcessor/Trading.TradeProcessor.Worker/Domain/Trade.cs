using System;
using System.Collections.Generic;
using System.Text;

namespace Trading.TradeProcessor.Worker.Domain
{
    public sealed class Trade
    {
        public Guid Id { get; set; }

        public Guid OrderId { get; set; }

        public Guid CustomerId { get; set; }

        public string Symbol { get; set; } = null!;

        public string Side { get; set; } = null!;

        public decimal Quantity { get; set; }

        public decimal RequestedPrice { get; set; }

        public decimal ExecutedPrice { get; set; }

        public DateTime ExecutedAt { get; set; }
    }
}
