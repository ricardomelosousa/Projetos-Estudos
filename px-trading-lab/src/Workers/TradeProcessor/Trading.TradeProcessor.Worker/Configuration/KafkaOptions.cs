using System;
using System.Collections.Generic;
using System.Text;

namespace Trading.TradeProcessor.Worker.Configuration
{
    public sealed class KafkaOptions
    {
        public string BootstrapServers { get; set; } = string.Empty;

        public string GroupId { get; set; } = string.Empty;

        public KafkaTopicsOptions Topics { get; set; } = new();
    }

    public sealed class KafkaTopicsOptions
    {
        public string OrderCreated { get; set; } = string.Empty;
    }
}
