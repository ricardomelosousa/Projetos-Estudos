using System;
using System.Collections.Generic;
using System.Text;

namespace Trading.TradeProcessor.Worker.Contracts
{
    public sealed record OrderCreatedEvent(
     Guid MessageId,
     Guid OrderId,
     Guid CustomerId,
     string Symbol,
     string Side,
     decimal Quantity,
     decimal Price,
     DateTime CreatedAt
 );

}
