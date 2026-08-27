using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Trading.Orders.Api.Models;

namespace Trading.Orders.Api.Data.Configuration;

public sealed class OrderConfiguration : IEntityTypeConfiguration<Order>
{
    public void Configure(EntityTypeBuilder<Order> builder)
    {
        builder.ToTable("orders");
        builder.HasKey(x => x.Id);
        builder.Property(x => x.Id).HasColumnName("id");
        builder.Property(x => x.AccountId).HasColumnName("account_id").HasMaxLength(50).IsRequired();
        builder.Property(x => x.Symbol).HasColumnName("symbol").HasMaxLength(20).IsRequired();
        builder.Property(x=> x.Side).HasColumnName("side").HasConversion<string>().HasMaxLength(10).IsRequired();
        builder.Property(x => x.Quantity).HasColumnName("quantity").IsRequired();
        builder.Property(x => x.Price).HasColumnName("price").HasPrecision(18, 8).IsRequired();
        builder.Property(x => x.Status).HasColumnName("status").HasConversion<string>().HasMaxLength(20).IsRequired();
        builder.Property(x => x.CreatedAt).HasColumnName("created_at").IsRequired();

        builder.HasIndex(x => x.AccountId);
        builder.HasIndex(x => x.Symbol);
        builder.HasIndex(x=> x.CreatedAt);


    }
}