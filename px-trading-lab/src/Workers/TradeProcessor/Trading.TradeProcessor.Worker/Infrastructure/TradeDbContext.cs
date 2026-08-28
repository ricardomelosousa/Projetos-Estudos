using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Text;
using Trading.TradeProcessor.Worker.Domain;

namespace Trading.TradeProcessor.Worker.Infrastructure
{
    public sealed class TradeDbContext : DbContext
    {
        public TradeDbContext(DbContextOptions<TradeDbContext> options)
            : base(options)
        {
        }

        public DbSet<Trade> Trades => Set<Trade>();

        public DbSet<ProcessedMessage> ProcessedMessages =>
            Set<ProcessedMessage>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Trade>(entity =>
            {
                entity.ToTable("trades");

                entity.HasKey(x => x.Id);

                entity.HasIndex(x => x.OrderId)
                    .IsUnique();

                entity.Property(x => x.Symbol)
                    .HasMaxLength(20)
                    .IsRequired();

                entity.Property(x => x.Side)
                    .HasMaxLength(10)
                    .IsRequired();
            });

            modelBuilder.Entity<ProcessedMessage>(entity =>
            {
                entity.ToTable("processed_messages");

                entity.HasKey(x => x.MessageId);
            });
        }
    }
}
