using Microsoft.EntityFrameworkCore;
using Trading.Wallet.Api.Domain.Entities;



namespace Trading.Wallet.Api.Infrastructure.Persistence
{

    public class WalletDbContext : DbContext
    {
        public WalletDbContext(
            DbContextOptions<WalletDbContext> options)
            : base(options)
        {
        }

        public DbSet<Wallet.Api.Domain.Entities.Wallet> Wallets => Set<Domain.Entities.Wallet>();

        public DbSet<WalletReservation> Reservations =>
            Set<WalletReservation>();

        public DbSet<OutboxMessage> OutboxMessages =>
            Set<OutboxMessage>();
        public DbSet<ProcessedMessage> ProcessedMessages { get; set; }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Domain.Entities.Wallet>()
                .HasIndex(x => x.InvestorId)
                .IsUnique();

            modelBuilder.Entity<WalletReservation>()
                .HasIndex(x => x.OrderId)
                .IsUnique();

            modelBuilder.Entity<Domain.Entities.Wallet>()
                .Property(x => x.Balance)
                .HasPrecision(18, 2);

            modelBuilder.Entity<Domain.Entities.Wallet>()
                .Property(x => x.ReservedBalance)
                .HasPrecision(18, 2);

            modelBuilder.Entity<WalletReservation>()
                .Property(x => x.Amount)
                .HasPrecision(18, 2);

            modelBuilder.Entity<ProcessedMessage>(entity =>
            {
                entity.ToTable("processed_messages");

                entity.HasKey(x => x.Id);

                entity.Property(x => x.MessageId)
                    .IsRequired()
                    .HasMaxLength(100);

                entity.Property(x => x.Consumer)
                    .IsRequired()
                    .HasMaxLength(100);

                entity.Property(x => x.ProcessedAt)
                    .IsRequired();

                entity.HasIndex(x => new
                {
                    x.MessageId,
                    x.Consumer
                })
                .IsUnique();
            });

        }
    }

}