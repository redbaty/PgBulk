using Microsoft.EntityFrameworkCore;

namespace PgBulk.Tests;

public class MyContext : DbContext
{
    public MyContext(DbContextOptions options) : base(options)
    {
    }

    public DbSet<TestRow> TestRows => Set<TestRow>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasPostgresEnum<TestEnum>();
        modelBuilder.Entity<TestRow>(e => { e.Property(i => i.Id).ValueGeneratedNever(); });
    }
}