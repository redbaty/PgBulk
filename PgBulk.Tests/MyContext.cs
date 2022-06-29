using Microsoft.EntityFrameworkCore;

namespace PgBulk.Tests;

public class MyContext : DbContext
{
    public MyContext(DbContextOptions options) : base(options)
    {
    }

    public DbSet<TestRow> TestRows => Set<TestRow>();
}