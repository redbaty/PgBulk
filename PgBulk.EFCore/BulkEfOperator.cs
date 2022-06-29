using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PgBulk.EFCore;

public class BulkEfOperator : BulkOperator
{
    private ILogger<BulkEfOperator> Logger { get; }

    public BulkEfOperator(DbContext dbContext) : base(dbContext.Database.GetConnectionString()!, new EntityTableInformationProvider(dbContext))
    {
        Logger = dbContext.GetService<ILogger<BulkEfOperator>>();
    }

    public override void LogBeforeCommand(NpgsqlCommand npgsqlCommand)
    {
       Logger.LogInformation("Executing command {@Command}", npgsqlCommand.CommandText);
    }

    public override void LogAfterCommand(NpgsqlCommand npgsqlCommand, TimeSpan elapsed)
    {
        Logger.LogInformation("Executed DbCommand ({ElapsedMilliseconds}ms) {@Command}", elapsed.TotalMilliseconds, npgsqlCommand.CommandText);
    }
}