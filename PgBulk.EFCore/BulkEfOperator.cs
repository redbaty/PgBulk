using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PgBulk.EFCore;

public class BulkEfOperator : BulkOperator
{
    private ILogger<BulkEfOperator> Logger { get; }

    public BulkEfOperator(DbContext dbContext, int? timeoutOverride) : base(OverrideCommandTimeout(dbContext.Database.GetConnectionString(), timeoutOverride), new EntityTableInformationProvider(dbContext))
    {
        Logger = dbContext.GetService<ILogger<BulkEfOperator>>();
    }

    private static string OverrideCommandTimeout(string? originalConnectionString, int? timeoutOverride)
    {
        var newConnectionString = new NpgsqlConnectionStringBuilder(originalConnectionString);

        if (timeoutOverride.HasValue) newConnectionString.CommandTimeout = timeoutOverride.Value;

        return newConnectionString.ToString();
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