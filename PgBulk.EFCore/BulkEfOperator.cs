using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PgBulk.EFCore;

public class BulkEfOperator : BulkOperator
{
    public BulkEfOperator(DbContext dbContext, int? timeoutOverride) : base(OverrideCommandTimeout(dbContext.Database.GetConnectionString(), timeoutOverride), new EntityTableInformationProvider(dbContext))
    {
        DbContext = dbContext;
        DisposeConnection = false;

        var serviceProvider = dbContext.GetInfrastructure();
        Logger = serviceProvider.GetService<ILogger<BulkEfOperator>>();
    }

    private ILogger<BulkEfOperator>? Logger { get; }

    private DbContext DbContext { get; }

    private static string OverrideCommandTimeout(string? originalConnectionString, int? timeoutOverride)
    {
        var newConnectionString = new NpgsqlConnectionStringBuilder(originalConnectionString);

        if (timeoutOverride.HasValue) newConnectionString.CommandTimeout = timeoutOverride.Value;

        return newConnectionString.ToString();
    }

    protected override Task<NpgsqlConnection> CreateOpenedConnection()
    {
        return DbContext.Database.GetDbConnection() is NpgsqlConnection npgsqlConnection
            ? Task.FromResult(npgsqlConnection)
            : throw new InvalidOperationException("Connection is not NpgsqlConnection");
    }

    public override void LogBeforeCommand(NpgsqlCommand npgsqlCommand)
    {
        Logger?.LogInformation("Executing command {@Command}", npgsqlCommand.CommandText);
    }

    public override void LogAfterCommand(NpgsqlCommand npgsqlCommand, TimeSpan elapsed)
    {
        Logger?.LogInformation("Executed DbCommand ({ElapsedMilliseconds}ms) {@Command}", elapsed.TotalMilliseconds, npgsqlCommand.CommandText);
    }
}