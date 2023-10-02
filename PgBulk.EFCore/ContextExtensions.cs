using Microsoft.EntityFrameworkCore;

namespace PgBulk.EFCore;

public static class ContextExtensions
{
    public static Task BulkSyncAsync<T>(this DbContext dbContext, IEnumerable<T> entities, string? deleteWhere = null, int? timeoutOverride = 600, bool useContextConnection = true) where T : class
    {
        var @operator = new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
        return @operator.SyncAsync(entities, deleteWhere);
    }

    public static Task BulkMergeAsync<T>(this DbContext dbContext, IEnumerable<T> entities, int? timeoutOverride = 600, bool useContextConnection = true) where T : class
    {
        var @operator = new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
        return @operator.MergeAsync(entities);
    }

    public static Task BulkInsertAsync<T>(this DbContext dbContext, IEnumerable<T> entities, int? timeoutOverride = 600, bool useContextConnection = true) where T : class
    {
        var @operator = new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
        return @operator.InsertAsync(entities);
    }

    public static BulkEfOperator GetBulkOperator(this DbContext dbContext, int? timeoutOverride = 600, bool useContextConnection = true)
    {
        return new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
    }
}