using Microsoft.EntityFrameworkCore;

namespace PgBulk.EFCore;

public static class ContextExtensions
{
    public static Task BulkSyncAsync<T>(this DbContext dbContext, IEnumerable<T> entities) where T : class
    {
        var @operator = new BulkEfOperator(dbContext);
        return @operator.SyncAsync(entities);
    }

    public static Task BulkMergeAsync<T>(this DbContext dbContext, IEnumerable<T> entities) where T : class
    {
        var @operator = new BulkEfOperator(dbContext);
        return @operator.MergeAsync(entities);
    }
}