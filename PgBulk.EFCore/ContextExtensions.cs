using Microsoft.EntityFrameworkCore;
using PgBulk.Abstractions;

namespace PgBulk.EFCore;

public static class ContextExtensions
{
    public static Task BulkSyncAsync<T>(this DbContext dbContext, IEnumerable<T> entities, string? deleteWhere = null, int? timeoutOverride = 600, bool useContextConnection = true) where T : class
    {
        var @operator = new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
        return @operator.SyncAsync(entities, deleteWhere);
    }

    public static Task BulkMergeAsync<T>(this DbContext dbContext, IEnumerable<T> entities, int? timeoutOverride = 600, bool useContextConnection = true, ITableKeyProvider? tableKeyProvider = null) where T : class
    {
        var @operator = new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
        return @operator.MergeAsync(entities.ToList(), tableKeyProvider);
    }

    public static Task BulkMergeAsync<T>(this DbContext dbContext, ICollection<T> entities, int? timeoutOverride = 600, bool useContextConnection = true, ITableKeyProvider? tableKeyProvider = null) where T : class
    {
        var @operator = new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
        return @operator.MergeAsync(entities, tableKeyProvider);
    }

    public static Task BulkInsertAsync<T>(this DbContext dbContext, IEnumerable<T> entities, int? timeoutOverride = 600, bool useContextConnection = true, bool onConflictIgnore = false) where T : class
    {
        var @operator = new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
        return @operator.InsertAsync(entities, onConflictIgnore);
    }

    public static BulkEfOperator GetBulkOperator(this DbContext dbContext, int? timeoutOverride = 600, bool useContextConnection = true)
    {
        return new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
    }
}