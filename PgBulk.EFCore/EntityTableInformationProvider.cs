using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using PgBulk.Abstractions;

namespace PgBulk.EFCore;

public class EntityTableInformationProvider : ITableInformationProvider
{
    public EntityTableInformationProvider(DbContext dbContext)
    {
        DbContext = dbContext;
    }

    private DbContext DbContext { get; }

    public Task<ITableInformation> GetTableInformation(Type entityType)
    {
        var model = DbContext.Model.FindEntityType(entityType);

        if (model == null) throw new InvalidOperationException($"Failed to find model for type {entityType.Name}");

        var tableName = model.GetTableName();

        if (tableName == null) throw new InvalidOperationException($"Failed to find table name for type {entityType.Name}");

#if NET5_0
        var storeObjectIdentifier = StoreObjectIdentifier.Table(tableName, null);
#else
        var storeObjectIdentifier = StoreObjectIdentifier.Table(tableName);
#endif
        var columns = model
            .GetProperties()
            .Select(p => new EntityColumnInformation(p.GetColumnName(storeObjectIdentifier) ?? p.Name, p.IsPrimaryKey(), p.PropertyInfo));
        var entityTableInformation = new EntityTableInformation(tableName, columns);

        return Task.FromResult((ITableInformation)entityTableInformation);
    }
}