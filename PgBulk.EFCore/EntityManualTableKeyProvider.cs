using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using PgBulk.Abstractions;

namespace PgBulk.EFCore;

public class EntityManualTableKeyProvider<TEntity> : ITableKeyProvider
{
    private readonly ICollection<ITableColumnInformation> _primaryKeyColumns;

    public EntityManualTableKeyProvider(ICollection<ITableColumnInformation> primaryKeyColumns)
    {
        _primaryKeyColumns = primaryKeyColumns;
    }

    public EntityManualTableKeyProvider()
    {
        _primaryKeyColumns = new List<ITableColumnInformation>();
    }

    public async ValueTask AddKeyColumn<TObj>(Expression<Func<TEntity, TObj>> propertyLambda, DbContext dbContext)
    {
        var entityTableInformationProvider = new EntityTableInformationProvider(dbContext);
        var tableInformation = (EntityTableInformation)await entityTableInformationProvider.GetTableInformation(typeof(TEntity));
        AddKeyColumn(propertyLambda, tableInformation);
    }

    public void AddKeyColumn<TObj>(Expression<Func<TEntity, TObj>> propertyLambda, EntityTableInformation tableInformation)
    {
        var property = propertyLambda.GetPropertyAccess();
        var entityColumnInformation = tableInformation.Columns
            .OfType<EntityColumnInformation>()
            .SingleOrDefault(i => i.Property == property);

        if (entityColumnInformation != null)
            _primaryKeyColumns.Add(entityColumnInformation);
        else
            throw new InvalidOperationException($"Could not find column information for property {property.Name}");
    }

    public TableKey GetKeyColumns(ITableInformation tableInformation)
    {
        return new TableKey(_primaryKeyColumns, false);
    }
}