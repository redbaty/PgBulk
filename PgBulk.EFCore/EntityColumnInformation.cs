using System.Reflection;
using PgBulk.Abstractions;

namespace PgBulk.EFCore;

public class EntityColumnInformation : ITableColumnInformation
{
    public EntityColumnInformation(string name, bool primaryKey, PropertyInfo? property)
    {
        Name = name;
        PrimaryKey = primaryKey;
        Property = property;
    }

    private PropertyInfo? Property { get; }
    public string Name { get; }

    public bool PrimaryKey { get; }

    public object? GetValue(object? entity)
    {
        if (Property == null)
            throw new InvalidOperationException("No property is set for this column");

        return Property.PropertyType.IsEnum
            ? Convert.ToInt32(Property.GetValue(entity))
            : Property.GetValue(entity);
    }
}