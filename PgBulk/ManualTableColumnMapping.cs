using System.Reflection;
using PgBulk.Abstractions;

namespace PgBulk;

public record ManualTableColumnMapping : ITableColumnInformation
{
    public ManualTableColumnMapping(string name, PropertyInfo? property, bool valueGeneratedOnAdd, int index, bool primaryKey = false)
    {
        Name = name;
        Property = property;
        ValueGeneratedOnAdd = valueGeneratedOnAdd;
        Index = index;
        PrimaryKey = primaryKey;
    }

    internal PropertyInfo? Property { get; }
    
    public int Index { get; }
    
    public string Name { get; }

    public bool PrimaryKey { get; internal set; }

    public bool ValueGeneratedOnAdd { get; }

    public object? GetValue(object? entity)
    {
        if (Property == null)
            throw new InvalidOperationException("No property is set for this column");

        return Property.PropertyType.IsEnum
            ? Convert.ToInt32(Property.GetValue(entity))
            : Property.GetValue(entity);
    }
}