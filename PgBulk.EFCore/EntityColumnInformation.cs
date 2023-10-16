using System.Reflection;
using PgBulk.Abstractions;

namespace PgBulk.EFCore;

public class EntityColumnInformation : ITableColumnInformation
{
    public EntityColumnInformation(string name, bool primaryKey, bool valueGeneratedOnAdd, PropertyInfo? property)
    {
        Name = name;
        PrimaryKey = primaryKey;
        Property = property;
        ValueGeneratedOnAdd = valueGeneratedOnAdd;
    }

    private PropertyInfo? Property { get; }
    
    public string Name { get; }

    public bool PrimaryKey { get; }

    public bool ValueGeneratedOnAdd { get; }

    public object? GetValue(object? entity)
    {
        if (Property == null)
            throw new InvalidOperationException("No property is set for this column");

        var truePropertyType = Nullable.GetUnderlyingType(Property.PropertyType) ?? Property.PropertyType;
        var value = Property.GetValue(entity);
        
        return truePropertyType.IsEnum && value != null
            ? Convert.ToInt32(value)
            : value;
    }
}