using System.Reflection;
using PgBulk.Abstractions;
using PgBulk.Abstractions.PropertyAccess;

namespace PgBulk;

public record ManualTableColumnMapping : ITableColumnInformation
{
    private readonly PropertyInfo? _property;

    private readonly IPropertyReadAccess? _propertyReadAccess;

    private readonly Type? _truePropertyType;

    public ManualTableColumnMapping(string name, PropertyInfo? property, bool valueGeneratedOnAdd, int index, bool primaryKey = false)
    {
        Name = name;
        ValueGeneratedOnAdd = valueGeneratedOnAdd;
        Index = index;
        PrimaryKey = primaryKey;
        _property = property;
        _propertyReadAccess = property == null ? null : PropertyAccessFactory.CreateRead(property);
        _truePropertyType = property == null ? null : Nullable.GetUnderlyingType(property.PropertyType) ?? property?.PropertyType;
    }

    public int Index { get; }

    public string Name { get; }

    public bool PrimaryKey { get; internal set; }

    public bool ValueGeneratedOnAdd { get; }

    public object? GetValue(object entity)
    {
        if (_property == null)
            throw new InvalidOperationException("No property is set for this column");

        var value = _propertyReadAccess!.GetValue(entity);

        return _truePropertyType!.IsEnum && value != null
            ? Convert.ToInt32(value)
            : value;
    }
}