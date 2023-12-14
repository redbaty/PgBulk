using System.Reflection;

namespace PgBulk.EFCore;

public record EntityColumnInformation : ManualTableColumnMapping
{
    public EntityColumnInformation(string name, bool primaryKey, bool valueGeneratedOnAdd, PropertyInfo? property, int index) : base(name, property, valueGeneratedOnAdd, index, primaryKey)
    {
        Property = property;
    }
    
    public PropertyInfo? Property { get; }
}