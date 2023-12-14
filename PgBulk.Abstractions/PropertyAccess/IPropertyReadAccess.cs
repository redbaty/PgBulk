namespace PgBulk.Abstractions.PropertyAccess
{
    public interface IPropertyReadAccess
    {
        object? GetValue(object target);
    }
}