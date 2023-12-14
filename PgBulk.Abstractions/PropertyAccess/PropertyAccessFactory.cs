using System.Reflection;

namespace PgBulk.Abstractions.PropertyAccess;

public class PropertyAccessFactory
{
    public static IPropertyReadAccess? CreateRead(PropertyInfo propertyInfo)
    {
        return propertyInfo.DeclaringType!.IsValueType ? CreateForValue(propertyInfo) : CreateForClass(propertyInfo);
    }

    public static IClassPropertyAccess? CreateForClass(PropertyInfo propertyInfo)
    {
        var targetType = propertyInfo.DeclaringType;

        if (targetType is { IsValueType: false })
        {
            var type = typeof(DelegateClassPropertyAccess<,>).MakeGenericType(targetType, propertyInfo.PropertyType);
            return (IClassPropertyAccess?)Activator.CreateInstance(type, propertyInfo);
        }

        return null;
    }

    public static IValuePropertyAccess? CreateForValue(PropertyInfo propertyInfo)
    {
        var targetType = propertyInfo.DeclaringType;

        if (targetType is { IsValueType: true })
        {
            // Value types have to be handled differently, because the corresponding delegate types
            // have to provide their first argument ('this') as a ref.
            // see: http://stackoverflow.com/questions/4326736/how-can-i-create-an-open-delegate-from-a-structs-instance-method
            // see: http://stackoverflow.com/questions/1212346/uncurrying-an-instance-method-in-net/1212396#1212396

            var type = typeof(DelegateValuePropertyAccess<,>).MakeGenericType(targetType, propertyInfo.PropertyType);
            return (IValuePropertyAccess?)Activator.CreateInstance(type, propertyInfo);
        }

        return null;
    }
}