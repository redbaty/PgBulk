using System.Linq.Expressions;
using System.Reflection;

namespace PgBulk;

internal static class ReflectionExtensions
{
    public static PropertyInfo GetProperty<TSource, TObj>(
        this Expression<Func<TSource, TObj>> propertyLambda)
    {
        var type = typeof(TSource);

        if (!(propertyLambda.Body is MemberExpression member))
            throw new ArgumentException($"Expression '{propertyLambda}' refers to a method, not a property.");

        var propInfo = member.Member as PropertyInfo;
        if (propInfo == null)
            throw new ArgumentException($"Expression '{propertyLambda}' refers to a field, not a property.");

        if (propInfo.ReflectedType != null && type != propInfo.ReflectedType &&
            !type.IsSubclassOf(propInfo.ReflectedType))
            throw new ArgumentException(
                $"Expression '{propertyLambda}' refers to a property that is not from type {type}.");

        return propInfo;
    }
}