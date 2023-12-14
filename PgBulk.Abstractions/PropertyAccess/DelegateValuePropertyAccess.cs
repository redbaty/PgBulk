using System.Reflection;

namespace PgBulk.Abstractions.PropertyAccess;

public class DelegateValuePropertyAccess<TTarget, TProperty> : IValuePropertyAccess where TTarget : struct
{
    private readonly PropertyValueGetter _getter;

    public DelegateValuePropertyAccess(PropertyInfo propertyInfo)
    {
        _getter = CreateGetter(propertyInfo);
    }

    public object? GetValue(object target)
    {
        return GetValue((TTarget)target);
    }

    private static PropertyValueGetter CreateGetter(PropertyInfo propertyInfo)
    {
        var name = propertyInfo.Name;

        if (propertyInfo.CanRead && propertyInfo.GetIndexParameters().Length == 0)
        {
            var getMethod = propertyInfo.GetGetMethod();
            if (getMethod!.IsStatic)
            {
                var staticGetter = (StaticPropertyValueGetter)Delegate.CreateDelegate(typeof(StaticPropertyValueGetter), getMethod);
                return new StaticGetWrapper(staticGetter).Get;
            }

            return (PropertyValueGetter)Delegate.CreateDelegate(typeof(PropertyValueGetter), getMethod);
        }

        return new StaticGetError("No getter implemented for property " + name).Get;
    }


    public TProperty? GetValue(TTarget target)
    {
        return _getter.Invoke(ref target);
    }

    private delegate TProperty? PropertyValueGetter(ref TTarget target);

    private delegate TProperty StaticPropertyValueGetter();

    private class StaticGetWrapper
    {
        private readonly StaticPropertyValueGetter _getter;

        public StaticGetWrapper(StaticPropertyValueGetter getter)
        {
            _getter = getter;
        }

        public TProperty Get(ref TTarget target)
        {
            return _getter.Invoke();
        }
    }

    private class StaticGetError
    {
        private readonly string _error;

        public StaticGetError(string error)
        {
            _error = error;
        }

        public TProperty Get(ref TTarget target)
        {
            throw new NotImplementedException(_error);
        }
    }
}