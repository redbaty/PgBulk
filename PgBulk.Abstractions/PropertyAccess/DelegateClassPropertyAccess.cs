﻿using System.Reflection;

namespace PgBulk.Abstractions.PropertyAccess;

/// <summary>
///     Inspired by http://msmvps.com/blogs/jon_skeet/archive/2008/08/09/making-reflection-fly-and-exploring-delegates.aspx
/// </summary>
/// <typeparam name="TTarget"></typeparam>
/// <typeparam name="TProperty"></typeparam>
public class DelegateClassPropertyAccess<TTarget, TProperty> : IClassPropertyAccess where TTarget : class
{
    private readonly PropertyValueGetter _getter;
    private readonly PropertyValueSetter _setter;

    public DelegateClassPropertyAccess(PropertyInfo propertyInfo)
    {
        _getter = CreateGetter(propertyInfo);
        _setter = CreateSetter(propertyInfo);
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
                return _ => staticGetter.Invoke();
            }

            return (PropertyValueGetter)Delegate.CreateDelegate(typeof(PropertyValueGetter), getMethod);
        }

        return _ => throw new NotImplementedException("No getter implemented for property " + name);
    }

    private static PropertyValueSetter CreateSetter(PropertyInfo propertyInfo)
    {
        var name = propertyInfo.Name;
        if (propertyInfo.CanWrite && propertyInfo.GetIndexParameters().Length == 0)
        {
            var setMethod = propertyInfo.GetSetMethod();
            if (setMethod!.IsStatic)
            {
                var staticSetter = (StaticPropertyValueSetter)Delegate.CreateDelegate(typeof(StaticPropertyValueSetter), setMethod);
                return (target, value) => staticSetter.Invoke(value);
            }

            return (PropertyValueSetter)Delegate.CreateDelegate(typeof(PropertyValueSetter), setMethod);
        }

        return (_, _) => throw new NotImplementedException("No setter implemented for property " + name);
    }

    public TProperty? GetValue(TTarget target)
    {
        return _getter.Invoke(target);
    }

    public void SetValue(TTarget target, TProperty value)
    {
        _setter.Invoke(target, value);
    }

    public void SetValue(object target, object value)
    {
        SetValue((TTarget)target, (TProperty)value);
    }

    private delegate TProperty? PropertyValueGetter(TTarget target);

    private delegate TProperty StaticPropertyValueGetter();

    private delegate void PropertyValueSetter(TTarget target, TProperty value);

    private delegate void StaticPropertyValueSetter(TProperty value);
}