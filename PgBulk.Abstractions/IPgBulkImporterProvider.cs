using System.Reflection;

namespace PgBulk.Abstractions
{
    public interface IPgBulkImporterProvider<in T>
    {
        ValueTask<ICollection<ITableColumnInformation>> FilterColumns(IEnumerable<ITableColumnInformation> columns)
        {
            return DefaultFilter(columns, GetPropertyOrder());
        }

        public static ValueTask<ICollection<ITableColumnInformation>> DefaultFilter(IEnumerable<ITableColumnInformation> columns, IEnumerable<PropertyInfo> propertyOrder)
        {
            var properties = propertyOrder.ToList();
            
            var columnsFiltered = columns.Where(i => !i.ValueGeneratedOnAdd);

            if (properties.Count != 0)
                columnsFiltered = columnsFiltered.Where(i => properties.Contains(i.Property))
                    .OrderBy(i => properties.IndexOf(i.Property));

            return ValueTask.FromResult((ICollection<ITableColumnInformation>)columnsFiltered.ToList());
        }

        IEnumerable<object?> GetValues(T entity);
        
        IEnumerable<PropertyInfo> GetPropertyOrder();
    }
}