using System.Linq.Expressions;
using PgBulk.Abstractions;

namespace PgBulk;

public class ManualTableColumnInformationBuilder<T>
{
    public ManualTableColumnInformationBuilder(string tableName)
    {
        TableName = tableName;
    }

    private string TableName { get; }

    private HashSet<ManualTableColumnMapping> ColumnMappings { get; } = new();

    public ManualTableColumnInformationBuilder<T> Automap()
    {
        foreach (var propertyInfo in typeof(T).GetProperties().Where(i => i.CanRead && i.CanWrite))
        {
            ColumnMappings.Add(new ManualTableColumnMapping(propertyInfo.Name, propertyInfo));
        }

        return this;
    }

    public ManualTableColumnInformationBuilder<T> Property<TObj>(Expression<Func<T, TObj>> propertyLambda, string columnName, bool primaryKey = false)
    {
        var propertyInfo = propertyLambda.GetProperty();
        var columnMapping = new ManualTableColumnMapping(columnName, propertyInfo, primaryKey);
        ColumnMappings.Add(columnMapping);

        return this;
    }

    internal void AddToProvider(ManualTableInformationProvider provider)
    {
        provider.TableColumnInformations.Add(typeof(T), new ManualTableInformation(TableName, ColumnMappings.Cast<ITableColumnInformation>().ToArray()));
    }
}