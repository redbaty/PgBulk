using System.Linq.Expressions;
using PgBulk.Abstractions;

namespace PgBulk;

public class ManualTableColumnInformationBuilder<T>
{
    public ManualTableColumnInformationBuilder(string tableName, string schema = "public")
    {
        TableName = tableName;
        Schema = schema;
    }

    private string TableName { get; }
    
    private string Schema { get; }

    private HashSet<ManualTableColumnMapping> ColumnMappings { get; } = new();

    public ManualTableColumnInformationBuilder<T> Automap()
    {
        foreach (var propertyInfo in typeof(T).GetProperties().Where(i => i is { CanRead: true, CanWrite: true }))
        {
            var previousMax = ColumnMappings.Count < 1 ? 0 : ColumnMappings.Max(x => x.Index);
            ColumnMappings.Add(new ManualTableColumnMapping(propertyInfo.Name, propertyInfo, false, previousMax + 1));
        }

        return this;
    }

    public ManualTableColumnInformationBuilder<T> Property<TObj>(Expression<Func<T, TObj>> propertyLambda, string columnName, bool primaryKey = false)
    {
        var propertyInfo = propertyLambda.GetProperty();
        var previousMax = ColumnMappings.Count < 1 ? 0 : ColumnMappings.Max(x => x.Index);
        var columnMapping = new ManualTableColumnMapping(columnName, propertyInfo, primaryKey, previousMax + 1);
        ColumnMappings.Add(columnMapping);

        return this;
    }

    internal void AddToProvider(ManualTableInformationProvider provider)
    {
        provider.TableColumnInformations.Add(typeof(T), new ManualTableInformation(Schema, TableName, ColumnMappings.Cast<ITableColumnInformation>().ToList()));
    }
}