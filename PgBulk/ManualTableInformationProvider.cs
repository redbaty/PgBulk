using PgBulk.Abstractions;

namespace PgBulk;

public class ManualTableInformationProvider : ITableInformationProvider
{
    internal Dictionary<Type, ManualTableInformation> TableColumnInformations { get; } = new();

    public Task<ITableInformation> GetTableInformation(Type entityType)
    {
        if (TableColumnInformations.ContainsKey(entityType))
            return Task.FromResult((ITableInformation)TableColumnInformations[entityType]);

        throw new NotImplementedException();
    }

    public ManualTableInformationProvider? AddTableMapping<T>(string tableName, Action<ManualTableColumnInformationBuilder<T>> configuration)
    {
        var newMapping = new ManualTableColumnInformationBuilder<T>(tableName);
        configuration.Invoke(newMapping);

        newMapping.AddToProvider(this);
        return this;
    }
}