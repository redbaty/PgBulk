using PgBulk.Abstractions;

namespace PgBulk;

public class ManualTableInformationProvider : ITableInformationProvider
{
    internal Dictionary<Type, ManualTableInformation> TableColumnInformations { get; } = new();

    public Task<ITableInformation> GetTableInformation(Type entityType)
    {
        if (TableColumnInformations.TryGetValue(entityType, out var information))
            return Task.FromResult((ITableInformation)information);

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