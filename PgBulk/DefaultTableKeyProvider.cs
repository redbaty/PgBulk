using PgBulk.Abstractions;

namespace PgBulk;

public class DefaultTableKeyProvider : ITableKeyProvider
{
    public TableKey GetKeyColumns(ITableInformation tableInformation)
    {
        return new TableKey(tableInformation.Columns
            .Where(i => i is { PrimaryKey: true, ValueGeneratedOnAdd: false })
            .ToArray(), true);
    }
}