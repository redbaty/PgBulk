using PgBulk.Abstractions;

namespace PgBulk;

public class ManualTableInformation : ITableInformation
{
    public ManualTableInformation(string name, ICollection<ITableColumnInformation> columns)
    {
        Name = name;
        Columns = columns;
    }

    public string Name { get; }

    public ICollection<ITableColumnInformation> Columns { get; }
}