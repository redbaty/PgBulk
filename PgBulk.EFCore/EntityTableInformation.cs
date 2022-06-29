using PgBulk.Abstractions;

namespace PgBulk.EFCore;

public class EntityTableInformation : ITableInformation
{
    public EntityTableInformation(string name, IEnumerable<ITableColumnInformation> columns)
    {
        Name = name;
        Columns = columns.ToArray();
    }

    public string Name { get; }

    public ICollection<ITableColumnInformation> Columns { get; }
}