using PgBulk.Abstractions;

namespace PgBulk.EFCore;

public class EntityTableInformation : ITableInformation
{
    public EntityTableInformation(string schema, string name, IEnumerable<ITableColumnInformation> columns)
    {
        Name = name;
        Schema = schema;
        Columns = columns.ToArray();
    }

    public string Name { get; }

    public string Schema { get; }

    public ICollection<ITableColumnInformation> Columns { get; }
}