using PgBulk.Abstractions;

namespace PgBulk;

public class ManualTableInformation : ITableInformation
{
    public ManualTableInformation(string schema, string name, ICollection<ITableColumnInformation> columns)
    {
        Name = name;
        Columns = columns;
        Schema = schema;
    }

    public string Name { get; }
    
    public string Schema { get; }

    public ICollection<ITableColumnInformation> Columns { get; }
}