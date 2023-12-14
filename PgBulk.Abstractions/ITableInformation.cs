namespace PgBulk.Abstractions;

public interface ITableInformation
{
    string Name { get; }

    string Schema { get; }

    ICollection<ITableColumnInformation> Columns { get; }
}