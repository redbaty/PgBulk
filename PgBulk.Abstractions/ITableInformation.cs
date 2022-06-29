namespace PgBulk.Abstractions;

public interface ITableInformation
{
    string Name { get; }

    ICollection<ITableColumnInformation> Columns { get; }
}