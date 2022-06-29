namespace PgBulk.Abstractions;

public interface ITableColumnInformation
{
    string Name { get; }

    bool PrimaryKey { get; }

    object? GetValue(object? entity);
}