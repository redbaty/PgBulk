namespace PgBulk.Abstractions;

public interface ITableColumnInformation
{
    string Name { get; }

    bool PrimaryKey { get; }
    
    bool ValueGeneratedOnAdd { get; }

    object? GetValue(object? entity);
}