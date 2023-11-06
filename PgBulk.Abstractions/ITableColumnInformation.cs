namespace PgBulk.Abstractions;

public interface ITableColumnInformation
{
    int Index { get; }
    
    string Name { get; }

    string SafeName => Name.StartsWith('"') && Name.EndsWith('"') ? Name : $"\"{Name}\"";
    
    bool PrimaryKey { get; }

    bool ValueGeneratedOnAdd { get; }

    object? GetValue(object? entity);
}