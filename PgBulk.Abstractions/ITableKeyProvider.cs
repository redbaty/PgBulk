namespace PgBulk.Abstractions;

public interface ITableKeyProvider
{
    TableKey GetKeyColumns(ITableInformation tableInformation);
}