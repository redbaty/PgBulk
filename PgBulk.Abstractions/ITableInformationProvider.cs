namespace PgBulk.Abstractions;

public interface ITableInformationProvider
{
    Task<ITableInformation> GetTableInformation(Type entityType);
}