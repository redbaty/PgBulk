using System.Text;
using Npgsql;
using PgBulk.Abstractions;

namespace PgBulk;

public sealed class ManualBulkOperator : BulkOperator
{
    public ManualBulkOperator(string? connectionString, ManualTableInformationProvider tableInformationProvider) : base(connectionString, tableInformationProvider)
    {
    }

    public async Task VerifyPrimaryKeys()
    {
        if (TableInformationProvider is not ManualTableInformationProvider manualTableInformationProvider)
        {
            throw new InvalidOperationException("Table information provider is not of manual type");
        }

        foreach (var manualTableInformation in manualTableInformationProvider.TableColumnInformations.Values)
        {
            await using var connection = await CreateOpenedConnection();
            var command = connection.CreateCommand();
            var scriptBuilder = new StringBuilder();
            scriptBuilder.AppendLine("SELECT a.attname");
            scriptBuilder.AppendLine("FROM pg_index i");
            scriptBuilder.AppendLine("JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)");
            scriptBuilder.AppendLine($"WHERE  i.indrelid = '\"{manualTableInformation.Name}\"'::regclass");
            command.CommandText = scriptBuilder.ToString();
            var reader = await command.ExecuteReaderAsync();
            var primaryKeys = new HashSet<string>();

            while (await reader.ReadAsync())
            {
                primaryKeys.Add(reader.GetString(0));
            }

            foreach (var tableColumnInformation in manualTableInformation.Columns.OfType<ManualTableColumnMapping>())
            {
                tableColumnInformation.PrimaryKey = primaryKeys.Contains(tableColumnInformation.Name);
            }
        }
    }
}

public class BulkOperator
{
    protected BulkOperator(ITableInformationProvider tableInformationProvider)
    {
        TableInformationProvider = tableInformationProvider;
    }

    public BulkOperator(string? connectionString, ITableInformationProvider tableInformationProvider)
    {
        ConnectionString = connectionString;
        TableInformationProvider = tableInformationProvider;
    }

    protected ITableInformationProvider TableInformationProvider { get; }

    private string? ConnectionString { get; }

    public virtual async Task<NpgsqlConnection> CreateOpenedConnection()
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    public async Task MergeAsync<T>(IEnumerable<T> entities)
    {
        await MergeAsync(await CreateOpenedConnection(), entities);
    }

    public virtual async Task MergeAsync<T>(NpgsqlConnection connection, IEnumerable<T> entities)
    {
        var tableInformation = await TableInformationProvider.GetTableInformation(typeof(T));
        var temporaryName = GetTemporaryTableName(tableInformation);

        await InsertToTemporaryTableAsync(connection, entities, tableInformation, temporaryName);

        var primaryKeyColumns = tableInformation.Columns.Where(i => i.PrimaryKey).Select(i => $"\"{i.Name}\"")
            .Aggregate((x, y) => $"{x},{y}");

        var setStatement = tableInformation.Columns.Where(i => !i.PrimaryKey)
            .Select(i => $"\"{i.Name}\" = EXCLUDED.\"{i.Name}\"")
            .Aggregate((x, y) => $"{x}, {y}");

        await using var npgsqlCommand = connection.CreateCommand();
        npgsqlCommand.CommandText = $"insert into \"{tableInformation.Name}\" (select * from \"{temporaryName}\") ON CONFLICT ({primaryKeyColumns}) DO UPDATE SET {setStatement}";
        await npgsqlCommand.ExecuteNonQueryAsync();
    }

    public async Task SyncAsync<T>(IEnumerable<T> entities)
    {
        await SyncAsync(await CreateOpenedConnection(), entities);
    }

    public virtual async Task SyncAsync<T>(NpgsqlConnection connection, IEnumerable<T> entities)
    {
        var tableInformation = await TableInformationProvider.GetTableInformation(typeof(T));
        var temporaryName = GetTemporaryTableName(tableInformation);

        await InsertToTemporaryTableAsync(connection, entities, tableInformation, temporaryName);

        await using var transação = await connection.BeginTransactionAsync();
        await using var deleteCommand = connection.CreateCommand();
        deleteCommand.CommandText = $"delete from \"{tableInformation.Name}\"";
        await deleteCommand.ExecuteNonQueryAsync();

        await using var npgsqlCommand = connection.CreateCommand();
        npgsqlCommand.CommandText = $"insert into \"{tableInformation.Name}\" (select * from \"{temporaryName}\")";
        await npgsqlCommand.ExecuteNonQueryAsync();

        await transação.CommitAsync(CancellationToken.None);
    }

    public virtual string GetTemporaryTableName(ITableInformation tableColumnInformation)
    {
        var newGuid = Guid.NewGuid();
        var s = newGuid.ToString().Replace("-", string.Empty)[..10];
        return $"{tableColumnInformation.Columns}_temp_{s}";
    }

    protected static async Task<ulong> InsertToTemporaryTableAsync<T>(NpgsqlConnection connection, IEnumerable<T> entities, ITableInformation tableInformation, string temporaryTableName)
    {
        await using var npgsqlCommand = connection.CreateCommand();
        npgsqlCommand.CommandText = $"CREATE TABLE \"{temporaryTableName}\" AS TABLE \"{tableInformation.Name}\" WITH NO DATA;";
        await npgsqlCommand.ExecuteNonQueryAsync();

        var columns = tableInformation.Columns
            .Select(i => $"\"{i.Name}\"")
            .Aggregate((x, y) => $"{x}, {y}");
#if NET5_0
        await using var npgsqlBinaryImporter = connection.BeginBinaryImport($"COPY \"{temporaryTableName}\" ({columns}) FROM STDIN (FORMAT BINARY)");
#else
        await using var npgsqlBinaryImporter =
            await connection.BeginBinaryImportAsync($"COPY \"{temporaryTableName}\" ({columns}) FROM STDIN (FORMAT BINARY)");
#endif

        ulong inserted = 0;

        foreach (var entity in entities)
        {
            var valores = tableInformation.Columns
                .Select(i => i.GetValue(entity))
                .ToArray();
            await npgsqlBinaryImporter.WriteRowAsync(CancellationToken.None, valores!);

            inserted++;
        }

        await npgsqlBinaryImporter.CompleteAsync();
        await npgsqlBinaryImporter.DisposeAsync();
        return inserted;
    }
}