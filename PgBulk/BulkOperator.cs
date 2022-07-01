using System.Diagnostics;
using System.Text;
using Npgsql;
using PgBulk.Abstractions;

namespace PgBulk;

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

    public virtual void LogBeforeCommand(NpgsqlCommand npgsqlCommand)
    {
    }

    public virtual void LogAfterCommand(NpgsqlCommand npgsqlCommand, TimeSpan elapsed)
    {
    }

    public virtual async Task<NpgsqlConnection> CreateOpenedConnection()
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    public async Task MergeAsync<T>(IEnumerable<T> entities)
    {
        await using var connection = await CreateOpenedConnection();
        await MergeAsync(connection, entities);
    }

    public async Task InsertAsync<T>(IEnumerable<T> entities)
    {
        await using var connection = await CreateOpenedConnection();
        await InsertToTableAsync(connection, entities);
    }

    private async Task<ulong> InsertToTableAsync<T>(NpgsqlConnection npgsqlConnection, IEnumerable<T> entities)
    {
        var tableInformation = await TableInformationProvider.GetTableInformation(typeof(T));
        return await InsertToTableAsync(npgsqlConnection, entities, tableInformation, tableInformation.Name);
    }

    public virtual async Task MergeAsync<T>(NpgsqlConnection connection, IEnumerable<T> entities)
    {
        var tableInformation = await TableInformationProvider.GetTableInformation(typeof(T));
        var temporaryName = GetTemporaryTableName(tableInformation);

        await ExecuteCommand(connection, $"CREATE TEMPORARY TABLE \"{temporaryName}\" AS TABLE \"{tableInformation.Name}\" WITH NO DATA;");
        await InsertToTableAsync(connection, entities, tableInformation, temporaryName);

        var primaryKeyColumns = tableInformation.Columns
            .Where(i => i.PrimaryKey)
            .Select(i => $"\"{i.Name}\"")
            .DefaultIfEmpty()
            .Aggregate((x, y) => $"{x},{y}");

        if (string.IsNullOrEmpty(primaryKeyColumns))
            throw new InvalidOperationException($"No primary keys defined for table \"{tableInformation.Name}\"");

        var setStatement = tableInformation.Columns
            .Where(i => !i.PrimaryKey)
            .Select(i => $"\"{i.Name}\" = EXCLUDED.\"{i.Name}\"")
            .DefaultIfEmpty()
            .Aggregate((x, y) => $"{x}, {y}");


        if (!string.IsNullOrEmpty(setStatement))
            await ExecuteCommand(connection, $"insert into \"{tableInformation.Name}\" (select * from \"{temporaryName}\") ON CONFLICT ({primaryKeyColumns}) DO UPDATE SET {setStatement}");
        else
            await ExecuteCommand(connection, $"insert into \"{tableInformation.Name}\" (select * from \"{temporaryName}\") ON CONFLICT DO NOTHING");
    }

    private async Task ExecuteCommand(NpgsqlConnection connection, string script)
    {
        await using var npgsqlCommand = connection.CreateCommand();
        npgsqlCommand.CommandText = script;

        LogBeforeCommand(npgsqlCommand);
        var stopWatch = Stopwatch.StartNew();
        await npgsqlCommand.ExecuteNonQueryAsync();

        stopWatch.Start();
        LogAfterCommand(npgsqlCommand, stopWatch.Elapsed);
    }

    public async Task SyncAsync<T>(IEnumerable<T> entities, string? deleteWhere = null)
    {
        await using var connection = await CreateOpenedConnection();
        await SyncAsync(connection, entities, deleteWhere);
    }

    public virtual async Task SyncAsync<T>(NpgsqlConnection connection, IEnumerable<T> entities, string? deleteWhere)
    {
        var tableInformation = await TableInformationProvider.GetTableInformation(typeof(T));
        var temporaryName = GetTemporaryTableName(tableInformation);

        await ExecuteCommand(connection, $"CREATE TEMPORARY TABLE \"{temporaryName}\" AS TABLE \"{tableInformation.Name}\" WITH NO DATA;");
        await InsertToTableAsync(connection, entities, tableInformation, temporaryName);

        await using var transaction = await connection.BeginTransactionAsync();

        var deleteScriptBuilder = new StringBuilder($"delete from \"{tableInformation.Name}\"");

        if (!string.IsNullOrEmpty(deleteWhere))
        {
            deleteScriptBuilder.AppendLine("WHERE");
            deleteScriptBuilder.AppendLine(deleteWhere);
        }

        await ExecuteCommand(connection, deleteScriptBuilder.ToString());
        await ExecuteCommand(connection, $"insert into \"{tableInformation.Name}\" (select * from \"{temporaryName}\")");
        await transaction.CommitAsync(CancellationToken.None);
    }

    public virtual string GetTemporaryTableName(ITableInformation tableColumnInformation)
    {
        return $"{tableColumnInformation.Name}_temp_{Nanoid.Nanoid.Generate(size: 10)}";
    }

    private async Task<ulong> InsertToTableAsync<T>(NpgsqlConnection connection, IEnumerable<T> entities, ITableInformation tableInformation, string tableName)
    {
        var columns = tableInformation.Columns
            .Select(i => $"\"{i.Name}\"")
            .Aggregate((x, y) => $"{x}, {y}");
#if NET5_0
        await using var npgsqlBinaryImporter = connection.BeginBinaryImport($"COPY \"{tableName}\" ({columns}) FROM STDIN (FORMAT BINARY)");
#else
        await using var npgsqlBinaryImporter = await connection.BeginBinaryImportAsync($"COPY \"{tableName}\" ({columns}) FROM STDIN (FORMAT BINARY)");
#endif

        ulong inserted = 0;

        foreach (var entity in entities)
        {
            await npgsqlBinaryImporter.StartRowAsync();

            foreach (var columnValue in tableInformation.Columns.Select(i => i.GetValue(entity))) await npgsqlBinaryImporter.WriteAsync(columnValue);

            inserted++;
        }

        await npgsqlBinaryImporter.CompleteAsync();
        await npgsqlBinaryImporter.DisposeAsync();
        return inserted;
    }
}