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

    protected bool DisposeConnection { get; set; } = true;

    private string? ConnectionString { get; }

    public virtual void LogBeforeCommand(NpgsqlCommand npgsqlCommand)
    {
    }

    public virtual void LogAfterCommand(NpgsqlCommand npgsqlCommand, TimeSpan elapsed)
    {
    }

    protected virtual async Task<NpgsqlConnection> CreateOpenedConnection()
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    public async Task MergeAsync<T>(IEnumerable<T> entities)
    {
        var connection = await CreateOpenedConnection();

        try
        {
            await MergeAsync(connection, entities);
        }
        finally
        {
            if (DisposeConnection)
                await connection.DisposeAsync();
        }
    }

    public async Task InsertAsync<T>(IEnumerable<T> entities)
    {
        var connection = await CreateOpenedConnection();

        try
        {
            await InsertToTableAsync(connection, entities);
        }
        finally
        {
            if (DisposeConnection)
                await connection.DisposeAsync();
        }
    }

    private async Task<ulong> InsertToTableAsync<T>(NpgsqlConnection npgsqlConnection, IEnumerable<T> entities)
    {
        var tableInformation = await TableInformationProvider.GetTableInformation(typeof(T));
        return await InsertToTableAsync(npgsqlConnection, entities, tableInformation, tableInformation.Name);
    }

    public virtual async Task MergeAsync<T>(NpgsqlConnection connection, IEnumerable<T> entities, Func<string, string, Task>? runAfterTemporaryTableInsert = null)
    {
        var tableInformation = await TableInformationProvider.GetTableInformation(typeof(T));
        var temporaryName = GetTemporaryTableName(tableInformation);

        await ExecuteCommand(connection, $"CREATE TEMPORARY TABLE \"{temporaryName}\" AS TABLE \"{tableInformation.Name}\" WITH NO DATA;");
        await InsertToTableAsync(connection, entities, tableInformation, temporaryName);

        if (runAfterTemporaryTableInsert != null) await runAfterTemporaryTableInsert(tableInformation.Name, temporaryName);

        var primaryKeyColumns = tableInformation.Columns
            .Where(i => i is { PrimaryKey: true, ValueGeneratedOnAdd: false })
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

    public async Task SyncAsync<T>(IEnumerable<T> entities, string? deleteWhere = null, Func<string, string, Task>? runAfterTemporaryTableInsert = null)
    {
        var connection = await CreateOpenedConnection();

        try
        {
            await SyncAsync(connection, entities, deleteWhere, runAfterTemporaryTableInsert);
        }
        finally
        {
            if (DisposeConnection)
                await connection.DisposeAsync();
        }
    }

    public virtual async Task SyncAsync<T>(NpgsqlConnection connection, IEnumerable<T> entities, string? deleteWhere, Func<string, string, Task>? runAfterTemporaryTableInsert = null)
    {
        var tableInformation = await TableInformationProvider.GetTableInformation(typeof(T));
        var temporaryName = GetTemporaryTableName(tableInformation);

        await ExecuteCommand(connection, $"CREATE TEMPORARY TABLE \"{temporaryName}\" AS TABLE \"{tableInformation.Name}\" WITH NO DATA;");
        await InsertToTableAsync(connection, entities, tableInformation, temporaryName);

        if (runAfterTemporaryTableInsert != null) await runAfterTemporaryTableInsert(tableInformation.Name, temporaryName);

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

    public async Task<NpgsqlBinaryImporter<T>> CreateBinaryImporterAsync<T>(NpgsqlConnection connection)
    {
        var tableInformation = await TableInformationProvider.GetTableInformation(typeof(T));
        return await CreateBinaryImporterAsync<T>(connection, tableInformation, tableInformation.Name);
    }
    
    public async Task<NpgsqlBinaryImporter<T>> CreateBinaryImporterAsync<T>(NpgsqlConnection connection, ITableInformation tableInformation, string tableName)
    {
        var columns = tableInformation.Columns
            .Where(i => !i.ValueGeneratedOnAdd)
            .Select(i => $"\"{i.Name}\"")
            .Aggregate((x, y) => $"{x}, {y}");
        
#if NET5_0
        return await Task.FromResult(new NpgsqlBinaryImporter<T>(connection.BeginBinaryImport($"COPY \"{tableName}\" ({columns}) FROM STDIN (FORMAT BINARY)"), tableInformation));
#else
        return new NpgsqlBinaryImporter<T>(await connection.BeginBinaryImportAsync($"COPY \"{tableName}\" ({columns}) FROM STDIN (FORMAT BINARY)"), tableInformation);
#endif
    }

    private async Task<ulong> InsertToTableAsync<T>(NpgsqlConnection connection, IEnumerable<T> entities, ITableInformation tableInformation, string tableName)
    {
        await using var npgsqlBinaryImporter = await CreateBinaryImporterAsync<T>(connection, tableInformation, tableName);

        var inserted = await npgsqlBinaryImporter.WriteToBinaryImporter(entities);
        await npgsqlBinaryImporter.CompleteAsync();
        await npgsqlBinaryImporter.DisposeAsync();
        return inserted;
    }
}