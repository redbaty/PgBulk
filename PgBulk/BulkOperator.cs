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

    public virtual async Task<NpgsqlConnection> CreateOpenedConnection()
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

        await CreateTemporaryTable(connection, tableInformation, temporaryName);
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

        var baseCommand = new StringBuilder($"insert into \"{tableInformation.Schema}\".\"{tableInformation.Name}\" (select * from \"{temporaryName}\") ON CONFLICT ");

        if (!string.IsNullOrEmpty(setStatement))
            baseCommand.Append($"({primaryKeyColumns}) DO UPDATE SET {setStatement}");
        else
            baseCommand.Append("DO NOTHING");
        
        await ExecuteCommand(connection, baseCommand.ToString());
    }

    private async Task CreateTemporaryTable(NpgsqlConnection connection, ITableInformation sourceTable, string temporaryName)
    {
        var script = $"CREATE TEMPORARY TABLE \"{temporaryName}\" AS TABLE \"{sourceTable.Schema}\".\"{sourceTable.Name}\" WITH NO DATA;";
        await ExecuteCommand(connection, script);
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

        await CreateTemporaryTable(connection, tableInformation, temporaryName);
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
        return await CreateBinaryImporterAsync<T>(connection, tableInformation.Columns, tableInformation.Name);
    }

    /// <summary>
    /// Creates a binary importer to a specific type/table.
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="columns"></param>
    /// <param name="targetTableName">If none is provided, the name from tableInformation will be used instead.</param>
    /// <param name="targetSchema">If none is provided, the schema from tableInformation will be used instead.</param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public async Task<NpgsqlBinaryImporter<T>> CreateBinaryImporterAsync<T>(NpgsqlConnection connection, ICollection<ITableColumnInformation> columns, string targetTableName, string? targetSchema = null)
    {
        var columnsString = columns
            .Select(i => $"\"{i.Name}\"")
            .Aggregate((x, y) => $"{x}, {y}");
        
        var commandBuilder = new StringBuilder("COPY ");

        if (!string.IsNullOrEmpty(targetSchema))
        {
            commandBuilder.Append($"\"{targetSchema}\".");
        }
        
        commandBuilder.Append($"\"{targetTableName}\" ({columnsString}) FROM STDIN (FORMAT BINARY)");
        
        var command = commandBuilder.ToString();
        
#if NET5_0
        return await Task.FromResult(new NpgsqlBinaryImporter<T>(connection.BeginBinaryImport(command), columns));
#else
        return new NpgsqlBinaryImporter<T>(await connection.BeginBinaryImportAsync(command), columns);
#endif
    }

    private async Task<ulong> InsertToTableAsync<T>(NpgsqlConnection connection, IEnumerable<T> entities, ITableInformation tableInformation, string tableName)
    {
        var isValueSet = tableInformation.Columns
            .Where(i => i is { ValueGeneratedOnAdd: true })
            .Any(i => entities.Any(o => i.GetValue(o) != default));

        await using var npgsqlBinaryImporter = await CreateBinaryImporterAsync<T>(connection, tableInformation.Columns.Where(i => isValueSet || !i.ValueGeneratedOnAdd).ToList(), tableName);

        var inserted = await npgsqlBinaryImporter.WriteToBinaryImporter(entities);
        await npgsqlBinaryImporter.CompleteAsync();
        await npgsqlBinaryImporter.DisposeAsync();
        return inserted;
    }
}