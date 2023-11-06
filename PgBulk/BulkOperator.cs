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

    public ITableInformationProvider TableInformationProvider { get; }

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

    public async Task MergeAsync<T>(ICollection<T> entities, ITableKeyProvider? tableKeyProvider = null)
    {
        var connection = await CreateOpenedConnection();

        try
        {
            await MergeAsync(connection, entities, tableKeyProvider ?? new DefaultTableKeyProvider());
        }
        finally
        {
            if (DisposeConnection)
                await connection.DisposeAsync();
        }
    }

    public async Task InsertAsync<T>(IEnumerable<T> entities, bool onConflictIgnore)
    {
        var connection = await CreateOpenedConnection();

        try
        {
            await InsertToTableAsync(connection, entities, onConflictIgnore);
        }
        finally
        {
            if (DisposeConnection)
                await connection.DisposeAsync();
        }
    }

    private async Task<ulong> InsertToTableAsync<T>(NpgsqlConnection npgsqlConnection, IEnumerable<T> entities, bool onConflictIgnore)
    {
        var tableInformation = await TableInformationProvider.GetTableInformation(typeof(T));
        return await InsertToTableAsync(npgsqlConnection, entities, tableInformation, tableInformation.Name, onConflictIgnore);
    }

    public virtual async Task MergeAsync<T>(NpgsqlConnection connection, ICollection<T> entities, ITableKeyProvider tableKeyProvider, Func<string, string, Task>? runAfterTemporaryTableInsert = null)
    {
        var tableInformation = await TableInformationProvider.GetTableInformation(typeof(T));
        var temporaryName = GetTemporaryTableName(tableInformation);

        await CreateTemporaryTable(connection, tableInformation, temporaryName);
        await InsertToTableAsync(connection, entities, tableInformation, temporaryName, false);

        if (runAfterTemporaryTableInsert != null) await runAfterTemporaryTableInsert(tableInformation.Name, temporaryName);

        var tableKey = tableKeyProvider.GetKeyColumns(tableInformation);
        var primaryKeyColumns = tableKey
            .Columns
            .Select(i => i.SafeName)
            .DefaultIfEmpty()
            .Aggregate((x, y) => $"{x},{y}");

        if (string.IsNullOrEmpty(primaryKeyColumns))
            throw new InvalidOperationException($"No primary keys defined for table \"{tableInformation.Name}\"");

        if (tableKey.IsUniqueConstraint)
        {
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
        else
        {
            await using var transaction = await connection.BeginTransactionAsync();
            try
            {
                var deleteScriptBuilder = new StringBuilder($"delete from \"{tableInformation.Schema}\".\"{tableInformation.Name}\" where ");
                var first = true;

                foreach (var column in tableKey.Columns.OrderBy(i => i.Index))
                {
                    if (!first)
                        deleteScriptBuilder.Append(" and ");

                    deleteScriptBuilder.Append($"{column.SafeName} = @p{column.Index}");
                    first = false;
                }

                var deleteScript = deleteScriptBuilder.ToString();

                foreach (var entity in entities)
                {
                    var npgsqlParameters = tableKey.Columns
                        .Select(i => new NpgsqlParameter($"p{i.Index}", i.GetValue(entity)))
                        .ToArray();

                    await ExecuteCommand(connection, deleteScript, npgsqlParameters);
                }

                await ExecuteCommand(connection, $"insert into \"{tableInformation.Schema}\".\"{tableInformation.Name}\" (select * from \"{temporaryName}\")");
                await transaction.CommitAsync();
            }
            finally
            {
                await transaction.DisposeAsync();
            }
        }
    }

    private async Task CreateTemporaryTable(NpgsqlConnection connection, ITableInformation sourceTable, string temporaryName)
    {
        var script = $"CREATE TEMPORARY TABLE \"{temporaryName}\" AS TABLE \"{sourceTable.Schema}\".\"{sourceTable.Name}\" WITH NO DATA;";
        await ExecuteCommand(connection, script);
    }

    private async Task<int> ExecuteCommand(NpgsqlConnection connection, string script, IEnumerable<NpgsqlParameter>? parameters = null)
    {
        await using var npgsqlCommand = connection.CreateCommand();
        npgsqlCommand.CommandText = script;

        if (parameters != null)
        {
            foreach (var parameter in parameters)
            {
                npgsqlCommand.Parameters.Add(parameter);
            }
        }

        LogBeforeCommand(npgsqlCommand);
        var stopWatch = Stopwatch.StartNew();
        var updatedRows = await npgsqlCommand.ExecuteNonQueryAsync();

        stopWatch.Start();
        LogAfterCommand(npgsqlCommand, stopWatch.Elapsed);

        return updatedRows;
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
        await InsertToTableAsync(connection, entities, tableInformation, temporaryName, false);

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

    public async Task<NpgsqlBinaryImporter<T>> CreateBinaryImporterAsync<T>()
    {
        await using var connection = await CreateOpenedConnection();
        return await CreateBinaryImporterAsync<T>(connection);
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
    public async Task<NpgsqlBinaryImporter<T>> CreateBinaryImporterAsync<T>(NpgsqlConnection connection, IEnumerable<ITableColumnInformation> columns, string targetTableName, string? targetSchema = null)
    {
        var columnsFiltered = columns.Where(i => !i.ValueGeneratedOnAdd).ToList();

        if (columnsFiltered.Count <= 0)
            throw new InvalidOperationException("No valid columns found on type " + typeof(T).Name);

        var columnsString = columnsFiltered
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
        return await Task.FromResult(new NpgsqlBinaryImporter<T>(connection.BeginBinaryImport(command), columnsFiltered));
#else
        return new NpgsqlBinaryImporter<T>(await connection.BeginBinaryImportAsync(command), columnsFiltered);
#endif
    }

    private async Task<ulong> InsertToTableAsync<T>(NpgsqlConnection connection, IEnumerable<T> entities, ITableInformation tableInformation, string tableName, bool onConflictIgnore)
    {
        if (!onConflictIgnore) return await InsertToTableAsync(connection, entities, tableInformation, tableName);
        
        var temporaryName = GetTemporaryTableName(tableInformation);
        await CreateTemporaryTable(connection, tableInformation, temporaryName);
        await InsertToTableAsync(connection, entities, tableInformation, temporaryName);
        var count = await ExecuteCommand(connection, $"insert into \"{tableInformation.Schema}\".\"{tableInformation.Name}\" (select * from \"{temporaryName}\") ON CONFLICT DO NOTHING");
        return (ulong)count;

    }

    private async Task<ulong> InsertToTableAsync<T>(NpgsqlConnection connection, IEnumerable<T> entities, ITableInformation tableInformation, string tableName)
    {
        await using var npgsqlBinaryImporter = await CreateBinaryImporterAsync<T>(connection, tableInformation.Columns, tableName);

        var inserted = await npgsqlBinaryImporter.WriteToBinaryImporter(entities);
        await npgsqlBinaryImporter.CompleteAsync();
        await npgsqlBinaryImporter.DisposeAsync();
        return inserted;
    }
}