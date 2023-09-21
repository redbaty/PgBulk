using Npgsql;
using PgBulk.Abstractions;

namespace PgBulk;

public sealed class NpgsqlBinaryImporter<T> : IDisposable, IAsyncDisposable
{
    public NpgsqlBinaryImporter(NpgsqlBinaryImporter binaryImporter, ITableInformation tableInformation)
    {
        BinaryImporter = binaryImporter;
        TableInformation = tableInformation;
        WritableColumns = TableInformation.Columns.Where(i => !i.ValueGeneratedOnAdd).ToList();
    }

    private NpgsqlBinaryImporter BinaryImporter { get; }

    private ITableInformation TableInformation { get; }

    private ICollection<ITableColumnInformation> WritableColumns { get; }

    public async ValueTask DisposeAsync()
    {
        await BinaryImporter.DisposeAsync();
    }

    public void Dispose()
    {
        BinaryImporter.Dispose();
    }

    public async Task<ulong> WriteToBinaryImporter(IEnumerable<T> entities)
    {
        ulong inserted = 0;

        foreach (var entity in entities)
        {
            await BinaryImporter.StartRowAsync();

            foreach (var columnValue in WritableColumns.Select(i => i.GetValue(entity)))
                await BinaryImporter.WriteAsync(columnValue);

            inserted++;
        }

        return inserted;
    }

    public ValueTask<ulong> CompleteAsync()
    {
        return BinaryImporter.CompleteAsync();
    }
}