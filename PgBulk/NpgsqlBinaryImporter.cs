using Npgsql;
using PgBulk.Abstractions;

namespace PgBulk;

public sealed class NpgsqlBinaryImporter<T> : IDisposable, IAsyncDisposable
{
    public NpgsqlBinaryImporter(NpgsqlBinaryImporter binaryImporter, ICollection<ITableColumnInformation> columns)
    {
        BinaryImporter = binaryImporter;
        Columns = columns;
    }

    private NpgsqlBinaryImporter BinaryImporter { get; }

    private ICollection<ITableColumnInformation> Columns { get; }

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

            foreach (var columnValue in Columns.Select(i => i.GetValue(entity)))
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