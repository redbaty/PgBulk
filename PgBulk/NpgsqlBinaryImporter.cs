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

    private readonly SemaphoreSlim _writeLock = new(1, 1);

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
            await WriteToBinaryImporter(entity);
            inserted++;
        }

        return inserted;
    }

    public async Task WriteToBinaryImporter(T entity)
    {
        await _writeLock.WaitAsync();
        
        try
        {
            await BinaryImporter.StartRowAsync();

            foreach (var column in Columns)
            {
                var value = column.GetValue(entity);
                await BinaryImporter.WriteAsync(value);
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public ValueTask<ulong> CompleteAsync()
    {
        return BinaryImporter.CompleteAsync();
    }
}