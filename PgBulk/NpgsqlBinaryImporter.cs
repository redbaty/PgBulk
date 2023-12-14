using Npgsql;
using PgBulk.Abstractions;
using PgBulk.Abstractions.PropertyAccess;

namespace PgBulk;

public sealed class NpgsqlBinaryImporter<T> : IDisposable, IAsyncDisposable
{
    public NpgsqlBinaryImporter(NpgsqlBinaryImporter binaryImporter, IEnumerable<ITableColumnInformation> columns)
    {
        _binaryImporter = binaryImporter;
        _columns = columns.ToList();
    }

    private readonly NpgsqlBinaryImporter _binaryImporter;

    private readonly ICollection<ITableColumnInformation> _columns;

    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public ValueTask DisposeAsync()
    {
        return _binaryImporter.DisposeAsync();
    }

    public void Dispose()
    {
        _binaryImporter.Dispose();
    }

    public async ValueTask<ulong> WriteToBinaryImporter(IEnumerable<T> entities)
    {
        ulong inserted = 0;
        
        foreach (var entity in entities)
        {
            await WriteToBinaryImporter(entity);
            inserted++;
        }

        return inserted;
    }

    public ValueTask WriteToBinaryImporter(T entity)
    {
        if (entity == null)
            throw new ArgumentNullException(nameof(entity));
        
        return WriteToBinaryImporter(_columns.Select(c => c.GetValue(entity)));
    }
    
    public async ValueTask WriteToBinaryImporter(IEnumerable<object?> values)
    {
        await _writeLock.WaitAsync();
        
        try
        {
            await _binaryImporter.StartRowAsync();

            foreach (var value in values)
            {
                await _binaryImporter.WriteAsync(value);
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public ValueTask<ulong> CompleteAsync()
    {
        return _binaryImporter.CompleteAsync();
    }
}