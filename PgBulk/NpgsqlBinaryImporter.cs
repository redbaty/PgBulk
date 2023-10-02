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

            foreach (var column in Columns)
            {
                var value = column.GetValue(entity);

                if (value == null)
                {
                    await BinaryImporter.WriteNullAsync();
                }
                else
                {
                    await BinaryImporter.WriteAsync(value);
                }
            }

            inserted++;
        }

        return inserted;
    }

    public ValueTask<ulong> CompleteAsync()
    {
        return BinaryImporter.CompleteAsync();
    }
}