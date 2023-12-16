using Npgsql;
using PgBulk.SourceGenerator.Abstractions;

namespace PgBulk.SourceGenerator.Debug;

[PgBulkImporter]
public partial class GeneratedNpgsqlBinaryImporter : IGeneratedNpgsqlBinaryImporter<TestRow>
{
    public partial ValueTask WriteAsync(TestRow entity, NpgsqlBinaryImporter writer);
}