using System.Text;

namespace PgBulk;

public sealed class ManualBulkOperator : BulkOperator
{
    public ManualBulkOperator(string? connectionString, ManualTableInformationProvider tableInformationProvider) : base(connectionString, tableInformationProvider)
    {
    }

    public async Task VerifyPrimaryKeys()
    {
        if (TableInformationProvider is not ManualTableInformationProvider manualTableInformationProvider) throw new InvalidOperationException("Table information provider is not of manual type");

        foreach (var manualTableInformation in manualTableInformationProvider.TableColumnInformations.Values)
        {
            await using var connection = await CreateOpenedConnection();
            var command = connection.CreateCommand();
            var scriptBuilder = new StringBuilder();
            scriptBuilder.AppendLine("SELECT a.attname");
            scriptBuilder.AppendLine("FROM pg_index i");
            scriptBuilder.AppendLine("JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)");
            scriptBuilder.AppendLine($"WHERE  i.indrelid = '\"{manualTableInformation.Name}\"'::regclass");
            command.CommandText = scriptBuilder.ToString();
            var reader = await command.ExecuteReaderAsync();
            var primaryKeys = new HashSet<string>();

            while (await reader.ReadAsync()) primaryKeys.Add(reader.GetString(0));

            foreach (var tableColumnInformation in manualTableInformation.Columns.OfType<ManualTableColumnMapping>()) tableColumnInformation.PrimaryKey = primaryKeys.Contains(tableColumnInformation.Name);
        }
    }
}