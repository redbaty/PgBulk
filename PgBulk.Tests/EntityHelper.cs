using System.Text;
using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace PgBulk.Tests;

internal static class EntityHelper
{
    public static MyContext CreateContext(string databaseName)
    {
        var npgsqlCs = new NpgsqlConnectionStringBuilder
        {
            Host = Environment.GetEnvironmentVariable("T_POSTGRES_HOST"),
            Username = Environment.GetEnvironmentVariable("T_POSTGRES_USER"),
            Password = Environment.GetEnvironmentVariable("T_POSTGRES_PASS")
        };

        using var connection = new NpgsqlConnection(npgsqlCs.ToString());
        connection.Open();

        using var npgsqlCommand = connection.CreateCommand();
        var scriptBuilder = new StringBuilder();
        scriptBuilder.AppendLine($"DROP DATABASE IF EXISTS \"{databaseName}\";");
        scriptBuilder.AppendLine($"CREATE DATABASE \"{databaseName}\";");
        npgsqlCommand.CommandText = scriptBuilder.ToString();
        npgsqlCommand.ExecuteNonQuery();
        connection.Close();

        npgsqlCs.Database = databaseName;
        var optionsBuilder = new DbContextOptionsBuilder().UseNpgsql(npgsqlCs.ToString());
        var myContext = new MyContext(optionsBuilder.Options);
        myContext.Database.EnsureCreated();
        return myContext;
    }
}