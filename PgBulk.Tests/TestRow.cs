namespace PgBulk.Tests;

public enum TestEnum
{
    Value1,
    Value2,
    Value3
}

public class TestRow
{
    public int Id { get; set; }

    public string Value1 { get; set; } = null!;

    public string Value2 { get; set; } = null!;

    public string? Value3 { get; set; }

    public TestEnum? Value4 { get; set; }
}