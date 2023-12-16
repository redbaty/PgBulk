using System.Runtime.Serialization;

namespace PgBulk.SourceGenerator.Debug;

public record TestRow(string Data1, string Data2)
{
    public string Test3 { get; set; }
    
    public string Test4 { get; set; }
}