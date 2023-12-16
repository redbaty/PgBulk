using System.Reflection;
using PgBulk.Abstractions;
using PgBulk.SourceGenerator.Abstractions;

namespace PgBulk.SourceGenerator.Debug;

[PgBulkValueProvider]
public partial class TestRowValueProvider : IPgBulkImporterProvider<TestRow>
{
    public partial IEnumerable<object> GetValues(TestRow entity);
    
    public partial IEnumerable<PropertyInfo> GetPropertyOrder();
}