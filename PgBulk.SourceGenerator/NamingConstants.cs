using System.Reflection;
using PgBulk.SourceGenerator.Abstractions;

namespace PgBulk.SourceGenerator;

internal static class NamingConstants
{
    private static readonly Assembly Assembly = typeof(NamingConstants).Assembly;
        
    private static readonly string AssemblyName = Assembly.GetName().Name ?? "PgBulk.SourceGenerator";
        
    internal static readonly string ValueGeneratorTemplateName = $"{AssemblyName}.Templates.value_generator.template";
    
    internal static readonly string ImporterTemplateName = $"{AssemblyName}.Templates.importer.template";

    internal const string BulkValueProviderInterfaceName = "IPgBulkImporterProvider";

    internal static readonly string GeneratedNpgsqlBinaryImporterInterfaceName = GetNameWithoutGenericArity(typeof(IGeneratedNpgsqlBinaryImporter<>));
        
    internal static readonly string ValueProviderAttributeName = typeof(PgBulkValueProviderAttribute).FullName!;
    
    internal static readonly string ImporterAttributeName = typeof(PgBulkImporterAttribute).FullName!;

    private static string GetNameWithoutGenericArity(MemberInfo t)
    {
        var name = t.Name;
        var index = name.IndexOf('`');
        return index == -1 ? name : name[..index];
    }
}