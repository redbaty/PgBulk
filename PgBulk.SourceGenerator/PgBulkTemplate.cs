using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Scriban;

namespace PgBulk.SourceGenerator
{
    internal record PgBulkTemplate(INamedTypeSymbol TargetSymbol, ClassDeclarationSyntax TargetNode, Template Template)
    {
        public string FileName => $"{TargetSymbol.Name}.g.cs";
        
        public string? Emit()
        {
            var bulkValueProviderInterface = TargetSymbol.Interfaces.SingleOrDefault(o => (o.Name == NamingConstants.BulkValueProviderInterfaceName || o.Name == NamingConstants.GeneratedNpgsqlBinaryImporterInterfaceName) && o.IsGenericType);
            var entityType = bulkValueProviderInterface?.TypeArguments.Single();

            if (entityType == null)
                return null;

            var properties = entityType.GetMembers()
                .OfType<IPropertySymbol>()
                .Where(i =>
                {
                    var isValid = i is { IsStatic: false, DeclaringSyntaxReferences.Length: > 0 };

                    if (isValid)
                    {
                        var attributeDatas = i.GetAttributes().ToArray();
                        var ignore = attributeDatas.Any(o => o.AttributeClass?.Name == nameof(IgnoreDataMemberAttribute));
                        
                        if (ignore)
                            return false;
                    }
                    
                    return isValid;
                })
                .Select(i => i.Name)
                .ToList();
            
            var usings = TargetNode.SyntaxTree.GetRoot()
                .DescendantNodes()
                .OfType<UsingDirectiveSyntax>()
                .Select(i => i.ToString())
                .ToList();

            var displayString = TargetSymbol.ContainingNamespace.ToDisplayString();
            return Template!.Render(new
            {
                csnamespace = displayString,
                className = TargetSymbol.Name,
                properties,
                entityTypeName = entityType.Name,
                usings
            });
        }
    }
}