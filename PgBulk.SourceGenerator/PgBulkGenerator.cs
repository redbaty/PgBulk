using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using Scriban;

namespace PgBulk.SourceGenerator
{
    [Generator(LanguageNames.CSharp)]
    public class PgBulkGenerator : IIncrementalGenerator
    {
        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            try
            {
                var compilationDiagnostics = context
                    .CompilationProvider
                    .SelectMany(static (compilation, _) => BuildCompilationDiagnostics(compilation));
                context.ReportDiagnostics(compilationDiagnostics);

                var valueProviderTemplate = GetTemplate(NamingConstants.ValueGeneratorTemplateName);
                var importersTemplate = GetTemplate(NamingConstants.ImporterTemplateName);
            
                var valueProviders = context
                    .SyntaxProvider
                    .ForAttributeWithMetadataName(
                        NamingConstants.ValueProviderAttributeName,
                        static (s, _) => s is ClassDeclarationSyntax,
                        static (ctx, _) => (ctx.TargetSymbol, TargetNode: (ClassDeclarationSyntax)ctx.TargetNode)
                    )
                    .Where(x => x.TargetSymbol is INamedTypeSymbol)
                    .Select((x, _) => new PgBulkTemplate((INamedTypeSymbol)x.TargetSymbol, x.TargetNode, valueProviderTemplate));
            
                var importers = context
                    .SyntaxProvider
                    .ForAttributeWithMetadataName(
                        NamingConstants.ImporterAttributeName,
                        static (s, _) => s is ClassDeclarationSyntax,
                        static (ctx, _) => (ctx.TargetSymbol, TargetNode: (ClassDeclarationSyntax)ctx.TargetNode)
                    )
                    .Where(x => x.TargetSymbol is INamedTypeSymbol)
                    .Select((x, _) => new PgBulkTemplate((INamedTypeSymbol)x.TargetSymbol, x.TargetNode, importersTemplate));
            
                RegisterForOutput(context, valueProviders);
                RegisterForOutput(context, importers);
            }
            catch (Exception e)
            {
                if (e.InnerException != null)
                    throw e.InnerException;

                throw;
            }
        }

        private static Template GetTemplate(string templateName)
        {
            using var st = typeof(PgBulkTemplate).Assembly.GetManifestResourceStream(templateName);
            using var reader = new StreamReader(st!);
            return Template.Parse(reader.ReadToEnd());
        }

        private static void RegisterForOutput(IncrementalGeneratorInitializationContext context, IncrementalValuesProvider<PgBulkTemplate> valueProviders)
        {
            context.RegisterImplementationSourceOutput(
                valueProviders,
                static (spc, template) =>
                {
                    var sourceCode = template.Emit();
                    
                    if(sourceCode != null)
                        spc.AddSource(template.FileName, SourceText.From(sourceCode, Encoding.UTF8));
                });
        }

        private static IEnumerable<Diagnostic> BuildCompilationDiagnostics(Compilation compilation)
        {
            if (compilation is CSharpCompilation { LanguageVersion: < LanguageVersion.CSharp9 } cSharpCompilation)
            {
                yield return Diagnostic.Create(
                    new DiagnosticDescriptor(
                        "RMG046",
                        "The used C# language version is not supported by PgBulk, PgBulk requires at least C# 9.0",
                        "PgBulk does not support the C# language version {0} but requires at C# least version {1}",
                        "PgBulk",
                        DiagnosticSeverity.Error,
                        true
                    ),
                    null,
                    cSharpCompilation.LanguageVersion.ToDisplayString(),
                    LanguageVersion.CSharp9.ToDisplayString()
                );
            }
        }
    }
}