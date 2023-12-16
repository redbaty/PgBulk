using Microsoft.CodeAnalysis;

namespace PgBulk.SourceGenerator
{
    internal static class IncrementalValuesProviderExtensions
    {
        /// <summary>
        /// Registers an output node into an <see cref="IncrementalGeneratorInitializationContext"/> to output a diagnostic.
        /// </summary>
        /// <param name="context">The input <see cref="IncrementalGeneratorInitializationContext"/> instance.</param>
        /// <param name="diagnostic">The input <see cref="IncrementalValuesProvider{TValues}"/> sequence of diagnostics.</param>
        public static void ReportDiagnostics(
            this IncrementalGeneratorInitializationContext context,
            IncrementalValuesProvider<Diagnostic> diagnostic
        )
        {
            context.RegisterSourceOutput(diagnostic, static (context, diagnostic) => context.ReportDiagnostic(diagnostic));
        }
    }
}