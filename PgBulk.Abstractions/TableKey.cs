namespace PgBulk.Abstractions;

public record TableKey(ICollection<ITableColumnInformation> Columns, bool IsUniqueConstraint);