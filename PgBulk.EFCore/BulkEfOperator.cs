using Microsoft.EntityFrameworkCore;

namespace PgBulk.EFCore;

public class BulkEfOperator : BulkOperator
{
    public BulkEfOperator(DbContext dbContext) : base(dbContext.Database.GetConnectionString()!, new EntityTableInformationProvider(dbContext))
    {
    }
}