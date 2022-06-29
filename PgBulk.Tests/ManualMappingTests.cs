using Bogus;
using Microsoft.EntityFrameworkCore;

namespace PgBulk.Tests;

[TestClass]
public class ManualMappingTests
{
    private ManualTableInformationProvider? ManualTableInformationProvider { get; set; }
    
    private static Faker<TestRow> Faker => new Faker<TestRow>().RuleFor(i => i.Id, f => f.IndexFaker)
        .RuleFor(i => i.Value1, f => f.Address.City())
        .RuleFor(i => i.Value2, f => f.Company.CompanyName())
        .RuleFor(i => i.Value3, f => f.PickRandom(null, f.Name.Suffix()));
    
    [TestInitialize]
    public void Setup()
    {
        ManualTableInformationProvider = new ManualTableInformationProvider()
            .AddTableMapping<TestRow>("TestRows", c => c.Automap());
    }

    private async Task<Tuple<ManualBulkOperator, MyContext>> GetOperator()
    {
        var myContext = EntityHelper.CreateContext(await Nanoid.Nanoid.GenerateAsync(size: 8));
        var bulkOperator = new ManualBulkOperator(myContext.Database.GetConnectionString(), ManualTableInformationProvider!);
        await bulkOperator.VerifyPrimaryKeys();

        return new Tuple<ManualBulkOperator, MyContext>(bulkOperator, myContext);
    }
    
    [TestMethod]
    [DataRow(100)]
    [DataRow(1000)]
    public async Task Insert(int value)
    {
        var (@operator, dbContext) = await GetOperator();
        await @operator.MergeAsync(Faker.Generate(value));

        var currentCount = await dbContext.TestRows.CountAsync();
        Assert.AreEqual(value, currentCount);

        await dbContext.Database.EnsureDeletedAsync();
    }

    [TestMethod]
    [DataRow(100)]
    [DataRow(1000)]
    public async Task Upsert(int value)
    {
        var (@operator, dbContext) = await GetOperator();
        var testRows = Faker.Generate(value).OrderBy(i => i.Id).ToArray();
        await @operator.MergeAsync(testRows);

        var currentCount = await dbContext.TestRows.CountAsync();
        Assert.AreEqual(value, currentCount);

        var newRows = Faker.Generate(10).OrderBy(i => i.Id).ToArray();
        await @operator.MergeAsync(newRows);

        Assert.AreEqual(value, currentCount);

        var dbRows = dbContext.TestRows.OrderBy(i => i.Id).ToArray();

        for (var index = 0; index < testRows.Length; index++)
        {
            var originalRow = testRows[index];
            var updatedRow = newRows.ElementAtOrDefault(index);

            Assert.AreEqual(index, dbRows[index].Id);
            Assert.AreEqual(updatedRow != null ? updatedRow.Value1 : originalRow.Value1, dbRows[index].Value1);
            Assert.AreEqual(updatedRow != null ? updatedRow.Value2 : originalRow.Value2, dbRows[index].Value2);
            Assert.AreEqual(updatedRow != null ? updatedRow.Value3 : originalRow.Value3, dbRows[index].Value3);
        }

        await dbContext.Database.EnsureDeletedAsync();
    }

    [TestMethod]
    [DataRow(100)]
    [DataRow(1000)]
    public async Task Sync(int value)
    {
        var (@operator, dbContext) = await GetOperator();
        var testRows = Faker.Generate(value).OrderBy(i => i.Id).ToArray();
        await @operator.SyncAsync(testRows);

        var currentCount = await dbContext.TestRows.CountAsync();
        Assert.AreEqual(value, currentCount);

        testRows = Faker.Generate(value * 2).OrderBy(i => i.Id).ToArray();
        await @operator.SyncAsync(testRows);

        currentCount = await dbContext.TestRows.CountAsync();
        Assert.AreEqual(value * 2, currentCount);

        testRows = Faker.Generate(value).OrderBy(i => i.Id).ToArray();
        await @operator.SyncAsync(testRows);

        currentCount = await dbContext.TestRows.CountAsync();
        Assert.AreEqual(value, currentCount);

        await dbContext.Database.EnsureDeletedAsync();
    }
}