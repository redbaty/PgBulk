using Bogus;
using Microsoft.EntityFrameworkCore;
using PgBulk.EFCore;

namespace PgBulk.Tests;

[TestClass]
public class EFCoreTests
{
    private static Faker<TestRow> Faker => new Faker<TestRow>().RuleFor(i => i.Id, f => f.IndexFaker)
        .RuleFor(i => i.Value1, f => f.Address.City())
        .RuleFor(i => i.Value2, f => f.Company.CompanyName())
        .RuleFor(i => i.Value3, f => f.PickRandom(null, f.Name.Suffix()));

    private static MyContext CreateContext()
    {
        return EntityHelper.CreateContext(Nanoid.Nanoid.Generate(size: 8));
    }

    [TestMethod]
    [DataRow(100)]
    [DataRow(1000)]
    public async Task Insert(int value)
    {
        await using var myContext = CreateContext();
        await myContext.BulkInsertAsync(Faker.Generate(value));

        var currentCount = await myContext.TestRows.CountAsync();
        Assert.AreEqual(value, currentCount);

        await myContext.Database.EnsureDeletedAsync();
    }

    [TestMethod]
    [DataRow(100)]
    [DataRow(1000)]
    public async Task Upsert(int value)
    {
        await using var myContext = CreateContext();
        var testRows = Faker.Generate(value).OrderBy(i => i.Id).ToArray();
        await myContext.BulkMergeAsync(testRows);

        var currentCount = await myContext.TestRows.CountAsync();
        Assert.AreEqual(value, currentCount);

        var newRows = Faker.Generate(10).OrderBy(i => i.Id).ToArray();
        await myContext.BulkMergeAsync(newRows);

        Assert.AreEqual(value, currentCount);

        var dbRows = myContext.TestRows.OrderBy(i => i.Id).ToArray();

        for (var index = 0; index < testRows.Length; index++)
        {
            var originalRow = testRows[index];
            var updatedRow = newRows.ElementAtOrDefault(index);

            Assert.AreEqual(index, dbRows[index].Id);
            Assert.AreEqual(updatedRow != null ? updatedRow.Value1 : originalRow.Value1, dbRows[index].Value1);
            Assert.AreEqual(updatedRow != null ? updatedRow.Value2 : originalRow.Value2, dbRows[index].Value2);
            Assert.AreEqual(updatedRow != null ? updatedRow.Value3 : originalRow.Value3, dbRows[index].Value3);
        }

        await myContext.Database.EnsureDeletedAsync();
    }

    [TestMethod]
    [DataRow(100)]
    [DataRow(1000)]
    public async Task Sync(int value)
    {
        await using var myContext = CreateContext();
        var testRows = Faker.Generate(value).OrderBy(i => i.Id).ToArray();
        await myContext.BulkSyncAsync(testRows);

        var currentCount = await myContext.TestRows.CountAsync();
        Assert.AreEqual(value, currentCount);

        testRows = Faker.Generate(value * 2).OrderBy(i => i.Id).ToArray();
        await myContext.BulkSyncAsync(testRows);

        currentCount = await myContext.TestRows.CountAsync();
        Assert.AreEqual(value * 2, currentCount);

        testRows = Faker.Generate(value).OrderBy(i => i.Id).ToArray();
        await myContext.BulkSyncAsync(testRows);

        currentCount = await myContext.TestRows.CountAsync();
        Assert.AreEqual(value, currentCount);

        await myContext.Database.EnsureDeletedAsync();
    }
}