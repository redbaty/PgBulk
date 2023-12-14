using System.Diagnostics;
using Bogus;
using Microsoft.EntityFrameworkCore;
using NanoidDotNet;
using PgBulk.EFCore;

namespace PgBulk.Tests;

[TestClass]
public class EFCoreTests
{
    private static Faker<TestRow> Faker => new Faker<TestRow>().RuleFor(i => i.Id, f => f.IndexFaker)
        .RuleFor(i => i.Value1, f => f.Address.City())
        .RuleFor(i => i.Value2, f => f.Company.CompanyName())
        .RuleFor(i => i.Value3, f => f.PickRandom(null, f.Name.Suffix()))
        .RuleFor(i => i.Value4, f => f.PickRandom<TestEnum?>(null, TestEnum.Value1, TestEnum.Value2, TestEnum.Value3))
    ;

    private static MyContext CreateContext()
    {
        return EntityHelper.CreateContext(Nanoid.Generate(size: 8));
    }

    [TestMethod]
    [DataRow(100)]
    [DataRow(1000)]
    [DataRow(100_000)]
    [DataRow(1_000_000)]
    [DataRow(10_000_000)]
    public async Task Insert(int value)
    {
        await using var myContext = CreateContext();
        var entities = Faker.Generate(value);
        
        try
        {
            var st = Stopwatch.StartNew();
            await myContext.BulkInsertAsync(entities);
            st.Stop();

            Console.WriteLine($"Insert {value} rows in \"{st.Elapsed}\"");

            var currentCount = await myContext.TestRows.CountAsync();
            Assert.AreEqual(value, currentCount);
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }
    
    [TestMethod]
    [DataRow(100)]
    [DataRow(1000)]
    public async Task InsertConflictIgnore(int value)
    {
        await using var myContext = CreateContext();
        var testRows = Faker.Generate(value);
        await myContext.BulkInsertAsync(testRows);

        var currentCount = await myContext.TestRows.CountAsync();
        Assert.AreEqual(value, currentCount);
        
        var newRows = Faker
            .RuleFor(x => x.Id, f => f.IndexFaker + testRows.Count - 5)
            .Generate(10)
            .ToArray();
        
        await myContext.BulkInsertAsync(newRows, onConflictIgnore: true);
        currentCount = await myContext.TestRows.CountAsync();

        Assert.AreEqual(value + 5, currentCount);
        
        await myContext.Database.EnsureDeletedAsync();
    }

    [TestMethod]
    [DataRow(100)]
    [DataRow(1000)]
    public async Task Upsert(int value)
    {
        await using var myContext = CreateContext();
        
        try
        {
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
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }
    
    [TestMethod]
    [DataRow(100)]
    [DataRow(1000)]
    public async Task UpsertCustomKey(int value)
    {
        await using var myContext = CreateContext();
        try
        {
            var customKeyProvider = new EntityManualTableKeyProvider<TestRow>();
            await customKeyProvider.AddKeyColumn(i => i.Value1, myContext);
        
            var testRows = Faker.Generate(value).OrderBy(i => i.Value1).ToArray();
            await myContext.BulkMergeAsync(testRows, tableKeyProvider: customKeyProvider);

            var currentCount = await myContext.TestRows.CountAsync();
            Assert.AreEqual(value, currentCount);

            var values = testRows.Select(i => i.Value1).Take(10).ToList();
            var newRows = Faker
                .RuleFor(x => x.Value1, f =>
                {
                    var picked = f.PickRandom(values);
                    values.Remove(picked);
                    return picked;
                })
                .RuleFor(x => x.Id, f => f.IndexFaker + testRows.Length)
                .Generate(10).OrderBy(i => i.Value1).ToArray();
            
            await myContext.BulkMergeAsync(newRows, tableKeyProvider: customKeyProvider);
            currentCount = await myContext.TestRows.CountAsync();
            Assert.AreEqual(value, currentCount);
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();   
        }
    }

    [TestMethod]
    [DataRow(100)]
    [DataRow(1000)]
    public async Task Sync(int value)
    {
        await using var myContext = CreateContext();
        
        try
        {
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
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }
}