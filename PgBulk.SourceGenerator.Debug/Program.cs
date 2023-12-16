// See https://aka.ms/new-console-template for more information

using System;
using PgBulk;
using PgBulk.SourceGenerator.Debug;

var bkOperator = new BulkOperator("", new ManualTableInformationProvider());
Console.WriteLine("Hello, World!");