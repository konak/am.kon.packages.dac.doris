# am.kon.packages.dac.doris

`am.kon.packages.dac.doris` adapts the DAC abstraction to Apache Doris via its MySQL-compatible wire protocol (using `MySql.Data`). The surface area mirrors the other providers so repositories and services stay interchangeable.

## Installation

```bash
 dotnet add package am.kon.packages.dac.doris
```

## Creating a database instance

```csharp
using am.kon.packages.dac.doris;

var database = new DataBase(
    connectionString: configuration.GetConnectionString("Doris"),
    cancellationToken: stoppingToken);
```

## Building parameters

```csharp
var parameters = new DacDorisParameters()
    .AddItem("@customer_id", 42)
    .AddItem("@is_active", true);
```

## Executing commands

```csharp
int affected = await database.ExecuteNonQueryAsync(
    sql: "UPDATE customers SET is_active = 0 WHERE customer_id = @customer_id",
    parameters: parameters.ToArray());

object count = await database.ExecuteScalarAsync(
    sql: "SELECT COUNT(*) FROM orders WHERE customer_id = @customer_id",
    parameters: parameters.ToArray());
```

## Reading and materialising data

```csharp
await using var reader = await database.ExecuteReaderAsync(
    sql: "SELECT customer_id, display_name FROM customers WHERE customer_id = @customer_id",
    parameters: parameters.ToArray());

while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(reader.GetOrdinal("display_name")));
}

var table = new DataTable();
database.FillData(
    dataOut: table,
    sql: "SELECT * FROM orders WHERE customer_id = @customer_id",
    parameters: parameters.ToArray());
```

`FillData` buffers rows into a `DataTable` or `DataSet` before applying optional paging (`startRecord`, `maxRecords`). `GetDataTable`/`GetDataSet` just allocate a container and call `FillData`.

## Batch helpers and transactions

Use `ExecuteSQLBatchAsync` when you want to reuse a single open connection without starting a transaction:

```csharp
using MySql.Data.MySqlClient;

var totals = await database.ExecuteSQLBatchAsync(async connection =>
{
    var conn = (MySqlConnection)connection;

    using var countCustomers = new MySqlCommand("SELECT COUNT() FROM customers", conn);
    int customerCount = Convert.ToInt32(await countCustomers.ExecuteScalarAsync());

    using var countOrders = new MySqlCommand("SELECT COUNT() FROM orders", conn);
    int orderCount = Convert.ToInt32(await countOrders.ExecuteScalarAsync());

    return (customerCount, orderCount);
});
```

Wrap multi-statement work in `ExecuteTransactionalSQLBatchAsync` to get automatic commit or rollback:

```csharp
await database.ExecuteTransactionalSQLBatchAsync(async transaction =>
{
    var conn = (MySqlConnection)transaction.Connection;
    var tx = (MySqlTransaction)transaction;

    using var debit = new MySqlCommand("UPDATE accounts SET balance = balance - @amount WHERE id = @source", conn, tx);
    debit.Parameters.AddWithValue("@amount", amount);
    debit.Parameters.AddWithValue("@source", sourceAccount);
    await debit.ExecuteNonQueryAsync();

    using var credit = new MySqlCommand("UPDATE accounts SET balance = balance + @amount WHERE id = @target", conn, tx);
    credit.Parameters.AddWithValue("@amount", amount);
    credit.Parameters.AddWithValue("@target", targetAccount);
    await credit.ExecuteNonQueryAsync();

    return true;
});
// Commit is issued when the delegate completes; any thrown exception rolls the transaction back.
```

### Transactional updates across databases

Coordinate changes to multiple Doris databases with `TransactionScope`. Call `scope.Complete()` to commit; omitting it cancels the distributed transaction.

```csharp
using System.Transactions;
using MySql.Data.MySqlClient;
using am.kon.packages.dac.doris;

var primary = new DataBase(primaryConnectionString, CancellationToken.None);
var analytics = new DataBase(analyticsConnectionString, CancellationToken.None);

using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

await primary.ExecuteTransactionalSQLBatchAsync(async tx =>
{
    var cmd = (MySqlCommand)tx.Connection.CreateCommand();
    cmd.Transaction = (MySqlTransaction)tx;
    cmd.CommandText = "UPDATE customers SET is_active = 0 WHERE customer_id = @id";
    cmd.Parameters.AddWithValue("@id", customerId);
    await cmd.ExecuteNonQueryAsync();
    return 0;
});

await analytics.ExecuteTransactionalSQLBatchAsync(async tx =>
{
    var cmd = (MySqlCommand)tx.Connection.CreateCommand();
    cmd.Transaction = (MySqlTransaction)tx;
    cmd.CommandText = "INSERT INTO customer_events(customer_id, event) VALUES(@id, 'deactivated')";
    cmd.Parameters.AddWithValue("@id", customerId);
    await cmd.ExecuteNonQueryAsync();
    return 0;
});

scope.Complete(); // Remove this line to cancel and roll back both updates.
```

## Deriving custom databases

Create derived classes when you want to bundle domain-specific helpers or enforce consistent parameter creation.

```csharp
public sealed class ReportingDataBase : DataBase
{
    public ReportingDataBase(string connectionString, CancellationToken token)
        : base(connectionString, token) { }

    public Task<DataTable> LoadMonthlySummaryAsync(int year, int month)
    {
        var parameters = new DacDorisParameters()
            .AddItem("@year", year)
            .AddItem("@month", month);

        return Task.FromResult(GetDataTable(
            sql: "SELECT * FROM reporting.monthly_summary WHERE year = @year AND month = @month",
            parameters: parameters.ToArray()));
    }
}
```

Because the public API follows `IDataBase`, derived classes can compose new helpers without re-implementing connection management or transaction handling.
