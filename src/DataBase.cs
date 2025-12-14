using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using am.kon.packages.dac.primitives;
using am.kon.packages.dac.primitives.Constants.Exception;
using am.kon.packages.dac.primitives.Exceptions;
using MySql.Data.MySqlClient;

namespace am.kon.packages.dac.doris;

/// <summary>
/// Represents an Apache Doris database abstraction (via the MySQL wire protocol) that implements functionality for executing SQL commands and transactions.
/// </summary>
public partial class DataBase : IDataBase
{
    private readonly string _connectionString;
    private readonly CancellationToken _cancellationToken;

    /// <summary>
    /// Connection string of <see cref="IDataBase"/> connection.
    /// </summary>
    public string ConnectionString => _connectionString;

    /// <summary>
    /// Initializes a new instance of the DataBase class.
    /// </summary>
    /// <param name="connectionString">The connection string used to establish database connection.</param>
    /// <param name="cancellationToken">The cancellation token to cancel async operations.</param>
    public DataBase(string connectionString, CancellationToken cancellationToken)
    {
        _connectionString = connectionString;
        _cancellationToken = cancellationToken;
    }

    /// <inheritdoc />
    public async Task<T> ExecuteSQLBatchAsync<T>(Func<IDbConnection, Task<T>> batch, bool closeConnection = true, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        T res = default;
        MySqlConnection connection = null;

        try
        {
            connection = new MySqlConnection(_connectionString);
            await connection.OpenAsync(_cancellationToken).ConfigureAwait(false);
            res = await batch(connection).ConfigureAwait(false);
        }
        catch (MySqlException ex)
        {
            if (throwDbException)
                throw new DacSqlExecutionException(ex);
        }
        catch (DacSqlExecutionReturnedErrorCodeException)
        {
            throw;
        }
        catch (DacGenericException)
        {
            if (throwGenericException)
                throw;
        }
        catch (Exception ex)
        {
            if (throwSystemException)
                throw new DacGenericException(Messages.SYSTEM_EXCEPTION_ON_EXECUTE_SQL_BATCH_LEVEL, ex);
        }
        finally
        {
            if (closeConnection && connection != null)
            {
                try
                {
                    await connection.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (throwSystemException)
                        throw new DacGenericException(Messages.SQL_CONNECTION_CLOSE_EXCEPTION, ex);
                }
            }
        }

        return res;
    }

    /// <inheritdoc />
    public async Task<T> ExecuteTransactionalSQLBatchAsync<T>(Func<IDbTransaction, Task<T>> batch, bool closeConnection = true, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        T res = default;
        MySqlConnection connection = null;
        DbTransaction transaction = null;

        try
        {
            connection = new MySqlConnection(_connectionString);
            await connection.OpenAsync(_cancellationToken).ConfigureAwait(false);
            transaction = await connection.BeginTransactionAsync(_cancellationToken).ConfigureAwait(false);

            res = await batch(transaction).ConfigureAwait(false);

            await transaction.CommitAsync(_cancellationToken).ConfigureAwait(false);
        }
        catch (MySqlException ex)
        {
            if (transaction != null)
                await transaction.RollbackAsync(_cancellationToken).ConfigureAwait(false);

            if (throwDbException)
                throw new DacSqlExecutionException(ex);
        }
        catch (DacSqlExecutionReturnedErrorCodeException)
        {
            if (transaction != null)
                await transaction.RollbackAsync(_cancellationToken).ConfigureAwait(false);

            throw;
        }
        catch (DacGenericException)
        {
            if (transaction != null)
                await transaction.RollbackAsync(_cancellationToken).ConfigureAwait(false);

            if (throwGenericException)
                throw;
        }
        catch (Exception ex)
        {
            if (transaction != null)
                await transaction.RollbackAsync(_cancellationToken).ConfigureAwait(false);

            if (throwSystemException)
                throw new DacGenericException(Messages.SYSTEM_EXCEPTION_ON_EXECUTE_SQL_BATCH_LEVEL, ex);
        }
        finally
        {
            if (closeConnection && connection != null)
            {
                try
                {
                    await connection.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (throwSystemException)
                        throw new DacGenericException(Messages.SQL_CONNECTION_CLOSE_EXCEPTION, ex);
                }
            }
        }

        return res;
    }
}
