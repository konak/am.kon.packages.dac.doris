using System;
using System.Data;
using System.Threading.Tasks;
using am.kon.packages.dac.doris.Extensions;
using am.kon.packages.dac.primitives;
using am.kon.packages.dac.primitives.Exceptions;
using MySql.Data.MySqlClient;

namespace am.kon.packages.dac.doris;

public partial class DataBase
{
    internal async Task<IDataReader> ExecuteReaderAsyncInternal(IDbConnection connection, string sqlQuery, IDataParameter[] parameters, CommandType commandType = CommandType.Text)
    {
        var conn = connection as MySqlConnection;
        var command = new MySqlCommand(sqlQuery, conn)
        {
            CommandType = commandType
        };

        var returnValue = new MySqlParameter("@return_value", MySqlDbType.Int32)
        {
            Direction = ParameterDirection.ReturnValue,
            IsNullable = false
        };

        command.Parameters.Add(returnValue);

        if (parameters != null && parameters.Length > 0)
            command.Parameters.AddRange(parameters);

        var reader = await command.ExecuteReaderAsync(CommandBehavior.CloseConnection, _cancellationToken).ConfigureAwait(false);

        int retVal = 0;

        if (returnValue.Value != null && returnValue.Value != DBNull.Value)
            retVal = Convert.ToInt32(returnValue.Value);

        if (retVal != 0)
            throw new DacSqlExecutionReturnedErrorCodeException(retVal, reader);

        return reader;
    }

    public Task<IDataReader> ExecuteReaderAsync(string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        Func<IDbConnection, Task<IDataReader>> execute = connection => ExecuteReaderAsyncInternal(connection, sql, parameters, commandType);

        return ExecuteSQLBatchAsync(execute, false, throwDbException, throwGenericException, throwSystemException);
    }

    public Task<IDataReader> ExecuteReaderAsync(string sql, MySqlParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        return ExecuteReaderAsync(sql, (IDataParameter[])parameters, commandType, throwDbException, throwGenericException, throwSystemException);
    }

    [Obsolete("Use ExecuteReaderAsync(string sql, DacDorisParameters parameters, ...) instead", false)]
    public Task<IDataReader> ExecuteReaderAsync(string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        return ExecuteReaderAsync(sql, parameters.ToDataParameters(), commandType, throwDbException, throwGenericException, throwSystemException);
    }

    public Task<IDataReader> ExecuteReaderAsync(string sql, DacDorisParameters parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        Func<IDbConnection, Task<IDataReader>> execute = connection => ExecuteReaderAsyncInternal(connection, sql, (IDataParameter[])parameters.ToArray(), commandType);

        return ExecuteSQLBatchAsync(execute, false, throwDbException, throwGenericException, throwSystemException);
    }
}
