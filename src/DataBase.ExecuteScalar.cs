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
    public Task<object> ExecuteScalarAsync(string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text)
    {
        async Task<object> Execute(IDbConnection connection)
        {
            var conn = (MySqlConnection)connection;
            var command = new MySqlCommand(sql, conn)
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

            object result = await command.ExecuteScalarAsync(_cancellationToken).ConfigureAwait(false);

            int retVal = 0;
            if (returnValue.Value != null && returnValue.Value != DBNull.Value)
                retVal = System.Convert.ToInt32(returnValue.Value);

            if (retVal != 0)
                throw new DacSqlExecutionReturnedErrorCodeException(retVal, result);

            return result;
        }

        return ExecuteSQLBatchAsync(Execute);
    }

    public Task<object> ExecuteScalarAsync(string sql, MySqlParameter[] parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteScalarAsync(sql, (IDataParameter[])parameters, commandType);
    }

    [Obsolete("Use ExecuteScalarAsync(string sql, DacDorisParameters parameters, ...) instead.", false)]
    public Task<object> ExecuteScalarAsync(string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteScalarAsync(sql, parameters.ToDataParameters(), commandType);
    }

    public Task<object> ExecuteScalarAsync(string sql, DacDorisParameters parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteScalarAsync(sql, (IDataParameter[])parameters.ToArray(), commandType);
    }
}
