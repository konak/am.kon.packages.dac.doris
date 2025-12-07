using System;
using System.Data;
using am.kon.packages.dac.doris.Extensions;
using am.kon.packages.dac.primitives;
using am.kon.packages.dac.primitives.Constants.Exception;
using am.kon.packages.dac.primitives.Exceptions;
using MySql.Data.MySqlClient;

namespace am.kon.packages.dac.doris;

public partial class DataBase
{
    public void FillData<T>(T dataOut, string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        MySqlCommand command = null;
        MySqlDataAdapter adapter = null;

        try
        {
            command = new MySqlCommand(sql, new MySqlConnection(_connectionString))
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

            adapter = new MySqlDataAdapter(command);

            switch (dataOut)
            {
                case DataTable table:
                    if (maxRecords == 0)
                        adapter.Fill(table);
                    else
                        adapter.Fill(startRecord, maxRecords, new DataTable[] { table });
                    break;

                case DataSet dataSet:
                    if (maxRecords == 0)
                        adapter.Fill(dataSet);
                    else
                        adapter.Fill(dataSet, startRecord, maxRecords, string.Empty);
                    break;

                default:
                    if (throwSystemException)
                        throw new DacGenericException(Messages.FILL_DATA_INVALID_TYPE_PASSED + typeof(T));
                    break;
            }

            int retVal = 0;
            if (returnValue.Value != null && returnValue.Value != DBNull.Value)
                retVal = System.Convert.ToInt32(returnValue.Value);

            if (retVal != 0)
                throw new DacSqlExecutionReturnedErrorCodeException(retVal, dataOut);
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
            if (command != null)
            {
                try
                {
                    command.Connection?.Close();
                }
                catch (Exception ex)
                {
                    if (throwSystemException)
                        throw new DacGenericException(Messages.SQL_CONNECTION_CLOSE_EXCEPTION, ex);
                }
            }
        }
    }

    public void FillData<T>(T dataOut, string sql, MySqlParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        FillData(dataOut, sql, (IDataParameter[])parameters, commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    [Obsolete("Use FillData<T>(..., DacDorisParameters ...) instead.", false)]
    public void FillData<T>(T dataOut, string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        FillData(dataOut, sql, parameters.ToDataParameters(), commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillData<T>(T dataOut, string sql, DacDorisParameters parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        FillData(dataOut, sql, (IDataParameter[])parameters.ToArray(), commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }
}
