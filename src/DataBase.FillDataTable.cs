using System;
using System.Data;
using am.kon.packages.dac.doris.Extensions;
using am.kon.packages.dac.primitives;
using MySql.Data.MySqlClient;

namespace am.kon.packages.dac.doris;

public partial class DataBase
{
    public void FillDataTable(DataTable dataTable, string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true,
        bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        FillData(dataTable, sql, parameters, commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillDataTable(DataTable dataTable, string sql, MySqlParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true,
        bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        FillData(dataTable, sql, parameters, commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    [Obsolete("Use FillDataTable(..., DacDorisParameters ...) instead.", false)]
    public void FillDataTable(DataTable dataTable, string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true,
        bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        FillData(dataTable, sql, parameters.ToDataParameters(), commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillDataTable(DataTable dataTable, string sql, DacDorisParameters parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true,
        bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        FillData(dataTable, sql, (IDataParameter[])parameters.ToArray(), commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }
}
