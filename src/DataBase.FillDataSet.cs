using System;
using System.Data;
using am.kon.packages.dac.doris.Extensions;
using am.kon.packages.dac.primitives;
using am.kon.packages.dac.primitives.Exceptions;
using MySql.Data.MySqlClient;

namespace am.kon.packages.dac.doris;

public partial class DataBase
{
    public void FillDataSet(DataSet ds, string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        FillData(ds, sql, parameters, commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillDataSet(DataSet ds, string sql, MySqlParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        FillData(ds, sql, parameters, commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    [Obsolete("Use FillDataSet(..., DacDorisParameters ...) instead.", false)]
    public void FillDataSet(DataSet ds, string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        FillData(ds, sql, parameters.ToDataParameters(), commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillDataSet(DataSet ds, string sql, DacDorisParameters parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        FillData(ds, sql, (IDataParameter[])parameters.ToArray(), commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }
}
