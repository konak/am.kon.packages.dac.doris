using System;
using System.Data;
using am.kon.packages.dac.doris.Extensions;
using am.kon.packages.dac.primitives;
using MySql.Data.MySqlClient;

namespace am.kon.packages.dac.doris;

public partial class DataBase
{
    public DataSet GetDataSet(string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true,
        bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        var dataSet = new DataSet();
        FillData(dataSet, sql, parameters, commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
        return dataSet;
    }

    public DataSet GetDataSet(string sql, MySqlParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true,
        bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        var dataSet = new DataSet();
        FillData(dataSet, sql, parameters, commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
        return dataSet;
    }

    [Obsolete("Use GetDataSet(..., DacDorisParameters ...) instead.", false)]
    public DataSet GetDataSet(string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true,
        bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        var dataSet = new DataSet();
        FillData(dataSet, sql, parameters.ToDataParameters(), commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
        return dataSet;
    }

    public DataSet GetDataSet(string sql, DacDorisParameters parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true,
        bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        var dataSet = new DataSet();
        FillData(dataSet, sql, (IDataParameter[])parameters.ToArray(), commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
        return dataSet;
    }
}
