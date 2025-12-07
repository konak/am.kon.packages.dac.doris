using System;
using System.Data;
using am.kon.packages.dac.doris.Extensions;
using am.kon.packages.dac.primitives;
using MySql.Data.MySqlClient;

namespace am.kon.packages.dac.doris;

public partial class DataBase
{
    public DataTable GetDataTable(string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true,
        bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        var table = new DataTable("Table0");
        FillData(table, sql, parameters, commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
        return table;
    }

    public DataTable GetDataTable(string sql, MySqlParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true,
        bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        var table = new DataTable("Table0");
        FillData(table, sql, parameters, commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
        return table;
    }

    [Obsolete("Use GetDataTable(..., DacDorisParameters ...) instead.", false)]
    public DataTable GetDataTable(string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true,
        bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        var table = new DataTable("Table0");
        FillData(table, sql, parameters.ToDataParameters(), commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
        return table;
    }

    public DataTable GetDataTable(string sql, DacDorisParameters parameters, CommandType commandType = CommandType.Text, bool throwDbException = true, bool throwGenericException = true,
        bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        var table = new DataTable("Table0");
        FillData(table, sql, (IDataParameter[])parameters.ToArray(), commandType, throwDbException, throwGenericException, throwSystemException, startRecord, maxRecords);
        return table;
    }
}
