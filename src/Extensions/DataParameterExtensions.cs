using System;
using System.Collections.Generic;
using System.Data;
using am.kon.packages.dac.primitives;
using MySql.Data.MySqlClient;

namespace am.kon.packages.dac.doris.Extensions
{
    public static class DataParameterExtensions
    {
        private static IDataParameter[] ToDataParameters(this KeyValuePair<string, object>[] parameters)
        {
            if (parameters == null || parameters.Length == 0)
                return Array.Empty<IDataParameter>();

            var mysqlParameters = new MySqlParameter[parameters.Length];

            for (int i = 0; i < parameters.Length; i++)
            {
                KeyValuePair<string, object> entry = parameters[i];
                mysqlParameters[i] = new MySqlParameter(entry.Key, entry.Value ?? DBNull.Value);
            }

            return mysqlParameters;
        }

        public static IDataParameter[] ToDataParameters(this DacSqlParameters parameters)
        {
            if (parameters == null)
                return Array.Empty<IDataParameter>();

            return ToDataParameters(parameters.ToArray());
        }
    }
}
