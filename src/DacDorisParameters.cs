using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using am.kon.packages.dac.common.Cache;
using MySql.Data.MySqlClient;

namespace am.kon.packages.dac.doris;

/// <summary>
/// Represents a collection of Doris/MySQL parameters with fluent helpers for building command parameter lists.
/// </summary>
public class DacDorisParameters : IEnumerable<MySqlParameter>
{
    private readonly List<MySqlParameter> _parameters;

    public DacDorisParameters()
    {
        _parameters = new List<MySqlParameter>();
    }

    public DacDorisParameters(int capacity)
    {
        _parameters = new List<MySqlParameter>(capacity);
    }

    public DacDorisParameters(IEnumerable<MySqlParameter> collection)
    {
        _parameters = new List<MySqlParameter>(collection);
    }

    public DacDorisParameters AddItem(string name, object value)
    {
        _parameters.Add(new MySqlParameter(name, value ?? DBNull.Value));
        return this;
    }

    public DacDorisParameters AddItem(MySqlParameter parameter)
    {
        _parameters.Add(parameter);
        return this;
    }

    public DacDorisParameters AddItem(KeyValuePair<string, object> item)
    {
        _parameters.Add(new MySqlParameter(item.Key, item.Value ?? DBNull.Value));
        return this;
    }

    public DacDorisParameters AddRange(IEnumerable<MySqlParameter> collection)
    {
        _parameters.AddRange(collection);
        return this;
    }

    public DacDorisParameters AddRange(DacDorisParameters collection)
    {
        _parameters.AddRange(collection);
        return this;
    }

    public DacDorisParameters AddRange(IEnumerable<KeyValuePair<string, object>> collection)
    {
        foreach (KeyValuePair<string, object> record in collection)
        {
            _parameters.Add(new MySqlParameter(record.Key, record.Value ?? DBNull.Value));
        }

        return this;
    }

    public DacDorisParameters ReadFromObject<T>(T parameters)
    {
        if (parameters == null)
            return this;

        PropertyInfo[] properties = PropertyInfoCache.GetProperties(typeof(T));

        foreach (PropertyInfo propertyInfo in properties)
        {
            object value = propertyInfo.GetValue(parameters);
            _parameters.Add(new MySqlParameter(propertyInfo.Name, value ?? DBNull.Value));
        }

        return this;
    }

    public IEnumerator<MySqlParameter> GetEnumerator()
    {
        return _parameters.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public MySqlParameter[] ToArray()
    {
        return _parameters.ToArray();
    }
}
