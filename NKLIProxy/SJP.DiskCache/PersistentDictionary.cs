using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Community.CsharpSqlite.SQLiteClient;

#pragma warning disable 1591
namespace SJP
{
    public class DBFields<T> : IEqualityComparer<T>
    {
        // ReSharper disable StaticFieldInGenericType
        private static readonly Lazy<string> _fields = new Lazy<string>(GetFields);
        private static readonly Lazy<string> _fieldsWithType = new Lazy<string>(GetFieldsWithType);
        // ReSharper restore StaticFieldInGenericType

        public static string Fields { get { return _fields.Value; } }
        public static string FieldsWithType { get { return _fieldsWithType.Value; } }

        public static string Values(T obj) { return GetValues(obj); }
        public static string FieldsAndValues(T obj) { return GetFieldsAndValues(obj); }

        private static string GetFields()
        {
            var type = typeof(T);
            if (type.IsPrimitive || type == typeof(string))
            {
                return type.Name;
            }
            else
            {
                var props = typeof(T).GetProperties();
                var str = "";
                for (var i = 0; i < props.Length; i++)
                {
                    str += props[i].Name;
                    if (i != props.Length - 1)
                        str += ',';
                }
                return str;
            }
        }

        private static string GetFieldsWithType()
        {
            var type = typeof(T);
            if (type.IsPrimitive || type == typeof(string))
            {
                return type.Name + ' ' + TypeToSQL(type);
            }
            else
            {
                var props = typeof(T).GetProperties();
                var str = "";
                for (var i = 0; i < props.Length; i++)
                {
                    str += props[i].Name + ' ' + TypeToSQL(props[i].PropertyType);
                    if (i != props.Length - 1)
                        str += ',';
                }
                return str;
            }
        }

        private static string GetValues(T obj)
        {
            var type = typeof(T);
            if (type.IsPrimitive || type == typeof(string))
            {
                return "'" + obj + "'";
            }
            else
            {
                var props = type.GetProperties();
                var str = "";
                for (var i = 0; i < props.Length; i++)
                {
                    str += "'" + props[i].GetValue(obj) + "'";
                    if (i != props.Length - 1)
                        str += ',';
                }
                return str;
            }
        }

        private static string GetFieldsAndValues(T obj)
        {
            var type = typeof(T);
            if (type.IsPrimitive || type == typeof(string))
            {
                return type.Name + " = '" + obj + "'";
            }
            else
            {
                var props = typeof(T).GetProperties();
                var str = "";
                for (var i = 0; i < props.Length; i++)
                {
                    var f = props[i].Name;
                    var v = props[i].GetValue(obj);

                    str += f + " = '" + v + "'";
                    if (i != props.Length - 1)
                        str += ',';
                }
                return str;
            }
        }

        public static string TypeToSQL()
        {
            return TypeToSQL(typeof(T));
        }

        public static string TypeToSQL(Type t)
        {
            //var t = typeof (T);
            if (t == typeof(void))
                return "NULL";
            if (t == typeof(bool) || t == typeof(byte) ||
                t == typeof(char) || t == typeof(long) ||
                t == typeof(sbyte) || t == typeof(short) ||
                t == typeof(uint) || t == typeof(ulong) ||
                t == typeof(ushort) || t == typeof(int))
                return "INTEGER";
            if (t == typeof(decimal) || t == typeof(double) ||
                t == typeof(float))
                return "REAL";
            if (t == typeof(string))
                return "TEXT";
            return "BLOB"; // blob can be anything (sort of)
        }

        public bool Equals(T x, T y)
        {
            return Values(x).Equals(Values(y));
        }

        public int GetHashCode(T obj)
        {
            return Values(obj).GetHashCode();
        }
    }

    public class StringKey
    {
        public string String { get; private set; }

        public static implicit operator StringKey(string str)
        {
            return new StringKey { String = str };
        }

        public static implicit operator string(StringKey sf)
        {
            return sf.String;
        }
    }

    [DebuggerDisplay("Count = {Count}, DB = {_conn.Database}")]
    public class PersistentDictionary<TKey, TValue> : IDictionary<TKey, TValue>, IDisposable
        /*where TKey : new()*/
        //where TValue : new()
    {
        private readonly string _tableName;
        private readonly Dictionary<TKey, TValue> _dict;
        private readonly SqliteConnection _conn;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public PersistentDictionary(string db, string table)
        {
            if (string.IsNullOrWhiteSpace(db))
                throw new ArgumentNullException("db");
            if (string.IsNullOrWhiteSpace(table))
                throw new ArgumentNullException("table");

            _tableName = table;
            _dict = new Dictionary<TKey, TValue>(/*new DBFields<TKey>()*/);

            var cs = "Data Source=file:" + db + ".db";

            try
            {
                _conn = new SqliteConnection(cs);
                _conn.Open();

                var stm = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='" + _tableName + "'";
                if (!ExecuteScalar(stm, out string o))
                    throw new Exception("Could not execute query: " + stm);
                if (o.Length == 0) // new db
                {
                    stm = string.Format("CREATE TABLE {0} ({1}, {2}, PRIMARY KEY ({3}))",
                        _tableName,
                        DBFields<TKey>.FieldsWithType,
                        DBFields<TValue>.FieldsWithType,
                        DBFields<TKey>.Fields);
                    if (!ExecuteNonQuery(stm))
                        throw new Exception("Could not execute query: " + stm);
                }
                else
                {
                    stm = string.Format("SELECT {0}, {1} FROM {2}", DBFields<TKey>.Fields,
                        DBFields<TValue>.Fields, _tableName);
                    using (var cmd = new SqliteCommand(stm, _conn))
                    {
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                //var newKey = typeof (TKey).IsValueType ? default(TKey) : new TKey();v
                                var keyType = typeof(TKey);
                                TKey newKey;
                                if (keyType.IsPrimitive || keyType == typeof(string))
                                {
                                    newKey = (TKey)reader[DBFields<TKey>.Fields];
                                }
                                else
                                {
                                    newKey = Activator.CreateInstance<TKey>();
                                    var props = newKey.GetType().GetProperties();
                                    foreach (var prop in props)
                                    {
                                        var obj = reader[prop.Name];
                                        prop.SetValue(newKey, obj);
                                    }
                                }

                                TValue newValue;
                                var valueType = typeof(TValue);
                                if (valueType.IsPrimitive || valueType == typeof(string))
                                {
                                    newValue = (TValue)reader[DBFields<TValue>.Fields];
                                }
                                else
                                {
                                    newValue = Activator.CreateInstance<TValue>();
                                    var props = newValue.GetType().GetProperties();
                                    foreach (var prop in props)
                                    {                                       
                                        var obj = reader[prop.Name];

                                        if (prop.Name == "CreationTime"  || prop.Name == "LastAccessed")
                                        {
                                            prop.SetValue(newValue, Convert.ToDateTime(obj));

                                        }
                                        else if (prop.Name == "Size" || prop.Name == "AccessCount")
                                        {
                                            prop.SetValue(newValue, Convert.ToUInt64(obj));
                                        }
                                        else prop.SetValue(newValue, obj);
                                    }
                                }

                                _dict.Add(newKey, newValue);
                            }
                        }
                    }
                }
            }
            catch (SqliteException e)
            {
                Console.WriteLine(e);
            }
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return _dict.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)_dict).GetEnumerator();
        }

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        public void Clear()
        {
            var stm = string.Format("DELETE FROM {0}", _tableName);
            if (ExecuteNonQuery(stm))
                _dict.Clear();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return ((ICollection<KeyValuePair<TKey, TValue>>)_dict).Contains(item);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            ((ICollection<KeyValuePair<TKey, TValue>>)_dict).CopyTo(array, arrayIndex);
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key);
        }

        public int Count
        {
            get { return _dict.Count; }
        }

        public bool IsEmpty
        {
            get
            {
                if (Count > 0) return false;
                else return true;
            }
        }

        public bool IsReadOnly
        {
            get { return ((ICollection<KeyValuePair<TKey, TValue>>)_dict).IsReadOnly; }
        }

        public bool ContainsKey(TKey key)
        {
            return _dict.ContainsKey(key);
        }

        public void Add(TKey key, TValue value)
        {
            var stm = string.Format("INSERT INTO {0} ({1}, {2}) VALUES ({3}, {4})",
                _tableName,
                DBFields<TKey>.Fields, DBFields<TValue>.Fields,
                DBFields<TKey>.Values(key), DBFields<TValue>.Values(value));

            if (ExecuteNonQuery(stm))
                _dict.Add(key, value);
        }

        public bool TryRemove(TKey key, out TValue value)
        {
            if (TryGetValue(key, out value))
            {
                Remove(key);
                return true;
            }
            else return false;
        }

        public bool Remove(TKey key)
        {
            var stm = string.Format("DELETE FROM {0} WHERE {1}", _tableName, DBFields<TKey>.FieldsAndValues(key));
            return ExecuteNonQuery(stm) && _dict.Remove(key);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return _dict.TryGetValue(key, out value);
        }

        public TValue this[TKey key]
        {
            get { return _dict[key]; }
            set
            {
                if (!_dict.ContainsKey(key))
                    Add(key, value);
                else
                {
                    var stm = string.Format("UPDATE {0} SET {1} WHERE {2}",
                        _tableName,
                        DBFields<TValue>.FieldsAndValues(value),
                        DBFields<TKey>.FieldsAndValues(key));
                    if (ExecuteNonQuery(stm))
                        _dict[key] = value;
                }
            }
        }

        public ICollection<TKey> Keys
        {
            get { return _dict.Keys; }
        }

        public ICollection<TValue> Values
        {
            get { return _dict.Values; }
        }

        protected virtual void Dispose(bool d)
        {
            if (d)
            {
                if (_conn != null)
                {
                    try
                    {
                        _conn.Close();
                        _conn.Dispose();
                    }
                    catch (SqliteException) { } // the unseen exception is the deadliest
                    // but we are just disposing stuff, who
                    // cares if it fails?
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        private bool ExecuteScalar(string stm, out string ret)
        {
            lock (_conn)
            {
                try
                {
                    using (var cmd = new SqliteCommand(stm, _conn))
                        ret = Convert.ToString(cmd.ExecuteScalar());
                    return true;
                }
                catch (SqliteException ex)
                {
                    Console.WriteLine(ex);
                    ret = null;
                    //throw;
                    return false;
                }
            }

        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        private bool ExecuteNonQuery(string stm)
        {
            lock (_conn)
            {
                try
                {
                    using (var cmd = new SqliteCommand(stm, _conn))
                        cmd.ExecuteNonQuery();
                    return true;
                }
                catch (SqliteException ex)
                {
                    Console.WriteLine(ex);
                    //throw;
                    return false;
                }
            }
        }
    }
}
#pragma warning restore 1591