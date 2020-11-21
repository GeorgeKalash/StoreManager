using System;
using System.Collections.Generic;
using System.Linq;

namespace StoreManager
{
    public enum ExecutionType
    {
        TEXT = 1,
        STORED_PROCEDURE = 2 // CommandType.StoredProcedure
    }

    public class Field
    {
        public string name;
        public object value;
        public bool isPrimaryKey;
    }

    public abstract class StoreAdapter
    {

        protected List<Field> fields;

        protected string connectionString;

        public void clearParams()
        {
            if (fields.Count > 0)
                fields.RemoveRange(0, fields.Count);
        }

        protected ExecutionType executionType = ExecutionType.TEXT;

        public class AccessMode
        {
            public const int ReadWrite = 0;
            public const int Read = 1;
            public const int Write = 2;
        }
        public StoreAdapter()
        {
            fields = new List<Field>();
        }
        public virtual void setExecutionType(ExecutionType _executionType)
        {
            executionType = _executionType;
        }

        public object primaryKeys()
        {
            List<Field> filteredFields = fields.Where(x => x.isPrimaryKey == true).ToList();

            if (filteredFields.Count == 0)
                return null;

            if (filteredFields.Count == 1)
                return filteredFields[0];

            object[] keys = new object[filteredFields.Count];

            int idx = 0;

            foreach (Field key in filteredFields)
            {
                keys[idx] = key.value;
                idx++;
            }

            return keys;
        }

        public void setConnectionString(string _connectionString) { connectionString = _connectionString; }
        public abstract void close();
        public abstract void connect();
        public abstract void open(int _accessMode = AccessMode.ReadWrite);
        public abstract bool execute();
        public abstract void executeNonQuery();
        public abstract bool read();
        public abstract bool hasRows();
        public abstract string primaryKeyFilter();

        public abstract void init(string _cmd);
        public virtual void runBatch(string _sqlBatch) { }

        public abstract T Object<T>(string _tag);
        public abstract object Object(string _tag);
        public abstract DateTime? Date(string _tag);
        public abstract string String(string _tag);
        public abstract int? Integer(string _tag);
        public abstract short? Short(string _tag);
        public abstract bool? Bool(string _tag);
        public abstract double? Double(string _tag);
        public virtual string Time(string _tag)
        {
            string str = String(_tag);
            return str == "99:99" ? string.Empty : str;
        }

        public abstract void addParameter(string _parameterName, object _parameterValue, bool _isMandatory, bool _isPrimaryKey = false);

        public virtual void addField(string _parameterName, object _parameterValue, bool _isPrimaryKey)
        {
            if (fields.Exists(x => x.name == _parameterName))
            {
                Field f = fields.First(x => x.name == _parameterName);
                f.value = _parameterValue;
                f.isPrimaryKey = _isPrimaryKey;
                return;
            }
            fields.Add(new Field() { name = _parameterName, value = _parameterValue, isPrimaryKey = _isPrimaryKey });
        }

        public virtual void addField(string _parameterName)
        {
            fields.Add(new Field() { name = _parameterName });
        }

        protected bool isEmpty(object _obj)
        {

            if (_obj.GetType() == typeof(string))
            {
                return (string)_obj == "";
            }

            if (_obj.GetType() == typeof(DateTime))
            {
                return (DateTime)_obj == new DateTime(1, 1, 1);
            }

            if (_obj.GetType() == typeof(int))
            {
                return (int)_obj == 0;
            }

            if (_obj.GetType() == typeof(short))
            {
                return (short)_obj == 0;
            }

            if (_obj.GetType() == typeof(double))
            {
                return (double)_obj == 0;
            }


            return false;
        }
        public abstract string delCmd<T>(StoreManager<T> _model) where T : new();
        public abstract string getCmd<T>(ViewManager<T> _model) where T : new();
        public abstract string qryCmd<T>(ViewManager<T> _model) where T : new();
        public abstract string insertCmd<T>(StoreManager<T> _model) where T : new();
        public abstract string updateCmd<T>(StoreManager<T> _model) where T : new();

        public string setCmd<T>(StoreManager<T> _model, ref TrxMode _trxMode) where T : new()
        {
            if (executionType == ExecutionType.STORED_PROCEDURE)
            {
                if (_model.setObject() == null)
                    throw new Exception("DATA_OBJECT_NOT_SET");
                return _model.setObject();
            }
            if (_model.exists())
            {
                _trxMode = TrxMode.UPDATE;
                return updateCmd(_model);
            }
            _trxMode = TrxMode.INSERT;
            return insertCmd(_model);
        }
        protected string formatValue(object _value)
        {
            if (_value == null)
                return "null";

            Type type = _value.GetType();

            if (type == typeof(string))
                return "N'" + _value.ToString().Replace("'", "''") + "'";

            if (type == typeof(DateTime))
            {
                return "N'" + ((DateTime)_value).ToString("yyyy-MM-dd HH:mm:ss") + "'";
            }

            if (_value is Enum)
                return Convert.ToInt16(_value).ToString();

            if (type == typeof(int) || type == typeof(double) || type == typeof(short))
                return _value.ToString();
            if (type == typeof(bool))
                return (bool)_value ? "1" : "0";
            throw new Exception("VALUE_OUT_OF_RANGE" + string.Format("formatting type {0}", type.ToString()));
        }
    }

}