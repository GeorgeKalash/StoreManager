using System.Data.SqlClient;
using System.Data;
using System;

namespace StoreManager
{
    public class SqlAdapter : StoreAdapter
    {
        SqlConnection connection;
        SqlCommand cmd;
        SqlDataReader reader;

        public SqlAdapter() : base()
        {
            cmd = new SqlCommand();
        }

        public override void setExecutionType(ExecutionType _executionType)
        {
            base.setExecutionType(_executionType);
            cmd.CommandType = executionType == ExecutionType.STORED_PROCEDURE ? CommandType.StoredProcedure : CommandType.Text;
        }

        public override void close()
        {
            if (connection == null)
            {
                return;
            }
            if (connection.State == ConnectionState.Open)
            {
                connection.Dispose();
                connection.Close();
            }

        }

        public override void connect()
        {
            connection = new SqlConnection(connectionString);
        }

        public override void init(string _cmd)
        {
            if (_cmd == string.Empty)
            {
                throw new Exception("DATA_OBJECT_NOT_SET");
            }
            cmd.CommandText = _cmd;
            cmd.Connection = connection;
            cmd.CommandTimeout = 60;
        }
        public override void open(int _accessMode = AccessMode.ReadWrite)
        {
            try
            {
                connection.Open();
            }
            catch (Exception ex)
            {
                throw new Exception("STORE_CONNECTION_FAILED "  + ex.Message);
            }
        }
        public override void addParameter(string _parameterName, object _parameterValue, bool _isMandatory, bool _isPrimaryKey = false)
        {
            try
            {
                if (_parameterValue != null)
                {
                    if (_isMandatory == true && isEmpty(_parameterValue))
                        throw new Exception("EMPTY_MANDATORY_FIELD " + _parameterName);
                }
                else
                {
                    if (_isMandatory == true)
                        throw new Exception("EMPTY_MANDATORY_FIELD " + _parameterName);
                }
                if (executionType == ExecutionType.STORED_PROCEDURE)
                    cmd.Parameters.AddWithValue(_parameterName, _parameterValue == null ? DBNull.Value : _parameterValue);
                else
                    base.addField(_parameterName, _parameterValue, _isPrimaryKey);
            }
            finally
            {
            }
        }
        public override bool read()
        {
            try
            {
                return reader.Read();
            }
            catch (SqlException ex)
            {
                throw new Exception("STORE_READ_ERROR " + ex.Message);
            }
        }
        public override bool hasRows()
        {
            return reader.HasRows;
        }
        public override bool execute()
        {
            try
            {
                reader = cmd.ExecuteReader(CommandBehavior.KeyInfo);
                return true;
            }
            catch (SqlException ex)
            {
                throw new Exception("STORE_EXECUTION_ERROR " + ex.Message);
            }

        }

        public override void executeNonQuery()
        {
            cmd.CommandType = CommandType.Text;
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                throw new Exception("STORE_EXECUTION_ERROR " + ex.Message);
            }
        }

        public override object Object(string _tag)
        {
            object obj = SqlReader.Object(reader, _tag);
            if (obj == DBNull.Value)
                return null;
            return obj;
        }

        public override DateTime? Date(string _tag)
        {
            return SqlReader.Date(reader, _tag);
        }
        public override T Object<T>(string _tag)
        {
            if (typeof(T) == typeof(int?))
            {
                Type u = Nullable.GetUnderlyingType(typeof(T));
                int? i = Integer(_tag);
                if (i == null)
                    return default(T);
                return (T)Convert.ChangeType(i, u);
            }
            string _ = String(_tag);
            return (T)Convert.ChangeType(_, typeof(T));
        }
        public override String String(string _tag)
        {
            return SqlReader.String(reader, _tag);
        }
        public override short? Short(string _tag)
        {
            return SqlReader.Short(reader, _tag);
        }
        public override int? Integer(string _tag)
        {
            return SqlReader.Int(reader, _tag);
        }
        public override double? Double(string _tag)
        {
            return SqlReader.Double(reader, _tag);
        }
        public override bool? Bool(string _tag)
        {
            return SqlReader.Bool(reader, _tag);
        }
        public override string getCmd<T>(StoreManager<T> _model)
        {
            // stop deleting this tmp var, i need it for debugging!!
            string cmd = string.Format("select top 1 * from [{0}] where {1}", _model.sqlObjectForGet(), primaryKeyFilter());
            return cmd;
        }
        public override string delCmd<T>(StoreManager<T> _model)
        {
            return string.Format("delete from {0} where {1}", _model.dataObject(), primaryKeyFilter());
        }

        public override string qryCmd<T>(StoreManager<T> _model)
        {
            string whereClause = _model.filterString();
            string orderClause = _model.orderString();
            string objectName = _model.sqlObjectForGet();

            if (objectName == null)
                throw new Exception("GET_OBJECT_NOT_SET ");

            string cmd = string.Format("select {0} from {1}", fieldsList(), objectName);
            if (whereClause != string.Empty)
                cmd += " where " + whereClause;
            if (orderClause != null)
                cmd += " order by " + orderClause;
            return cmd;
        }

        //public override void runBatch(string _sqlBatch)
        //{
        //    connect();
        //    // Server server = new Server(new ServerConnection(connectionString));
        //    Server server = new Server(new ServerConnection(connection));
        //    try
        //    {
        //        connection.Open();
        //    }
        //    catch (SqlException ex)
        //    {
        //        Exception _ = new Exception(ErrorId.SQL_BATCH_RUN_ERROR, string.Format("{0}, {1}", ex.Message, ex.Source));
        //        throw _;
        //    }

        //    try
        //    {
        //        server.ConnectionContext.ExecuteNonQuery(_sqlBatch);
        //    }
        //    catch (Exception ex)
        //    {
        //        // Exception _2 = 
        //        throw new Exception(ErrorId.SQL_BATCH_RUN_ERROR, string.Format("{0}, {1}", ex.Message, ex.Source));
        //    }
        //    finally
        //    {
        //        connection.Close();
        //    }
        //}

        private string formatFieldName(string _fieldName)
        {
            return "[" + _fieldName + "]";
        }
        public override string updateCmd<T>(StoreManager<T> _model)
        {
            short idx = 0;

            while (idx < fields.Count && fields[idx].isPrimaryKey == true)
                ++idx;

            if (idx == fields.Count) // all fields are PKs, nothing to update.
                return null;

            string keyValues = string.Format("{0}={1}", formatFieldName(fields[idx].name), formatValue(fields[idx].value));
            ++idx;

            while (idx < fields.Count)
            {
                keyValues += "," + string.Format("[{0}]={1}", fields[idx].name, formatValue(fields[idx].value));
                ++idx;
            }
            string dataObject = _model.dataObject();
            if (dataObject == null)
                throw new Exception("DATA_OBJECT_NOT_SET ");

            return string.Format("update [{0}] set {1} where {2}", dataObject, keyValues, primaryKeyFilter());

        }

        private string fieldsList()
        {
            if (fields.Count == 0)
                return "*";

            string commaSeparatedParams = formatFieldName(fields[0].name);
            const string separater = ",";
            int idx = 1;
            while (idx < fields.Count)
            {
                commaSeparatedParams += separater + formatFieldName(fields[idx].name);
                ++idx;
            }
            return commaSeparatedParams;
        }

        public override string insertCmd<T>(StoreManager<T> _model)
        {
            short idx = 0;
            string commaSeparatedParams = string.Empty, commaSeparatedValues = string.Empty;

            string separater = ",";

            // skip the null values
            while (idx < fields.Count && fields[idx].value == null)
                ++idx;

            if (idx < fields.Count)
            {
                commaSeparatedParams = formatFieldName(fields[idx].name);
                commaSeparatedValues = formatValue(fields[idx].value);
                ++idx;
            }

            while (idx < fields.Count)
            {
                if (fields[idx].value != null)
                {
                    commaSeparatedParams += separater + formatFieldName(fields[idx].name);
                    commaSeparatedValues += separater + formatValue(fields[idx].value);
                }
                ++idx;
            }

            string dataObject = _model.dataObject();

            if (dataObject == null)
                throw new Exception("DATA_OBJECT_NOT_SET");

            return string.Format("insert into {0} ({1}) values ({2})", dataObject, commaSeparatedParams, commaSeparatedValues);
        }

        public override string primaryKeyFilter()
        {
            string returnValue = string.Empty;
            foreach (Field field in fields)
            {
                if (field.isPrimaryKey)
                {
                    if (field.value == null)
                        return null; // new identity object

                    string fieldValue = field.value.ToString();
                    
                    if (returnValue != string.Empty)
                        returnValue += " and ";
                    
                    Type fieldType = field.value.GetType();
                    if (fieldType == typeof(string))
                        fieldValue = string.Format("'{0}'", fieldValue);
                    returnValue += string.Format("{0} = {1}", field.name, fieldValue);
                }
            }
            return returnValue;
        }
    }
}