using System;

namespace StoreManager
{
    public static class SqlWrap
    {
        public static int recordCount(DataSourceManager _mgr, string _sqlCmd, string _countColName)
        {
            StoreAdapter reader = _mgr.exec(_sqlCmd);
            int? qrySize = reader.hasRows() == false ? 0 : reader.Integer(_countColName);
            reader.close();
            return qrySize == null ? 0 : (int) qrySize;
        }

        public static short? shortValue(DataSourceManager _mgr, string _fieldName, string _objectName, string _whereCondition = null, string _orderBy = null)
        {
            string cmd = sqlCmd(_fieldName, _objectName, _whereCondition, _orderBy);
            StoreAdapter reader = _mgr.exec(cmd);
            short? value = reader.hasRows() == false ? null : reader.Short(_fieldName);
            reader.close();
            return value;
        }

        public static bool? boolValue(DataSourceManager _mgr, string _fieldName, string _objectName, string _whereCondition = null, string _orderBy = null)
        {
            string cmd = sqlCmd(_fieldName, _objectName, _whereCondition, _orderBy);
            StoreAdapter reader = _mgr.exec(cmd);
            bool? value = reader.hasRows() == false ? null : reader.Bool(_fieldName);
            reader.close();
            return value;
        }

        public static string stringValue(DataSourceManager _mgr, string _sqlCmd, string _fieldName)
        {
            string cmd = string.Format("select top 1 {0} from ", _fieldName) + _sqlCmd;
            StoreAdapter reader = _mgr.exec(cmd);
            string value = reader.hasRows() == false ? null : reader.String(_fieldName);
            reader.close();
            return value;
        }

        public static string sqlCmd(string _fieldName, string _objectName, string _whereCondition, string _orderBy)
        {
            if (_whereCondition != null && _whereCondition != string.Empty)
                _whereCondition = " where " + _whereCondition;
            if (_orderBy != null && _orderBy != string.Empty)
                _orderBy = " order by " + _orderBy;
            return string.Format("select top 1 {0} from {1} {2} {3} ", _fieldName, _objectName, _whereCondition, _orderBy);
        }

        public static int? intValue(DataSourceManager _mgr, string _fieldName, string _objectName, string _whereCondition, string _orderBy)
        {
            string cmd = sqlCmd(_fieldName, _objectName, _whereCondition, _orderBy);
            StoreAdapter reader = _mgr.exec(cmd);
            int? value = reader.hasRows() == false ? null : reader.Integer(_fieldName);
            reader.close();
            return value;
        }
        public static string maxString(DataSourceManager _mgr, string _fieldName, string _objectName, string _whereCondition = null)
        {
            string sqlCmd = string.Format("select {0} = max({0}) from {1}", _fieldName, _objectName);
            if (_whereCondition != null)
                sqlCmd += " where " + _whereCondition;
            return stringValue(_mgr, sqlCmd, _fieldName);
        }

        public static int? maxInt(DataSourceManager _mgr, string _fieldName, string _objectName, string _whereCondition = null)
        {
            string sqlCmd = string.Format("select {0} = max({0}) from {1}", _fieldName, _objectName);
            if (_whereCondition != null)
                sqlCmd += " where " + _whereCondition;
            return intValue(_mgr, _fieldName, sqlCmd);
        }
        public static int? minInt(DataSourceManager _mgr, string _fieldName, string _objectName, string _whereCondition = null)
        {
            string sqlCmd = string.Format("select {0} = min({0}) from {1}", _fieldName, _objectName);
            if (_whereCondition != null)
                sqlCmd += " where " + _whereCondition;
            return intValue(_mgr, _fieldName, sqlCmd);
        }
        public static object objectValue(DataSourceManager _mgr, string _fieldName, string _cmd)
        {
            StoreAdapter reader = _mgr.exec(_cmd);
            object value = reader.hasRows() == false ? null : reader.Object(_fieldName);
            reader.close();
            return value;
        }

        public static short? shortValue(DataSourceManager _mgr, string _fieldName, string _cmd)
        {
            object value = objectValue(_mgr, _fieldName, _cmd);
            if (value == null)
                return null;
            return Convert.ToInt16(value);
        }

        public static int? intValue(DataSourceManager _mgr, string _fieldName, string _cmd)
        {
            object value = objectValue(_mgr, _fieldName, _cmd);
            if (value == null)
                return null;
            return Convert.ToInt32(value);
        }

        public static DateTime? dateValue(DataSourceManager _mgr, string _fieldName, string _objectName, string _whereCondition, string _orderBy)
        {
            string cmd = sqlCmd(_fieldName, _objectName, _whereCondition, _orderBy);
            StoreAdapter reader = _mgr.exec(cmd);
            DateTime? value = reader.hasRows() == false ? null : reader.Date(_fieldName);
            reader.close();
            return value;
        }
        public static string stringValue(DataSourceManager _mgr, string _fieldName, string _objectName, string _whereCondition, string _orderBy)
        {
            string cmd = sqlCmd(_fieldName, _objectName, _whereCondition, _orderBy);
            StoreAdapter reader = _mgr.exec(cmd);
            string value = reader.hasRows() == false ? null : reader.String(_fieldName);
            reader.close();
            return value;
        }

        public static double? doubleValue(DataSourceManager _mgr, string _fieldName, string _objectName, string _whereCondition, string _orderBy)
        {
            string cmd = sqlCmd(_fieldName, _objectName, _whereCondition, _orderBy);
            StoreAdapter reader = _mgr.exec(cmd);
            double? value = reader.hasRows() == false ? null : reader.Double(_fieldName);
            reader.close();
            return value;
        }

        public static DateTime? dateValue(DataSourceManager _mgr, string _sqlCmd, string _fieldName)
        {
            string cmd = string.Format("select top 1 {0} from ({1}) qry", _fieldName, _sqlCmd);
            StoreAdapter reader = _mgr.exec(cmd);
            DateTime? value = reader.hasRows() == false ? null : reader.Date(_fieldName);
            reader.close();
            return value;
        }

        public static double? doubleValue(DataSourceManager _mgr, string _sqlCmd, string _fieldName)
        {
            string cmd = string.Format("select top 1 {0} from ({1}) qry", _fieldName, _sqlCmd);
            StoreAdapter reader = _mgr.exec(cmd);
            double? value = reader.hasRows() == false ? null : reader.Double(_fieldName);
            reader.close();
            return value;
        }

        public static bool hasRecords(DataSourceManager _mgr, string _object, string _filter)
        {
            string sqlCmd = "select [qrySize] = count(*) from " + _object;
            if (_filter != string.Empty)
                sqlCmd += " where " + _filter;
            return recordCount(_mgr, sqlCmd, "qrySize") > 0;
        }

        public static bool exists(DataSourceManager _mgr, string _sqlCmd)
        {
            string cmd = string.Format("select [qrySize] = count(*) from {0}", _sqlCmd);
            StoreAdapter reader = _mgr.exec(cmd);
            int qrySize = (int) reader.Integer("qrySize");
            reader.close();
            return qrySize > 0;
        }

        public static bool exists(DataSourceManager _mgr, string _object, string _filter)
        {
            string cmd = string.Format("{0} where {1}", _object, _filter);
            return exists(_mgr, cmd);
        }
    }
}