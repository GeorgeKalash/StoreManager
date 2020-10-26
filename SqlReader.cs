using System;
using System.Data.SqlClient;

namespace StoreManager
{
    public abstract class SqlReader
    {
        public static string keyValue(SqlDataReader _reader, string _tag)
        {
            return string.Format("({0},{1})", _tag, _reader[_tag]);
        }

        public static object Object(SqlDataReader _reader, string _tag)
        {
            try
            {
                return _reader[_tag];
            }
            catch (IndexOutOfRangeException)
            {
                throw new Exception("OBJECT_MISSING_COLUMN " + _tag);
            }
        }

        public static string String(SqlDataReader _reader, string _tag)
        {
            try
            {
                object tmp = _reader[_tag];
                if (tmp == DBNull.Value)
                    return null;
                return (string)tmp;
            }
            catch (IndexOutOfRangeException)
            {
                throw new Exception("OBJECT_MISSING_COLUMN " + _tag);
            }
            catch (InvalidCastException ex)
            {
                throw new Exception("DATA_TYPE_MISMATCH " + string.Format("{0} {1}", ex.Message, keyValue(_reader, _tag)));
            }
        }

        public static bool? Bool(SqlDataReader _reader, string _tag)
        {
            try
            {
                object tmp = _reader[_tag];
                if (tmp == DBNull.Value)
                    return null;
                return Convert.ToBoolean(tmp);
            }
            catch (IndexOutOfRangeException)
            {
                throw new Exception("OBJECT_MISSING_COLUMN " + _tag);
            }
            catch (InvalidCastException ex)
            {
                throw new Exception("DATA_TYPE_MISMATCH " + string.Format("{0} {1}", ex.Message, keyValue(_reader, _tag)));
            }
        }

        public static short? Short(SqlDataReader _reader, string _tag)
        {
            try
            {
                object tmp = _reader[_tag];

                if (tmp == DBNull.Value)
                    return null;
                return Convert.ToInt16(tmp);
            }
            catch (IndexOutOfRangeException)
            {
                throw new Exception("OBJECT_MISSING_COLUMN " + _tag);
            }
            catch (InvalidCastException ex)
            {
                string keyValue = string.Format("({0}:{1}->{2})", _tag, _reader[_tag], ex.Message);
                throw new Exception("DATA_TYPE_MISMATCH " + keyValue);
            }
            catch (OverflowException ex)
            {
                string keyValue = string.Format("({0}:{1}->{2})", _tag, _reader[_tag], ex.Message);
                throw new Exception("VALUE_OUT_OF_RANGE " + keyValue);
            }
        }

        public static int? Int(SqlDataReader _reader, string _tag)
        {
            try
            {
                object tmp = _reader[_tag];

                if (tmp == DBNull.Value)
                    return null;
                return Convert.ToInt32(tmp);
            }
            catch (IndexOutOfRangeException)
            {
                throw new Exception("OBJECT_MISSING_COLUMN " + _tag);
            }
            catch (InvalidCastException ex)
            {
                throw new Exception("DATA_TYPE_MISMATCH " + string.Format("{0} {1}", ex.Message, keyValue(_reader, _tag)));
            }
            catch (OverflowException ex)
            {
                throw new Exception("VALUE_OUT_OF_RANGE " + string.Format("{0} {1}", ex.Message, keyValue(_reader, _tag)));
            }
        }

        public static double? Double(SqlDataReader _reader, string _tag)
        {
            try
            {
                object tmp = _reader[_tag];
                if (tmp == DBNull.Value)
                    return null;
                return Convert.ToDouble(tmp);
            }
            catch (IndexOutOfRangeException)
            {
                throw new Exception("OBJECT_MISSING_COLUMN " + _tag);
            }
            catch (InvalidCastException)
            {
                throw new Exception("DATA_TYPE_MISMATCH " + _tag);
            }
        }

        public static DateTime? Date(SqlDataReader _reader, string _tag)
        {
            try
            {
                object tmp = _reader[_tag];
                if (tmp == DBNull.Value)
                    return null;
                return Convert.ToDateTime(tmp);
            }
            catch (IndexOutOfRangeException)
            {
                throw new Exception("OBJECT_MISSING_COLUMN " + _tag);
            }
            catch (InvalidCastException)
            {
                throw new Exception("DATA_TYPE_MISMATCH " + _tag);
            }
        }

    }
}