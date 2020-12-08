using System;
using System.Collections.Generic;

namespace StoreManager
{
    public static class CloneClass
    {
        /// <summary>
        /// Clones a object via shallow copy
        /// </summary>
        /// <typeparam name="T">Object Type to Clone</typeparam>
        /// <param name="obj">Object to Clone</param>
        /// <returns>New Object reference</returns>
        public static T CloneObject<T>(this T obj) where T : class
        {
            if (obj == null) return null;
            System.Reflection.MethodInfo inst = obj.GetType().GetMethod("MemberwiseClone",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            if (inst != null)
                return (T)inst.Invoke(obj, null);
            else
                return null;
        }
    }

    public class Filter
    {
        private class FilterExpression
        {
            public string key;
            public string oper;
            public object value;

            public FilterExpression()
            {
            }

            private string stringValue()
            {
                if (value is Enum)
                    return Convert.ToInt16(value).ToString();
                if (value.GetType() != typeof(DateTime))
                    return value.ToString();
                return ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss");
            }
            public string clause()
            {
                string formattedValue = stringValue();

                if (value == null || value.ToString() == string.Empty || value.ToString() == "0")
                    return null;

                if (oper == "like")
                    formattedValue = "%" + formattedValue + "%";

                if (oper == "start")
                {
                    oper = "like";
                    formattedValue += "%";
                }

                if ((value.GetType() == typeof(string) || value.GetType() == typeof(DateTime)))
                    formattedValue = "N'" + formattedValue + "'";

                if (key.Substring(0, 1) != "[")
                    key = '[' + key + ']';


                return string.Format("({0} {1} {2})", key, oper, formattedValue);
            }

        }

        public const int ALL = 0;
        private readonly Dictionary<string, object> parameters;
        private readonly List<string> filters;
        private readonly List<FilterExpression> expressions;


        public Filter()
        {
            filters = new List<string>();
            expressions = new List<FilterExpression>();
            parameters = new Dictionary<string, object>();
        }


        public Filter(Filter _copyFrom)
        {
            //DebugLog.log("filters");
            filters = CloneClass.CloneObject<List<string>>(_copyFrom.filters);
            //DebugLog.log("expressions");
            expressions = CloneClass.CloneObject<List<FilterExpression>>(_copyFrom.expressions);
            //DebugLog.log("parameters");
            parameters = CloneClass.CloneObject<Dictionary<string, object>>(_copyFrom.parameters);
        }

        public Filter(string _clause) : this()
        {
            add(_clause);
        }

        public object value(string _key)
        {
            try
            {
                return parameters[_key];
            }
            catch
            {
                return null;
            }
        }
        public int Integer(string _key)
        {
            try
            {
                return Convert.ToInt32(parameters[_key]);
            }
            catch
            {
                return Filter.ALL;
            }
        }
        public short Short(string _key)
        {
            try
            {
                return Convert.ToInt16(parameters[_key]);
            }
            catch
            {
                return Filter.ALL;
            }
        }

        public DateTime? Date(string _key)
        {

            if (parameters.ContainsKey(_key))
            {
                if (parameters[_key] == null)
                    return null;
                return Convert.ToDateTime(parameters[_key]);
            }
            return null;
        }
        public String Str(string _key)
        {
            try
            {
                Object val = value(_key);
                return val?.ToString();
            }
            catch
            {
                return null;
            }
        }
        public void add(string _clause)
        {
            if (_clause != null && _clause != string.Empty)
                filters.Add("(" + _clause + ")");
        }

        public void add(string _key, string _oper, object _value)
        {
            if (_value != null)
            {
                add(_key, _value);
                expressions.Add(new FilterExpression() { key = _key, oper = _oper, value = _value });
            }
        }
        public void remove(string _key, string _oper)
        {
            int idx = 0;
            while (idx < expressions.Count)
            {
                FilterExpression exp = expressions[idx];
                if (exp.key == _key && exp.oper == _oper)
                {
                    expressions.RemoveAt(idx);
                    return;
                }
                ++idx;
            }
        }
        public void update(string _key, string _oper, object _value)
        {
            remove(_key, _oper);
            add(_key, _oper, _value);
        }

        public void add(string _parameterName, object _paramterValue)
        {
            if (parameters.ContainsKey(_parameterName) == true)
                parameters.Remove(_parameterName);

            parameters.Add(_parameterName, _paramterValue);
        }
        public virtual void deserialize()
        {

        }

        public bool hasData()
        {
            return filters.Count > 0 || expressions.Count > 0;
        }
        public virtual string filterString()
        {
            if (filters.Count == 0 && expressions.Count == 0)
                return string.Empty;

            string whereClause = string.Empty;

            if (filters.Count > 0)
            {
                whereClause = filters[0];

                for (int idx = 1; idx < filters.Count; idx++)
                {
                    whereClause = append(whereClause);
                    whereClause += filters[idx];
                }
            }

            if (expressions.Count == 0)
                return whereClause;

            for (int idx = 0; idx < expressions.Count; idx++)
            {
                string clause = expressions[idx].clause();
                if (clause != null)
                {
                    whereClause = append(whereClause);
                    whereClause += clause;
                }
            }

            return whereClause;
        }

        public string append(string _whereClause)
        {
            if (_whereClause != string.Empty)
                _whereClause += " and ";

            return _whereClause;
        }

        protected string clean(string _filter)
        {
            char[] charsToTrim = { '\"', ' ' };
            return _filter.Trim(charsToTrim);
        }

        public object Clone()
        {
            throw new NotImplementedException();
        }
    }

}