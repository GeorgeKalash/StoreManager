using System;
using System.Collections.Generic;
using SharedClasses;

namespace StoreManager
{
    public class Filter
    {
        internal class FilterExpression
        {
            public string key;
            public string oper;
            public object value;
            public string parentObject;

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
                if (parentObject != null)
                    return null;

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

        public Filter createParentFilter(string _parentObject)
        {
            Filter f = new Filter();
            foreach (FilterExpression filterExpression in expressions)
            {
                if (filterExpression.parentObject == _parentObject)
                    f.add(filterExpression.key, filterExpression.oper, filterExpression.value);
            }
            return f;
        }


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
        public string Str(string _key)
        {
            try
            {
                object val = value(_key);
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

        public void add(string _key, string _oper, object _value, string _parentObject = null)
        {
            if (_value != null)
            {
                add(_key, _value);
                expressions.Add(new FilterExpression() { key = _key, oper = _oper, value = _value, parentObject = _parentObject });
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

        //public Filter where(string _dataObject)
        //{
        //    foreach(pARA)
        //}
    }

}