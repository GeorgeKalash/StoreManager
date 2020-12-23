using System;
using System.Collections.Generic;
using System.Dynamic;

namespace StoreManager
{
    public enum TrxMode { INSERT, UPDATE, DELETE }

    public class PageData
    {
        public int startAt;
        public short pageSize;
        public string sortField;

        public PageData(int _startAt, short _pageSize, string _sortField)
        {
            startAt = _startAt;
            pageSize = _pageSize;
            sortField = _sortField;
        }

    }

    public abstract class DataSourceManager
    {
        public StoreAdapter dbAdapter;
        public void connect(String _cmd)
        {
            dbAdapter.connect();
            dbAdapter.init(_cmd);
        }
        public void closeConnection()
        {
            dbAdapter.close();
        }

        public virtual StoreAdapter exec(string _cmd, bool _disconnectWhenDone = false)
        {
            try
            {
                connect(_cmd);
                dbAdapter.open();
                dbAdapter.execute();
                dbAdapter.read();
                if (_disconnectWhenDone)
                    closeConnection();
            }
            finally
            {
            }
            return dbAdapter;
        }
    }

    public abstract class ViewManager<T> : DataSourceManager where T : new()
    {
        private const int MAX_BATCH_READ = 2000000;

        protected string dataObj = null;
        protected string getObj = null;

        public Filter filter;
        public int querySize;
        protected string orderBy = null;

        public List<T> qryResult;

        public ViewManager()
        {
        }
        ~ViewManager()
        {
            closeConnection();
        }

        protected virtual void getCompleted() { }
        protected abstract void read(T _object);
        protected virtual void runExpensiveCode() { }
        protected virtual void setFields() { }

        protected virtual T factory()
        {
            T obj = new T();
            read(obj);
            return obj;
        }

        // -------------------------------------------------------------------------------------
        // connect
        // -------------------------------------------------------------------------------------
        public void open()
        {
            dbAdapter.open();
        }
        public void setExecutionType(ExecutionType _executionType)
        {
            dbAdapter.setExecutionType(_executionType);
        }

        // -------------------------------------------------------------------------------------
        // qry
        // -------------------------------------------------------------------------------------
        public virtual void setFilter(Filter _filter)
        {
            filter = _filter;
        }
        public virtual void setOrder(string _order)
        {
            orderBy = _order;
        }
        public virtual string filterString()
        {
            if (filter == null)
                return string.Empty;

            return filter.filterString();
        }
        public virtual string orderString()
        {
            return orderBy;
        }
        public void setFields(string[] fields)
        {
            foreach (string fieldName in fields)
                dbAdapter.addField(fieldName);
        }

        private List<T> qry(string _cmd)
        {
            List<T> qryResult = new List<T>();
            try
            {
                connect(_cmd);
                dbAdapter.open(_accessMode: StoreAdapter.AccessMode.Read);
                dbAdapter.execute();
                runExpensiveCode();
                int idx = 0;

                //Stopwatch stopWatch = new Stopwatch();
                //stopWatch.Start();

                while (dbAdapter.read() && idx < MAX_BATCH_READ)
                {
                    T rec = factory();
                    if (rec != null)
                        qryResult.Add(rec);
                    ++idx;
                }

                //stopWatch.Stop();
                //int sec = stopWatch.Elapsed.Seconds;
                //int ms = stopWatch.Elapsed.Milliseconds;

            }
            finally
            {
                closeConnection();
            }
            querySize = qryResult.Count;
            qryResult = qryCompleted(qryResult);
            dbAdapter.clearParams();
            return qryResult;
        }
        protected virtual string qryCmd()
        {
            setFields();
            return dbAdapter.qryCmd(this);
        }

        public virtual List<T> pageCompleted(List<T> _page)
        {
            return _page;
        }

        public virtual List<T> qryCompleted(List<T> _qry)
        {
            return _qry;
        }


        public virtual List<T> qry()
        {
            return qry(qryCmd());
        }
        public virtual List<T> qry(Filter _filter)
        {
            setFilter(_filter);
            return qry();
        }
        public virtual List<T> qry(Filter _filter, string _order)
        {
            setOrder(_order);
            setFilter(_filter);
            return qry();
        }

        public static List<T> extract(List<T> _list, PageData _page)
        {
            List<T> page = new List<T>();
            
            if (_list == null || _list.Count == 0)
                return page;

            int size = _list.Count;
            if (_page.startAt > size)
                return page;

            int pageEnd = _page.startAt + _page.pageSize;

            if (pageEnd > size)
                pageEnd = size;

            for (int idx = _page.startAt; idx < pageEnd; idx++)
                page.Add(_list[idx]);
            return page;
        }

        public List<T> pageFromList(List<T> _list, PageData _page)
        {
            querySize = _list.Count;
            List<T> page = extract(_list, _page);
            return page;
        }
        public virtual List<T> qryPage(PageData _page)
        {
            if (_page.sortField == null || _page.sortField == String.Empty)
                throw new Exception("DATA_PAGE_SORT_BY_EMPTY");

            setFilter(filter);
            setOrder(_page.sortField);

            List<T> list = qry();

            List<T> page = pageFromList(list, _page);

            page = pageCompleted(page);

            return page;
        }

        public List<T> qryPage(Filter _filter, PageData _page)
        {
            setFilter(_filter);
            return qryPage(_page);
        }

        public int viewCount()
        {
            qry();
            return querySize;
        }

        // -------------------------------------------------------------------------------------
        // get
        // -------------------------------------------------------------------------------------

        public virtual string sqlObjectForGet()
        {
            return getObj ?? dataObj;
        }

        protected virtual string getCmd()
        {
            return dbAdapter.getCmd(this);
        }

        protected abstract void setPrimaryKeys(object _key);

        public virtual T get(object _keys)
        {
            try
            {
                setPrimaryKeys(_keys);
                connect(getCmd());
                dbAdapter.open();
                dbAdapter.execute();
                if (dbAdapter.read())
                {
                    runExpensiveCode();
                    T value = factory();
                    getCompleted();
                    dbAdapter.clearParams();
                    return value;
                }
                dbAdapter.clearParams();
            }
            finally
            {
                closeConnection();
            }

            return default;
        }

    }

    public abstract class StoreManager<T> : ViewManager<T> where T : new()
    {
        protected string setObj = null;
        public TrxMode trxMode;      

        public StoreManager()
        {
        }
        public StoreManager(StoreAdapter _dbAdapter) : this()
        {
            dbAdapter = _dbAdapter;
        }
        ~StoreManager()
        {
            closeConnection();
        }

        // -------------------------------------------------------------------------------------
        // set
        // -------------------------------------------------------------------------------------

        public virtual string dataObject()
        {
            return dataObj;
        }

        public virtual string getObject()
        {
            return getObj ?? dataObj;
        }

        public virtual string setObject()
        {
            return setObj;
        }

        public virtual string setCmd(ref TrxMode trxMode)
        {
            return dbAdapter.setCmd(this, ref trxMode);
        }

        public virtual bool ruleCheck()
        {
            return true;
        }

        public virtual string set(T _object)
        {
            try
            {
                if (ruleCheck() == true)
                {
                    dbAdapter.clearParams();
                    setParams(_object);
                    TrxMode trxMode = TrxMode.INSERT;
                    string cmd = setCmd(ref trxMode);
                    if (cmd != null)
                    {
                        connect(cmd);
                        dbAdapter.open();
                        dbAdapter.execute();
                        return identityInsert(_object);
                    }
                }
            }
            finally
            {
                closeConnection();
            }
            return null;
        }

        public virtual bool setArray(List<T> _array)
        {
            try
            {
                foreach (T rec in _array)
                {
                    set(rec);
                }
            }
            finally
            {
                closeConnection();
            }

            return false;
        }

        public virtual bool delArray(List<T> _array)
        {
            try
            {
                foreach (T rec in _array)
                {
                    del(rec);
                }
            }
            finally
            {
                closeConnection();
            }

            return false;
        }


        // -------------------------------------------------------------------------------------
        // del
        // -------------------------------------------------------------------------------------
        
        //protected virtual string delCmd()
        //{
        //    return dbAdapter.delCmd(this);
        //}

        public virtual int del(T _object)
        {
            try
            {
                dbAdapter.clearParams();
                setPrimaryKeys(_object);
                connect(dbAdapter.delCmd(this));
                dbAdapter.open();
                dbAdapter.executeNonQuery();
                dbAdapter.clearParams();
            }
            finally
            {
                closeConnection();
            }

            return 1;
        }

        public void runBatch(string _sqlBatch)
        {
            dbAdapter.runBatch(_sqlBatch);
        }

        public virtual int run()
        {
            return 0;
        }

        public int recordCount(Filter f)
        {
            return recordCount(f.filterString());
        }

        public virtual int recordCount(string _filter)
        {
            string dataObjectName = getObject();
            if (dataObjectName == null)
                throw new Exception("DATA_OBJECT_NOT_SET");
            string sqlCmd = string.Format("select [qrySize] = count(*) from {0}", dataObjectName);

            if (_filter != string.Empty)
                sqlCmd += string.Format(" where {0}", _filter);

            StoreAdapter adapter = exec(sqlCmd);
            int? _ = (int)adapter.Object("qrySize");
            closeConnection();
            return _ == null ? 0 : (int)_;
        }

        public bool exists(string _filter = null)
        {
            int? _ = recordCount(_filter);
            return _ != null && _ > 0;
        }

        public bool exists()
        {
            string pkFilter = dbAdapter.primaryKeyFilter();
            if (pkFilter == null)
                return false;
            return exists(pkFilter);
        }

        protected void addPrimaryKey(string _parameterName, object _parameterValue, bool _isMandatory)
        {
            dbAdapter.addParameter(_parameterName, _parameterValue, _isMandatory, _isPrimaryKey: true);
        }
        protected void addParameter(string _parameterName, object _parameterValue, bool _isMandatory)
        {
            dbAdapter.addParameter(_parameterName, _parameterValue, _isMandatory, _isPrimaryKey : false);
        }
        protected abstract void setPrimaryKeys(T _object);
        protected abstract void setParams(T _object);


        protected virtual string identityInsert(T _object) 
        {
            return null;
        }

        protected virtual string primaryKey(T _object) { return string.Empty; }
        protected virtual string masterKey(T _object) { return string.Empty; }
    }
}