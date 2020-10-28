using System;
using System.Collections.Generic;

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

    //interface IStoreManager
    //{
    //    string primaryKey(StoreAdapter _adapter);
    //    void setParams(StoreAdapter _adapter);
    //    string primaryKeyFilter();
    //}

    public abstract class StoreManager<T>
    {
        private const int MAX_BATCH_READ = 2000000;

        public Filter filter;
        public int querySize;
        private string orderBy = null;
        public StoreAdapter dbAdapter;

        protected string dataObj = null;
        protected string setObj = null;
        protected string getObj = null;

        public TrxMode trxMode;

        public List<T> qryResult;


        public StoreManager()
        {
        }
        public StoreManager(StoreAdapter _dbAdapter)
        {
            dbAdapter = _dbAdapter;
        }
        ~StoreManager()
        {
            closeConnection();
        }
        protected virtual T factory()
        {
            return read(dbAdapter);
        }


        // -------------------------------------------------------------------------------------
        // connect
        // -------------------------------------------------------------------------------------
        public void connect(String _cmd)
        {
            dbAdapter.connect();
            dbAdapter.init(_cmd);
        }
        public void open()
        {
            dbAdapter.open();
        }
        public void closeConnection()
        {
            dbAdapter.close();
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
        public List<T> pageFromList(List<T> _list, PageData _page)
        {
            List<T> page = new List<T>();

            if (_list == null)
                return page;

            querySize = _list.Count;

            if (querySize == 0)
                return page;

            if (_page.startAt > querySize)
                return page;

            int pageEnd = _page.startAt + _page.pageSize;

            if (pageEnd > querySize)
                pageEnd = querySize;

            for (int idx = _page.startAt; idx < pageEnd; idx++)
                page.Add(_list[idx]);

            page = pageCompleted(page);

            return page;
        }
        public virtual List<T> qryPage(PageData _page)
        {
            if (_page.sortField == null || _page.sortField == String.Empty)
                throw new Exception("DATA_PAGE_SORT_BY_EMPTY");

            setFilter(filter);
            setOrder(_page.sortField);

            List<T> list = qry();

            return pageFromList(list, _page);
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
        // set
        // -------------------------------------------------------------------------------------
        public virtual string dataObject()
        {
            return dataObj;
        }
        public virtual string getObject()
        {
            return getObj == null ? dataObject() : getObj;
        }
        public virtual string setObject()
        {
            return setObj;
        }
        public virtual string setCmd()
        {
            return dbAdapter.setCmd(this, ref trxMode);
        }


        
        
        public virtual string set(T _record)
        {
            setRecord(_record);
            return set();
        }

        public virtual bool ruleCheck()
        {
            return true;
        }

        public virtual string set()
        {
            try
            {
                if (ruleCheck() == true)
                {
                    dbAdapter.clearParams();
                    setParams(dbAdapter);
                    string cmd = setCmd();
                    if (cmd != null)
                    {
                        connect(cmd);
                        dbAdapter.open();
                        if (dbAdapter.execute())
                        {
                            if (dbAdapter.read())
                                return primaryKey(dbAdapter);
                        }
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
        // get
        // -------------------------------------------------------------------------------------
        public virtual string sqlObjectForGet()
        {
            return getObj == null ? dataObj : getObj;
        }
        protected virtual string getCmd(string _primaryKeyFilter = null)
        {
            if (_primaryKeyFilter == null)
                _primaryKeyFilter = primaryKeyFilter();
            return dbAdapter.getCmd(this);
        }

        public virtual T get(T _rec)
        {
            setRecord(_rec);
            return get(primaryKeyFilter());
        }

        public virtual T get()
        {
            return get(primaryKeyFilter());
        }

        public virtual T get(string _primaryKeyFilter)
        {
            try
            {
                connect(getCmd(_primaryKeyFilter));
                {
                    dbAdapter.open();
                    dbAdapter.execute();
                    if (dbAdapter.read())
                    {
                        runExpensiveCode();
                        T value = factory();
                        getCompleted();
                        return value;
                    }
                }
            }
            finally
            {
                closeConnection();
            }

            return default(T);
        }

        // -------------------------------------------------------------------------------------
        // del
        // -------------------------------------------------------------------------------------
        protected virtual string delCmd()
        {
            return dbAdapter.delCmd(this);
        }
        public virtual int del(T _rec)
        {
            setRecord(_rec);
            return del();
        }
        public virtual int del()
        {
            try
            {
                connect(dbAdapter.delCmd(this));
                dbAdapter.open();
                dbAdapter.executeNonQuery();
            }
            finally
            {
                closeConnection();
            }

            return 1;
        }

        // -------------------------------------------------------------------------------------
        // exec
        // -------------------------------------------------------------------------------------

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
        public void runBatch(string _sqlBatch)
        {
            dbAdapter.runBatch(_sqlBatch);
        }
        public virtual int run()
        {
            return 0;
        }
        public virtual int recordCount(string _filter)
        {
            string obj = getObject();
            if (obj == null)
                throw new Exception("DATA_OBJECT_NOT_SET");
            string sqlCmd = string.Format("select [qrySize] = count(*) from {0}", obj);

            if (_filter == null)
                _filter = primaryKeyFilter();

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
            return _ == null ? false : _ > 0;
        }
        public bool exists()
        {
            string pkFilter = primaryKeyFilter();
            if (pkFilter == null)
                return false;
            return exists(pkFilter);
        }

        public abstract string primaryKeyFilter();

        protected virtual void setParams(StoreAdapter _adapter, T _object) {}
        protected virtual void read(StoreAdapter _adapter, T _object) {}
        protected virtual void setKeys(object _keys) {}
        protected virtual void runExpensiveCode() {}
        protected virtual void setFields() {}
        protected virtual void getCompleted() {}

        protected abstract string primaryKey(StoreAdapter _adapter);
        protected abstract void setParams(StoreAdapter _adapter);
        protected abstract T read(StoreAdapter _adapter);
        protected abstract void deserialize(string _json);
        protected abstract object recordInstance();
        protected abstract string masterKey();
        protected abstract void setRecord(T _record);

    }
}