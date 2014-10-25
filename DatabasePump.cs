using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Collections.Concurrent;
using System.Threading;

namespace WikiReader
{
    class DatabasePump
    {
        private class CallerInfo
        {
            public Insertable _i;
            public SqlConnection _conn;

            public CallerInfo(SqlConnection conn, Insertable i)
            {
                _conn = conn;
                _i = i;
            }
        }
        
        BlockingCollection<Insertable> _queue = new BlockingCollection<Insertable>(20);

        String ConnectionString
        {
            get { return "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=Wikipedia;Data Source=burst"; }
        }

        public DatabasePump()
        {
        }

        public void TestConnection()
        {
            SqlConnection sql = new SqlConnection(ConnectionString);
            sql.Open();
            sql.Close();
        }

        public void Enqueue(Insertable i)
        {
            SqlConnection conn = new SqlConnection(ConnectionString);
            conn.Open();
            CallerInfo ci = new CallerInfo(conn, i);
            ThreadPool.QueueUserWorkItem(new WaitCallback(caller), ci);
            // conn.Close();
        }

        private static void caller(Object obj)
        {
            CallerInfo ci = (CallerInfo)obj;
            ci._i.Insert(ci._conn);
            ci._conn.Close();
        }
    }
}
