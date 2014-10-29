using System;
using System.Collections.Generic;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Collections.Concurrent;
using System.Threading;

namespace WikiReader
{
    /// <summary>
    /// The DatabasePump takes work items that implement the Insertable interface.
    /// As threads are available, it calls Insertable's methods to manage the items
    /// and have them insert themselves into the database.
    /// 
    /// Kind of coarse right now; it just uses the ThreadPool, which leaves no
    /// backpressure. It might be interesting to add backpressure as a way to 
    /// limit the load on the target database server, or to govern mmeory
    /// consumption.
    /// 
    /// Only one instance can exist because of some shared state in static members.
    /// Need to add some singleton pattern implementation so that it is protected.
    /// </summary>
    class DatabasePump
    {
        /// <summary>
        /// CallerInfo provides context for an execution. Contains the SqlConnection
        /// and the reference to the Insertable that we're working.
        /// </summary>
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

        /// <summary>
        /// Our connection string. Just in one place here so we don't have to copy
        /// it everywhere.
        /// </summary>
        private String ConnectionString
        {
            // uses integrated security to my server (named "burst"),
            // and a database named "Wikipedia".
            // Note that this turns ADO.NET connection pooling off;
            // another issue that might be better if we actually do some
            // connection pooling and/or use discrete threads instead of
            // the ThreadPool
            get { return "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=Wikipedia;Data Source=burst;Pooling=false;"; }
        }

        /// <summary>
        /// Create a DatabasePump instance
        /// </summary>
        public DatabasePump()
        {
        }

        /// <summary>
        /// Helper to test that the connection string works. This lets the app
        /// check that it won't just fail when database connections start.
        /// </summary>
        public void TestConnection()
        {
            SqlConnection sql = new SqlConnection(ConnectionString);
            sql.Open();
            sql.Close();
            sql.Dispose();
        }

        /// <summary>
        /// Add an Insertable that is ready to go to the database.
        /// </summary>
        /// <param name="i">reference to an object implementing Insertable; we'll enqueue it and add it whne we have threads</param>
        public void Enqueue(Insertable i)
        {
            // get a new connection
            SqlConnection conn = new SqlConnection(ConnectionString);
            conn.Open();

            // create a CallerInfo instance with the Insertable and our connection
            CallerInfo ci = new CallerInfo(conn, i);

            // queue it up! 
            ThreadPool.QueueUserWorkItem(new WaitCallback(caller), ci);

            // print out our current running count. This might not
            // have incremented just yet, but this is a convenient 
            // time to show status
            System.Console.WriteLine("{0} running", _running);
        }

        private static int _running = 0;

        /// <summary>
        /// Callback for the thread to do work. Does some
        /// accounting, and then makes the call.
        /// </summary>
        /// <param name="obj">CallerInfo object to work</param>
        private static void caller(Object obj)
        {
            // cast the generic object to our CallerInfo
            CallerInfo ci = (CallerInfo)obj;

            // add to the running count
            Interlocked.Increment( ref _running );

            try
            {
                // go work the insertion
                ci._i.Insert(ci._conn);
            }
            finally
            {
                // clean up the connection
                // and decrement our running count
                ci._conn.Close();
                ci._conn.Dispose();
                Interlocked.Decrement(ref _running);
            }
        }

        public void WaitForComplete()
        {
            while (_running > 0)
            {
                System.Console.WriteLine("{0} still running", _running);
                Thread.Sleep(1000);
            }
            return;
        }
    }
}
