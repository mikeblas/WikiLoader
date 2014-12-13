using System;
using System.Collections.Generic;
using System.Data;
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
    class DatabasePump : InsertableProgress
    {
        /// <summary>
        /// CallerInfo provides context for an execution. Contains the SqlConnection
        /// and the reference to the Insertable that we're working.
        /// </summary>
        private class CallerInfo
        {
            public Insertable _i;
            public SqlConnection _conn;
            public InsertableProgress _progress;

            public CallerInfo(SqlConnection conn, Insertable i, InsertableProgress progress)
            {
                _conn = conn;
                _i = i;
                _progress = progress;
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
        public void Enqueue(Insertable i, ref int running, ref int queued, ref int pendingRevisions )
        {
            // backpressure
            while (_running >= 5 || _queued >= 100)
            {
                System.Console.WriteLine("Backpressure: {0} running, {1} queued, {2} pending revisions", _running, _queued, _pendingRevisions);
                Thread.Sleep(1000);
            }

            // get a new connection
            // use the object name as the "application name",
            // but that name can't have semicolons and can't
            // be more than 128 characters. We also strip equals
            // signs to reduce attack surface area.
            String appNameRaw = i.ObjectName.Replace(";", "").Replace("=", "");
            String appNameTrimmed = appNameRaw.Substring(0, Math.Min(appNameRaw.Length, 100));
            SqlConnection conn = new SqlConnection(ConnectionString + "Application Name=" + appNameTrimmed + ";");

            for (int retries = 10; retries > 0; retries--)
            {
                try
                {
                    conn.Open();    
                    break;
                }
                catch (SqlException sex)
                {
                    // connection exception?
                    if (sex.Number == 64 && sex.Source == ".Net SqlClient Data Provider")
                    {
                        System.Console.WriteLine("{0}: {1}, {2}\nTrying again ({3} retries left)",
                            sex.Source, sex.Number, sex.Message, retries);
                        Thread.Sleep(1000);
                        continue;
                    }

                    // don't know that exception yet, just rethrow
                    throw sex;
                }
            }

            if (conn.State != ConnectionState.Open)
            {
                throw new Exception("Couldn't connect");
            }

            // create a CallerInfo instance with the Insertable and our connection
            CallerInfo ci = new CallerInfo(conn, i, this);

            // queue it up! 
            Interlocked.Increment(ref _queued);
            ThreadPool.QueueUserWorkItem(new WaitCallback(caller), ci);

            // return our current running count. This might
            // not have incremented just yet, but this is a convenient 
            // time to help show status
            running = _running;
            queued = _queued;
            pendingRevisions = _pendingRevisions;
        }

        private static volatile int _running = 0;
        private static volatile int _queued = 0;

        /// <summary>
        /// Callback for the thread to do work. Does some
        /// accounting, and then makes the call.
        /// </summary>
        /// <param name="obj">CallerInfo object to work</param>
        private static void caller(Object obj)
        {
            // cast the generic object to our CallerInfo
            CallerInfo ci = (CallerInfo)obj;

            // remove from the queued count
            Interlocked.Decrement(ref _queued);

            // add to the running count
            Interlocked.Increment( ref _running );


            try
            {
                // go work the insertion
                ci._i.Insert(ci._conn, ci._progress);
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
                System.Console.WriteLine("{0} still running, {1} pending revisions", _running, _pendingRevisions);
                Thread.Sleep(1000);
            }
            return;
        }

        public int getInsertedPages()
        {
            return _insertedPages;
        }

        /// <summary>
        /// implementation of InsertableProgress
        /// </summary>
        int _pendingRevisions = 0;
        int _insertedPages = 0;
        int _insertedUsers = 0;
        int _insertedRevisions = 0;
        public void AddPendingRevisions(int count)
        {
            Interlocked.Add(ref _pendingRevisions, count);
        }

        public void CompleteRevisions(int count)
        {
            Interlocked.Add( ref _pendingRevisions, -count);
        }


        public void InsertedPages(int count)
        {
            Interlocked.Add(ref _insertedPages, count);
        }

        public void InsertedUsers(int count)
        {
            Interlocked.Add(ref _insertedUsers, count);
        }

        public void InsertedRevisions(int count)
        {
            Interlocked.Add(ref _insertedRevisions, count);
        }
    }
}

