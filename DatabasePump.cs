using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Data.Sql;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using System.Collections.Concurrent;
using System.Threading;
using System.IO;

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
        long _runID;


        public long RunID
        {
            get { return _runID; }
        }
        
        /// <summary>
        /// CallerInfo provides context for an execution. Contains the SqlConnection
        /// and the reference to the Insertable that we're working.
        /// </summary>
        private class CallerInfo
        {
            public Insertable _i;
            public Insertable _previous;
            public SqlConnection _conn;
            public InsertableProgress _progress;
            public DatabasePump _pump;

            public CallerInfo(DatabasePump pump, SqlConnection conn, Insertable i, Insertable previous, InsertableProgress progress)
            {
                _pump = pump;
                _conn = conn;
                _i = i;
                _previous = previous;
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
            get { return "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=Wikipedia;Data Source=lake;"; }
            // get { return "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=Wikipedia;Data Source=lake;Pooling=false;"; }
            // get { return "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=Wikipedia2;Data Source=lake;"; }
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
            SqlConnection sql = GetConnection();
            sql.Close();
            sql.Dispose();
        }

        public SqlConnection GetConnection()
        {
            SqlConnection sql = new SqlConnection(ConnectionString);
            sql.Open();
            return sql;
        }

        public void StartRun(String fileName)
        {
            SqlConnection conn = GetConnection();

            SqlCommand insertRun = new SqlCommand(
                "INSERT INTO Run (HostName, ProcID, SourceFileName, SourceFileSize, SourceFileTimestamp, StartTime) " +
                "VALUES (@HostName, @ProcID, @SourceFileName, @SourceFileSize, @SourceFileTimestamp, GetUTCDate()); " +
                "SELECT SCOPE_IDENTITY();", conn);

            insertRun.Parameters.AddWithValue("@HostName",  Environment.MachineName);
            insertRun.Parameters.AddWithValue("@ProcID", Process.GetCurrentProcess().Id);
            insertRun.Parameters.AddWithValue("@SourceFileName", fileName);
            insertRun.Parameters.AddWithValue("@SourceFileTimestamp", File.GetLastWriteTimeUtc(fileName));
            insertRun.Parameters.AddWithValue("@SourceFileSize", new System.IO.FileInfo(fileName).Length);
            Decimal d = (Decimal) insertRun.ExecuteScalar();

            _runID = (long)d;

            conn.Close();
            conn.Dispose();
        }

        public void CompleteRun(string strResult)
        {
            SqlConnection conn = GetConnection();

            SqlCommand updateRun = new SqlCommand(
                "UPDATE Run " +
                "   SET EndTime = GetUTCDate(), " +
                "       Result = @Result" + 
                "  WHERE RunID = @RunID;", conn);

            updateRun.Parameters.AddWithValue("@RunID", _runID);
            if (strResult == null)
            {
                updateRun.Parameters.AddWithValue("@Result", DBNull.Value);
            }
            else
            {
                String str = strResult.Substring(0, Math.Min(strResult.Length, 1024));
                updateRun.Parameters.AddWithValue("@Result", str);
            }
            updateRun.ExecuteNonQuery();

            conn.Close();
            conn.Dispose();
        }

        public long StartActivity(String activityName, int? namespaceID, long? pageID, long? workCount)
        {
            SqlConnection conn = GetConnection();

            SqlCommand insertActivity = new SqlCommand(
                "INSERT INTO Activity (RunID, ThreadID, Activity, StartTime, TargetNamespace, TargetPageID, WorkCount) " +
                "VALUES (@RunID, @ThreadID, @Activity, GetUTCDate(), @TargetNamespace, @TargetPageID, @WorkCount); " +
                "SELECT SCOPE_IDENTITY();", conn);

            insertActivity.Parameters.AddWithValue("@RunID", _runID);
            insertActivity.Parameters.AddWithValue("@ThreadID", Thread.CurrentThread.ManagedThreadId);
            insertActivity.Parameters.AddWithValue("@Activity", activityName);
            insertActivity.Parameters.AddWithValue("@TargetNamespace", namespaceID ?? SqlInt32.Null);
            insertActivity.Parameters.AddWithValue("@TargetPageID", pageID ?? SqlInt64.Null);
            insertActivity.Parameters.AddWithValue("@WorkCount", workCount ?? SqlInt64.Null);
            Decimal d = (Decimal)insertActivity.ExecuteScalar();

            long activityID = (long)d;
            conn.Close();
            conn.Dispose();

            return activityID;
        }

        public void CompleteActivity(long activityID, long? completedCount, string result)
        {
            SqlConnection conn = GetConnection();

            SqlCommand completeActivity = new SqlCommand(
                "UPDATE Activity " +
                "   SET EndTime = GetUTCDate(), " +
                "       CompletedCount = @CompletedCount, " +
                "       Result = @Result, " +
                "       DurationMillis = DATEDIFF(MILLISECOND, StartTime, GetUTCDate() ) " +
                " WHERE ActivityID = @ActivityID AND RunID = @RunID;", conn);

            completeActivity.Parameters.AddWithValue("@ActivityID", activityID);
            completeActivity.Parameters.AddWithValue("@RunID", _runID);
            completeActivity.Parameters.AddWithValue("@CompletedCount", completedCount ?? SqlInt64.Null);

            if ( result == null ) 
            {
                completeActivity.Parameters.AddWithValue("@Result", DBNull.Value);
            }
            else
            {
                String str = result.Substring(0, Math.Min(result.Length, 1024));
                completeActivity.Parameters.AddWithValue("@Result", str);
            }

            completeActivity.ExecuteNonQuery();
            conn.Close();
            conn.Dispose();

            return;
        }

        /// <summary>
        /// Add an Insertable that is ready to go to the database.
        /// </summary>
        /// <param name="i">reference to an object implementing Insertable; we'll enqueue it and add it whne we have threads</param>
        public void Enqueue(Insertable i, Insertable previous, ref long running, ref long queued, ref long pendingRevisions )
        {
            // backpressure
            int pauses = 0;
            while (Interlocked.Read(ref _running) >= 5 || Interlocked.Read(ref _queued) >= 100)
            {
                if (pauses++ % 10 == 0)
                {
                    System.Console.WriteLine("Backpressure: {0} running, {1} queued, {2} pending revisions",
                        Interlocked.Read(ref _running),
                        Interlocked.Read(ref _queued),
                        _pendingRevisions);
                }
                Thread.Sleep(100);
            }

            // get a new connection
            // use the object name as the "application name",
            // but that name can't have semicolons and can't
            // be more than 128 characters. We also strip equals
            // signs to reduce attack surface area. Can't have
            // quotes or apostrophies, either.
            String appNameRaw = i.ObjectName.Replace(";", "").Replace("=", "").Replace("\"", "").Replace("'", "");
            String appNameTrimmed = appNameRaw.Substring(0, Math.Min(appNameRaw.Length, 100));
            String totalConnectionString = ConnectionString + "Application Name=" + appNameTrimmed + ";";

            SqlConnection conn = null;
            try
            {
                conn = new SqlConnection(totalConnectionString);
            }
            catch (ArgumentException)
            {
                // something wrong, so fall back to a safer string
                System.Console.WriteLine("Connection failed. Connection string = \"{0}\"", totalConnectionString);
                conn = new SqlConnection(ConnectionString);
            }
           
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
            CallerInfo ci = new CallerInfo(this, conn, i, previous, this);

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

        private static long _running = 0;
        private static long _queued = 0;

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
                ci._i.Insert(ci._previous, ci._pump, ci._conn, ci._progress);
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
                System.Console.WriteLine("{0} still running, {1} queued, {2} pending revisions", _running, _queued, _pendingRevisions);
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

