﻿namespace WikiReader
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Data.SqlTypes;
    using System.IO;
    using System.Threading;

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
    internal class DatabasePump : IInsertableProgress
    {
        private static long previousPendingRevisions = -1;

        private long runID;

        public long RunID
        {
            get { return this.runID; }
        }

        /// <summary>
        /// WorkItemInfo provides context for an execution. Contains the SqlConnection
        /// and the reference to the Insertable that we're working.
        /// </summary>
        private class WorkItemInfo : IDisposable
        {
            public IInsertable _i;
            public IInsertable? _previous;
            public SqlConnection? _conn;
            public IInsertableProgress _progress;
            public DatabasePump _pump;

            public WorkItemInfo(DatabasePump pump, IInsertable i, IInsertable? previous, IInsertableProgress progress)
            {
                _pump = pump;
                _i = i;
                _previous = previous;
                _progress = progress;
            }

            public void Dispose()
            {
                if (_conn != null)
                {
                    _conn.Close();
                    _conn.Dispose();
                }
                _conn = null;
            }

            public void BuildConnection()
            {
                // get a new connection; use an ApplicationName parameter to indicate who we are
                string totalConnectionString = $"{ConnectionString};Application Name=WikiLoader{Environment.CurrentManagedThreadId};";

                _conn = null;
                try
                {
                    _conn = new SqlConnection(totalConnectionString);
                }
                catch (ArgumentException)
                {
                    // something wrong, so fall back to a safer string
                    Console.WriteLine($"Connection failed. Connection string = \"{totalConnectionString}\"");
                    _conn = new SqlConnection(ConnectionString);
                }

                for (int retries = 10; retries > 0; retries--)
                {
                    try
                    {
                        _conn.Open();
                        break;
                    }
                    catch (SqlException sex)
                    {
                        // connection exception?
                        if (sex.Number == 64 && sex.Source == ".Net SqlClient Data Provider")
                        {
                            Console.WriteLine($"{sex.Source}: {sex.Number}, {sex.Message}\nTrying again ({retries} retries left)");
                            Thread.Sleep(1000);
                            continue;
                        }

                        // don't know that exception yet, just rethrow
                        throw sex;
                    }
                }

                if (_conn.State != ConnectionState.Open)
                    throw new Exception("Couldn't connect");
            }
        }

        /// <summary>
        /// Our connection string. Just in one place here so we don't have to copy
        /// it everywhere.
        /// </summary>
        private static string ConnectionString
        {
            // uses integrated security to my server (named "burst"),
            // and a database named "Wikipedia".
            // Note that this turns ADO.NET connection pooling off;
            // another issue that might be better if we actually do some
            // connection pooling and/or use discrete threads instead of
            // the ThreadPool
            get { return "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=Wikipedia;Data Source=lake;"; }
            // get { return "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=Wikipedia;Data Source=lake;Pooling=false;"; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabasePump"/> class.
        /// </summary>
        public DatabasePump()
        {
        }

        /// <summary>
        /// Helper to test that the connection string works. This lets the app
        /// check that it won't just fail when database connections start.
        /// </summary>
        public static void TestConnection()
        {
            using SqlConnection sql = GetConnection();
        }

        public static SqlConnection GetConnection()
        {
            var sql = new SqlConnection(ConnectionString);
            sql.Open();
            return sql;
        }

        public void StartRun(string fileName)
        {
            using SqlConnection conn = GetConnection();

            using var insertRun = new SqlCommand(
                "INSERT INTO Run (HostName, ProcID, SourceFileName, SourceFileSize, SourceFileTimestamp, StartTime) " +
                "VALUES (@HostName, @ProcID, @SourceFileName, @SourceFileSize, @SourceFileTimestamp, GetUTCDate()); " +
                "SELECT SCOPE_IDENTITY();", conn);

            insertRun.Parameters.AddWithValue("@HostName",  Environment.MachineName);
            insertRun.Parameters.AddWithValue("@ProcID", Environment.ProcessId);
            insertRun.Parameters.AddWithValue("@SourceFileName", fileName);
            insertRun.Parameters.AddWithValue("@SourceFileTimestamp", File.GetLastWriteTimeUtc(fileName));
            insertRun.Parameters.AddWithValue("@SourceFileSize", new System.IO.FileInfo(fileName).Length);
            decimal d = (decimal) insertRun.ExecuteScalar();

            this.runID = (long)d;
        }

        public void CompleteRun(string strResult)
        {
            using var conn = GetConnection();

            using var updateRun = new SqlCommand(
                "UPDATE Run " +
                "   SET EndTime = GetUTCDate(), " +
                "       Result = @Result" + 
                "  WHERE RunID = @RunID;", conn);

            updateRun.Parameters.AddWithValue("@RunID", this.runID);
            if (strResult == null)
            {
                updateRun.Parameters.AddWithValue("@Result", DBNull.Value);
            }
            else
            {
                string str = strResult[..Math.Min(strResult.Length, 1024)];
                updateRun.Parameters.AddWithValue("@Result", str);
            }
            updateRun.ExecuteNonQuery();
        }

        public long StartActivity(string activityName, int? namespaceID, long? pageID, long? workCount)
        {
            using var conn = GetConnection();

            using var insertActivity = new SqlCommand(
                "INSERT INTO Activity (RunID, ThreadID, Activity, StartTime, TargetNamespace, TargetPageID, WorkCount) " +
                "VALUES (@RunID, @ThreadID, @Activity, GetUTCDate(), @TargetNamespace, @TargetPageID, @WorkCount); " +
                "SELECT SCOPE_IDENTITY();", conn);

            insertActivity.Parameters.AddWithValue("@RunID", this.runID);
            insertActivity.Parameters.AddWithValue("@ThreadID", Environment.CurrentManagedThreadId);
            insertActivity.Parameters.AddWithValue("@Activity", activityName);
            insertActivity.Parameters.AddWithValue("@TargetNamespace", namespaceID ?? SqlInt32.Null);
            insertActivity.Parameters.AddWithValue("@TargetPageID", pageID ?? SqlInt64.Null);
            insertActivity.Parameters.AddWithValue("@WorkCount", workCount ?? SqlInt64.Null);
            decimal d = (decimal)insertActivity.ExecuteScalar();

            long activityID = (long)d;

            return activityID;
        }

        public void CompleteActivity(long activityID, long? completedCount, string? result)
        {
            using var conn = GetConnection();

            using var completeActivity = new SqlCommand(
                "UPDATE Activity " +
                "   SET EndTime = GetUTCDate(), " +
                "       CompletedCount = @CompletedCount, " +
                "       Result = @Result, " +
                "       DurationMillis = DATEDIFF(MILLISECOND, StartTime, GetUTCDate() ) " +
                " WHERE ActivityID = @ActivityID AND RunID = @RunID;", conn);

            completeActivity.Parameters.AddWithValue("@ActivityID", activityID);
            completeActivity.Parameters.AddWithValue("@RunID", this.runID);
            completeActivity.Parameters.AddWithValue("@CompletedCount", completedCount ?? SqlInt64.Null);
            completeActivity.CommandTimeout = 300;

            if (result == null)
            {
                completeActivity.Parameters.AddWithValue("@Result", DBNull.Value);
            }
            else
            {
                string str = result[..Math.Min(result.Length, 1024)];
                completeActivity.Parameters.AddWithValue("@Result", str);
            }

            completeActivity.ExecuteNonQuery();

            return;
        }

        /// <summary>
        /// Add an Insertable that is ready to go to the database.
        /// </summary>
        /// <param name="i">reference to an object implementing IInsertable; we'll enqueue it and add it whne we have threads.</param>
        /// <param name="previous">reference to an object using IInsertable for the previously executed item.</param>
        /// <returns>a tuple with the number of running, queued, and pending revisions</returns>
        public (long running, long queued, long pendingRevisions) Enqueue(IInsertable i, IInsertable? previous)
        {
            // backpressure
            int pauses = 0;
            while (Interlocked.Read(ref _running) >= 5 || Interlocked.Read(ref _queued) >= 100)
            {
                // report every 10 pauses == 1 second
                if (pauses++ % 10 == 0)
                {
                    string main = $"Backpressure: {Interlocked.Read(ref _running)} running, {Interlocked.Read(ref _queued)} queued, {_pendingRevisions} pending revisions";
                    if (previousPendingRevisions != -1)
                    {
                        long delta = _pendingRevisions - previousPendingRevisions;
                        Console.WriteLine($"{main} ({delta:+#;-#;0})");
                    }
                    else
                        Console.WriteLine(main);

                    previousPendingRevisions = _pendingRevisions;
                }
                Thread.Sleep(100);  // 100 milliseconds
            }

            // create a WorkItemInfo instance with the Insertable and our connection
            // disposable connection object is now owned by WorkItemInfo object
            WorkItemInfo ci = new(this, i, previous, this);

            // queue it up! 
            Interlocked.Increment(ref _queued);
            ThreadPool.QueueUserWorkItem(new WaitCallback(WorkCallback), ci);

            // return our current running count. This might
            // not have incremented just yet, but this is a convenient 
            // time to help show status
            return (_running, _queued, _pendingRevisions);
        }

        private static long _running = 0;
        private static long _queued = 0;

        /// <summary>
        /// Callback for the thread to do work. Does some
        /// accounting, and then makes the call.
        /// </summary>
        /// <param name="obj">WorkItemInfo object to work</param>
        private static void WorkCallback(object? obj)
        {
            if (obj == null)
                throw new InvalidOperationException("thread work item can't be null");

            // cast the generic object to our WorkItemInfo
            using WorkItemInfo ci = (WorkItemInfo)obj;

            // remove from the queued count
            Interlocked.Decrement(ref _queued);

            // add to the running count
            Interlocked.Increment(ref _running);

            try
            {
                // go work the insertion
                ci.BuildConnection();
                if (ci._conn == null)
                    throw new Exception("Couldn't connect");
                ci._i.Insert(ci._previous, ci._pump, ci._conn, ci._progress);
            }
            finally
            {
                // decrement our running count
                Interlocked.Decrement(ref _running);
            }
        }

        public void WaitForComplete()
        {
            while (_running > 0)
            {
                Console.WriteLine($"{_running} still running, {_queued} queued, {_pendingRevisions} pending revisions");
                Thread.Sleep(1000);
            }
            return;
        }

        public int InsertedPages()
        {
            return _insertedPages;
        }

        /// <summary>
        /// implementation of InsertableProgress.
        /// </summary>
        private int _pendingRevisions = 0;
        private int _insertedPages = 0;
        private int _insertedUsers = 0;
        private int _insertedRevisions = 0;

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

