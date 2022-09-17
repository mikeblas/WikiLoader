namespace WikiLoaderEngine
{
    using System;
    using System.Collections.Generic;
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
    /// backpressure, so we implement our own.
    ///
    /// Only one instance can exist because of some shared state in static members.
    /// Need to add some singleton pattern implementation so that it is protected.
    /// </summary>
    public class DatabasePump : IInsertableProgress
    {
        private static long previousPendingRevisionCount = -1;

        private static long runningCount = 0;
        private static long queuedCount = 0;

        private readonly HashSet<WorkItemInfo> runningSet = new ();

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
            private IInsertable insertable;
            private IInsertable? _previous;
            private SqlConnection? _conn;
            private IInsertableProgress _progress;
            private DatabasePump _pump;
            private IXmlDumpParserProgress parserProgress;

            public WorkItemInfo(DatabasePump pump, IInsertable i, IInsertable? previous, IInsertableProgress progress, IXmlDumpParserProgress parserProgress)
            {
                this._pump = pump;
                this.insertable = i;
                this._previous = previous;
                this._progress = progress;
                this.parserProgress = parserProgress;
            }

            public void Dispose()
            {
                if (this._conn != null)
                {
                    this._conn.Close();
                    this._conn.Dispose();
                }

                this._conn = null;
            }

            internal void Insert()
            {
                insertable.Insert(this._previous, this._pump, this._conn, this._progress, this.parserProgress);
            }

            internal bool BuildConnection()
            {
                // get a new connection; use an ApplicationName parameter to indicate who we are
                string totalConnectionString = $"{ConnectionString};Application Name=WikiLoader{Environment.CurrentManagedThreadId};";

                this._conn = null;
                try
                {
                    this._conn = new SqlConnection(totalConnectionString);
                }
                catch (ArgumentException)
                {
                    // something wrong, so fall back to a safer string
                    Console.WriteLine($"Connection failed. Connection string = \"{totalConnectionString}\"");
                    this._conn = new SqlConnection(ConnectionString);
                }

                for (int retries = 10; retries > 0; retries--)
                {
                    try
                    {
                        this._conn.Open();
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

                return this._conn.State == ConnectionState.Open;
            }

            internal string ObjectName
            {
                get { return this.insertable.ObjectName; }
            }

            internal int RevisionCount
            {
                get { return this.insertable.RevisionCount; }
            }

            internal int RemainingRevisionCount
            {
                get { return this.insertable.RemainingRevisionCount; }
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

        public long DetermineSkipPosition(string filename)
        {
            using var conn = GetConnection();

            using var selectRun = new SqlCommand(
                "  SELECT TOP 1 RunID, EndTime, Result " +
                "    FROM [Run] " +
                "   WHERE SourceFileName = @SourceFileName " +
                "     AND RunID != @ThisRunID " +
                "ORDER BY StartTime DESC", conn);

            selectRun.Parameters.AddWithValue("@SourceFileName", filename);
            selectRun.Parameters.AddWithValue("@ThisRunID", this.runID);

            long previousRunID;
            using (var reader = selectRun.ExecuteReader())
            {
                if (!reader.Read())
                {
                    // no record, so never did this file before.
                    return 0;
                }

                string? result = null;
                if (reader["Result"] != DBNull.Value)
                    result = (string)reader["Result"];

                if (result != null && result.StartsWith("Success"))
                {
                    // it's there, but it ended
                    throw new InvalidExpressionException($"Already ran file {filename} to completion at {reader["EndTime"]}");
                }

                // it didn't finish; get the RunID
                previousRunID = (long)reader["RunID"];
            }

            // with the RunID, query the progress

            using var selectProgress = new SqlCommand(
                "SELECT FilePosition, ReportTime from RunProgress WHERE RunID = @RunID", conn);
            selectProgress.Parameters.AddWithValue("@RunID", previousRunID);

            using (var reader = selectProgress.ExecuteReader())
            {
                if (!reader.Read())
                {
                    // wierd there's no record, but our only choice is to start over
                    Console.WriteLine($"No progress found for previous run {previousRunID}");
                    return 0;
                }

                long position = (long)reader["FilePosition"];
                DateTime reportTime = (DateTime)reader["ReportTime"];
                Console.WriteLine($"Found progress to byte {position} at {reportTime}");

                return position;
            }
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
            decimal d = (decimal)insertRun.ExecuteScalar();

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

        /// <summary>
        /// Mark an activity as complete in the Activity table.
        /// </summary>
        /// <param name="activityID">ID of the previously created activity to complete.</param>
        /// <param name="completedCount">Number of work items completed; null if not countable.</param>
        /// <param name="result">Error encountered; null if none.</param>
        public void CompleteActivity(long activityID, long? completedCount, string? result)
        {
            using var conn = GetConnection();

            using var completeActivity = new SqlCommand(
                "UPDATE Activity " +
                "   SET EndTime = GetUTCDate(), " +
                "       CompletedCount = @CompletedCount, " +
                "       Result = @Error, " +
                "       DurationMillis = DATEDIFF(MILLISECOND, StartTime, GetUTCDate() ) " +
                " WHERE ActivityID = @ActivityID AND RunID = @RunID;", conn);

            completeActivity.Parameters.AddWithValue("@ActivityID", activityID);
            completeActivity.Parameters.AddWithValue("@RunID", this.runID);
            completeActivity.Parameters.AddWithValue("@CompletedCount", completedCount ?? SqlInt64.Null);
            completeActivity.CommandTimeout = 300;

            if (result == null)
            {
                completeActivity.Parameters.AddWithValue("@Error", DBNull.Value);
            }
            else
            {
                string str = result[..Math.Min(result.Length, 1024)];
                completeActivity.Parameters.AddWithValue("@Error", str);
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
        public (long running, long queued, long pendingRevisions) Enqueue(IInsertable i, IInsertable? previous, IXmlDumpParserProgress parserProgress)
        {
            // backpressure
            int pauses = 0;
            while (Interlocked.Read(ref runningCount) >= 5 || Interlocked.Read(ref queuedCount) >= 100)
            {
                // report every 10 pauses == 1 second
                if (pauses++ % 10 == 0)
                {
                    string main = $"Backpressure: {Interlocked.Read(ref runningCount)} running, {Interlocked.Read(ref queuedCount)} queued, {this.pendingRevisions} pending revisions";
                    if (previousPendingRevisionCount != -1)
                    {
                        long delta = this.pendingRevisions - previousPendingRevisionCount;
                        main = $"{main} ({delta:+#;-#;0})";
                    }

                    main += "\n";

                    lock (this.runningSet)
                    {
                        foreach (var wii in this.runningSet)
                        {
                            main += $"   {wii.ObjectName}, {wii.RemainingRevisionCount} / {wii.RevisionCount}\n";
                        }
                    }

                    Console.Write(main);

                    previousPendingRevisionCount = this.pendingRevisions;
                }

                Thread.Sleep(100);  // 100 milliseconds
            }

            // create a WorkItemInfo instance with the Insertable and our connection
            // disposable connection object is now owned by WorkItemInfo object
            WorkItemInfo ci = new (this, i, previous, this, parserProgress);

            // queue it up!
            Interlocked.Increment(ref queuedCount);
            ThreadPool.QueueUserWorkItem(new WaitCallback(this.WorkCallback), ci);

            // return our current running count. This might
            // not have incremented just yet, but this is a convenient
            // time to help show status
            return (runningCount, queuedCount, this.pendingRevisions);
        }

        /// <summary>
        /// Callback for the thread to do work. Does some
        /// accounting, and then makes the call.
        /// </summary>
        /// <param name="obj">WorkItemInfo object to work.</param>
        private void WorkCallback(object? obj)
        {
            if (obj == null)
                throw new InvalidOperationException("thread work item can't be null");

            // cast the generic object to our WorkItemInfo
            using WorkItemInfo wii = (WorkItemInfo)obj;

            // remove from the queued count
            Interlocked.Decrement(ref queuedCount);

            // add to the running count
            Interlocked.Increment(ref runningCount);

            lock (this.runningSet)
            {
                this.runningSet.Add(wii);
            }

            try
            {
                // go work the insertion
                if (!wii.BuildConnection())
                    throw new Exception("Couldn't connect");
                wii.Insert();
            }
            finally
            {
                // decrement our running count
                Interlocked.Decrement(ref runningCount);

                // remove from the set of running
                lock (this.runningSet)
                {
                    this.runningSet.Remove(wii);
                 }
            }
        }

        public void WaitForComplete()
        {
            while (runningCount > 0)
            {
                Console.WriteLine($"{runningCount} still running, {queuedCount} queued, {this.pendingRevisions} pending revisions");
                Thread.Sleep(1000);
            }

            return;
        }

        public int InsertedPages()
        {
            return this.insertedPages;
        }

        /// <summary>
        /// implementation of IInsertableProgress.
        /// </summary>
        private int pendingRevisions = 0;
        private int insertedPages = 0;
        private int insertedUsers = 0;
        private int insertedRevisions = 0;

        public void AddPendingRevisions(int count)
        {
            Interlocked.Add(ref this.pendingRevisions, count);
        }

        public void CompleteRevisions(int count)
        {
            Interlocked.Add(ref this.pendingRevisions, -count);
        }


        public void InsertedPages(int count)
        {
            Interlocked.Add(ref this.insertedPages, count);
        }

        public void InsertedUsers(int count)
        {
            Interlocked.Add(ref this.insertedUsers, count);
        }

        public void InsertedRevisions(int count)
        {
            Interlocked.Add(ref this.insertedRevisions, count);
        }
    }
}
