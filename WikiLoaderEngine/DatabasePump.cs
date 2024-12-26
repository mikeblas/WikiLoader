namespace WikiLoaderEngine
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Data.SqlTypes;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;

    /// <summary>
    /// The DatabasePump takes work items that implement the Insertable interface.
    /// As threads are available, it calls Insertable's methods to manage the items
    /// and have them insert themselves into the database.
    ///
    /// Kind of coarse right now; it just uses the ThreadPool, which leaves no
    /// back pressure, so we implement our own.
    ///
    /// Only one instance can exist because of some shared state in static members.
    /// Need to add some singleton pattern implementation so that it is protected.
    /// </summary>
    public class DatabasePump : IInsertableProgress
    {
        private readonly ConcurrentDictionary<IWorkItemDescription, bool> runningSet = new ();

        // number of work items currently running
        private long runningWorkItemCount = 0;

        // number of work items queued
        private long queuedWorkItemCount = 0;

        /// <summary>
        /// implementation of IInsertableProgress.
        /// </summary>
        private int pendingRevisions = 0;
        private int insertedPages = 0;
        private int insertedUsers = 0;
        private int insertedRevisions = 0;

        private long runID;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabasePump"/> class.
        /// </summary>
        public DatabasePump()
        {
        }


        public long RunID
        {
            get { return this.runID; }
        }

        /// <summary>
        /// WorkItemInfo provides context for an execution. Contains the SqlConnection
        /// and the reference to the Insertable that we're working.
        /// </summary>
        private class WorkItemInfo : IDisposable, IWorkItemDescription
        {
            private IInsertable insertable;
            private IInsertable? previous;
            private SqlConnection? conn;
            private IInsertableProgress progress;
            private DatabasePump pump;
            private IXmlDumpParserProgress parserProgress;

            public WorkItemInfo(DatabasePump pump, IInsertable i, IInsertable? previous, IInsertableProgress progress, IXmlDumpParserProgress parserProgress)
            {
                this.pump = pump;
                this.insertable = i;
                this.previous = previous;
                this.progress = progress;
                this.parserProgress = parserProgress;
            }

            public void Dispose()
            {
                if (this.conn != null)
                {
                    this.conn.Close();
                    this.conn.Dispose();
                }

                this.conn = null;
            }

            internal void Insert()
            {
                if (conn == null)
                    throw new InvalidOperationException("Can't insert without a connection");

                insertable.Insert(this.previous, this.pump, this.conn, this.progress, this.parserProgress);
            }

            internal bool BuildConnection()
            {
                Stopwatch connectionTime = Stopwatch.StartNew();

                // get a new connection; use an ApplicationName parameter to indicate who we are
                string totalConnectionString = $"{ConnectionString};Application Name=WikiLoader{Environment.CurrentManagedThreadId};";

                this.conn = null;
                try
                {
                    this.conn = new SqlConnection(totalConnectionString);
                }
                catch (ArgumentException)
                {
                    // something wrong, so fall back to a safer string
                    Console.WriteLine($"Connection failed. Connection string = \"{totalConnectionString}\"");
                    this.conn = new SqlConnection(ConnectionString);
                }

                for (int retries = 10; retries > 0; retries--)
                {
                    try
                    {
                        this.conn.Open();
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

                connectionTime.Stop();
                // Console.WriteLine($"took {connectionTime.ElapsedMilliseconds} ms to connect");
                return this.conn.State == ConnectionState.Open;
            }

            public string ObjectName
            {
                get { return this.insertable.ObjectName; }
            }

            public int RevisionCount
            {
                get { return this.insertable.RevisionCount; }
            }

            public int RemainingRevisionCount
            {
                get { return this.insertable.RemainingRevisionCount; }
            }
        }

        /// <summary>
        /// Gets our connection string. Just in one place here so we don't have to copy it everywhere.
        /// </summary>
        private static string ConnectionString
        {
            // uses integrated security to my server (named "burst"),
            // and a database named "Wikipedia".
            // Note that this turns ADO.NET connection pooling off;
            // another issue that might be better if we actually do some
            // connection pooling and/or use discrete threads instead of
            // the ThreadPool
            get { return "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=Wikipedia;Data Source=circle;"; }
            // get { return "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=Wikipedia;Data Source=slide;Pooling=false;"; }
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
            return -1;

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
            if (activityID == -1)
                return;

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
        /// <param name="i">reference to an object implementing IInsertable; we'll enqueue it and add it when we have threads.</param>
        /// <param name="previous">reference to an object using IInsertable for the previously executed item.</param>
        /// <param name="parserProgress">IXmlDumpParserProgress that receives notifications of parsing progress.</param>
        /// <returns>a tuple with the number of running, queued, and pending revisions.</returns>
        public (long running, long queued, long pendingRevisions) Enqueue(IInsertable i, IInsertable? previous, IXmlDumpParserProgress parserProgress)
        {
            // back pressure
            int pauses = 0;
            while (Interlocked.Read(ref runningWorkItemCount) >= 32 && Interlocked.Read(ref queuedWorkItemCount) >= 640)
            {
                // report every 50 pauses == 5 seconds
                if (pauses++ % 50 == 0)
                {
                    parserProgress.BackPressurePulse(Interlocked.Read(ref runningWorkItemCount), Interlocked.Read(ref queuedWorkItemCount), this.pendingRevisions, this.runningSet);
                }

                Thread.Sleep(100);  // 100 milliseconds
            }

            // create a WorkItemInfo instance with the Insertable and our connection
            // disposable connection object is now owned by WorkItemInfo object
            WorkItemInfo ci = new (this, i, previous, this, parserProgress);

            // queue it up!
            Interlocked.Increment(ref queuedWorkItemCount);
            ThreadPool.SetMaxThreads(32, 1);
            ThreadPool.QueueUserWorkItem(new WaitCallback(this.WorkCallback), ci);

            // return our current running count. This might
            // not have incremented just yet, but this is a convenient
            // time to help show status
            return (runningWorkItemCount, queuedWorkItemCount, this.pendingRevisions);
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
            Interlocked.Decrement(ref queuedWorkItemCount);

            // add to the running count
            Interlocked.Increment(ref runningWorkItemCount);

            this.runningSet.TryAdd(wii, true);

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
                Interlocked.Decrement(ref runningWorkItemCount);

                // remove from the set of running
                this.runningSet.Remove(wii, out _);
            }
        }

        public void WaitForComplete()
        {
            while (runningWorkItemCount > 0)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Write($"{runningWorkItemCount} work items running, {queuedWorkItemCount} work items queued, {this.pendingRevisions} pending revisions");
                Console.ResetColor();
                Console.WriteLine();
                Thread.Sleep(2500);
            }

            return;
        }

        public int InsertedPages()
        {
            return this.insertedPages;
        }

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
