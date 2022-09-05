namespace WikiReader
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Threading;

    /// <summary>
    /// Class that represents a single page. It directly holds the page name, the page's namespaceID,
    /// and the pageID. It contains a list of zero or more revisions of the page,
    /// each an instance of the PageRevision class.
    /// </summary>
    class Page : IInsertable
    {
        /// <summary>
        /// Name of the page we represent.
        /// </summary>
        private readonly string pageName;

        /// <summary>
        /// Name of the page redirected to, null if not a redirect.
        /// </summary>
        private readonly string? redirectTitle;

        /// <summary>
        /// Namespace which holds that page.
        /// </summary>
        private readonly int namespaceId = 0;

        /// <summary>
        /// ID number of this page.
        /// </summary>
        private readonly long pageId = 0;

        private readonly long runId;
        private readonly long filePosition;

        private readonly ManualResetEvent completeEvent = new(false);

        public ManualResetEvent GetCompletedEvent()
        {
            return completeEvent;
        }

        /// <summary>
        /// set indicating which users we've already inserted
        /// </summary>
        static readonly HashSet<long> insertedUserSet = new ();

        /// <summary>
        /// Map of revisions from RevisionID to the PageRevision at that ID
        /// </summary>
        private readonly SortedList<long, PageRevision> revisions = new ();

        private int _usersAdded = 0;
        private int _usersAlready = 0;
        private int _revsAdded = 0;
        private int _revsAlready = 0;

        /// <summary>
        /// Initializes a new instance of the <see cref="Page"/> class.
        /// </summary>
        /// <param name="namespaceId">namespaceId where this page lives</param>
        /// <param name="pageId">pageID for this page</param>
        /// <param name="pageName">name of this page</param>
        public Page(int namespaceId, long pageId, string pageName, string? redirectName, long runId, long filePosition)
        {
            this.namespaceId = namespaceId;
            this.pageName = pageName;
            this.pageId = pageId;
            this.redirectTitle = redirectName;
            this.runId = runId;
            this.filePosition = filePosition;
        }

        /// <summary>
        /// add a revision to the history of this page in memory.
        /// </summary>
        /// <param name="pr"></param>
        public void AddRevision(PageRevision pr)
        {
            if (this.revisions.ContainsKey(pr.RevisionId))
            {
                Console.WriteLine($"Page [[{this.pageName}]] already has revision {pr.RevisionId}");
                Console.WriteLine($"current: {this.revisions[pr.RevisionId].TimeStamp} with id {this.revisions[pr.RevisionId].RevisionId}, parent id {this.revisions[pr.RevisionId].ParentRevisionId}");
                Console.WriteLine($"    new: {pr.TimeStamp} with id {pr.RevisionId}, parent id {pr.ParentRevisionId}");
            }

            this.revisions.Add(pr.RevisionId, pr);

            // always keep first and last revisions
            // keep every 100th revision
            // keep the most recent revision
            int nCandidate = this.revisions.Count - 2;
            if (nCandidate > 0 && nCandidate % 100 != 0)
            {
                this.revisions.Values[nCandidate].Text = null;
            }
        }

        /// <summary>
        /// Insert this page, all its revisions, text, and related users
        /// </summary>
        /// <param name="previous"></param>
        /// <param name="pump"></param>
        /// <param name="conn">connection to use for insertion</param>
        /// <param name="progress">InsertableProgress interface for progress callbacks</param>
        public void Insert(IInsertable? previous, DatabasePump pump, SqlConnection conn, IInsertableProgress progress)
        {
            progress.AddPendingRevisions(revisions.Count);

            // first, insert all the users
            this.BulkInsertUsers(pump, conn);

            // insert the page record itself
            this.InsertPage(pump, progress, conn);

            // then, insert the revisions
            this.SelectAndInsertRevisions(pump, progress, previous, conn);

            // insert the text that we have
            this.BulkInsertRevisionText(pump, conn);

            Console.WriteLine(
                $"[[{this.pageName}]]\n" +
                $"   {this._revsAdded} revisions added, {this._revsAlready} revisions exist\n" +
                $"   {this._usersAdded} users added, {this._usersAlready} users exist");

            UpdateProgress(conn);
        }

        private void UpdateProgress(SqlConnection conn)
        {
            // try to insert first

            using var insertCommand = new SqlCommand(
                "WITH X AS (" + 
                "    SELECT * FROM (VALUES (@RunID, GETUTCDATE(), @FilePosition, @PageID)) AS X(RunID, ReportTime, FilePosition, PageID) " +
                ") " +
                "INSERT RunProgress (RunID, ReportTime, FilePosition, PageID) " +
                "SELECT RunID, ReportTime, FilePosition, PageID " +
                "  FROM X " +
                " WHERE NOT EXISTS (SELECT RunID FROM RunProgress WHERE RunID = X.RunID)", conn);
            insertCommand.Parameters.AddWithValue("@RunID", this.runId);
            insertCommand.Parameters.AddWithValue("@FilePosition", this.filePosition);
            insertCommand.Parameters.AddWithValue("@PageID", this.pageId);

            int rows = insertCommand.ExecuteNonQuery();

            // not inserted, so a row exists ... let's update
            if (rows == 0)
            {
                using var updateCommand = new SqlCommand(
                    "UPDATE RunProgress " + 
                    "   SET FilePosition = @FilePosition, " +
                    "       ReportTime = GETUTCDATE(), " + 
                    "       PageID = @PageID" +
                    " WHERE FilePosition < @FilePosition " +
                    "   AND RunID = @RunID", conn);

                updateCommand.Parameters.AddWithValue("@RunID", this.runId);
                updateCommand.Parameters.AddWithValue("@FilePosition", this.filePosition);
                updateCommand.Parameters.AddWithValue("@PageID", this.pageId);

                updateCommand.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Insert the users who have edited this page.
        /// </summary>
        /// <param name="pump">DatabasePump to record our activity.</param>
        /// <param name="conn">connection to use for insertion.</param>
        private void BulkInsertUsers(DatabasePump pump, SqlConnection conn)
        {
            // build a unique temporary table name
            string tempTableName = $"#Users_{System.Environment.MachineName}_{Environment.CurrentManagedThreadId}";

            // create that temporary table
            using var tableCreate = new SqlCommand(
                $"CREATE TABLE [{tempTableName}] (" +
                "	UserID BIGINT NOT NULL," +
                "	UserName NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL );", conn);
            tableCreate.ExecuteNonQuery();

            long bulkActivity = -1;

            try
            {
                // bulk insert into the temporary table
                UserDataReader udr = new(insertedUserSet, this.revisions.Values);
                SqlBulkCopy sbc = new(conn);

                bulkActivity = pump.StartActivity("Bulk Insert Users", this.namespaceId, this.pageId, udr.Count);

                sbc.DestinationTableName = tempTableName;
                sbc.ColumnMappings.Add("UserID", "UserID");
                sbc.ColumnMappings.Add("UserName", "UserName");
                Trace.Assert(conn.State == ConnectionState.Open);
                sbc.WriteToServer(udr);
                Trace.Assert(conn.State == ConnectionState.Open);

                pump.CompleteActivity(bulkActivity, insertedUserSet.Count, "inserted users");
                bulkActivity = -1;

                // merge up.
                SqlException? mergeException = null;
                for (int retries = 10; retries > 0; retries--)
                {
                    long mergeActivity = pump.StartActivity("Merge Users", this.namespaceId, this.pageId, udr.Count);
                    try
                    {
                        using var tableMerge = new SqlCommand(
                            "MERGE INTO [User] WITH (HOLDLOCK) " +
                            $"USING [{tempTableName}] AS SRC ON [User].UserID = SRC.UserID " +
                            "WHEN NOT MATCHED THEN " +
                            " INSERT (UserID, UserName) VALUES (SRC.UserID, SRC.UserName);", conn);
                        tableMerge.CommandTimeout = 300;
                        this._usersAdded = tableMerge.ExecuteNonQuery();
                        this._usersAlready = udr.Count - this._usersAdded;
                        mergeException = null;
                        break;
                    }
                    catch (SqlException sex)
                    {
                        if (sex.Number == 1205)
                        {
                            Console.WriteLine($"[[{this.pageName}]]: Deadlock encountered during USER merge of {udr.Count} rows. Retry {retries}");
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else if (sex.Number == -2)
                        {
                            Console.WriteLine($"[[{this.pageName}]]: Timeout encountered during USER merge of {udr.Count} rows. Retry {retries}");
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else
                        {
                            Console.WriteLine($"[[{this.pageName}]]: Exception during USER merge of {udr.Count} rows. {sex.Number}: {sex.Source}\n{sex.Message}");
                            throw sex;
                        }
                    }
                    catch (InvalidOperationException ioe)
                    {
                        Console.WriteLine($"[[{this.pageName}]]: Exception during PageRevision merge of {udr.Count} rows. {ioe.Message}\n{ioe.StackTrace}");
                        throw ioe;
                    }
                    finally
                    {
                        pump.CompleteActivity(mergeActivity, this._usersAdded, mergeException?.Message);
                    }

                }
                if (mergeException != null)
                {
                    Console.WriteLine($"[[{this.pageName}]]: USER merge failed 10 times: {mergeException.Number}, {mergeException.Source}\n{mergeException.Message}");
                    throw mergeException;
                }
            }
            catch (Exception ex)
            {
                if (bulkActivity != -1)
                {
                    pump.CompleteActivity(bulkActivity, null, ex.Message);
                    bulkActivity = -1;
                }
            }
            finally
            {
                // drop the temporary table
                using var tableDrop = new SqlCommand($"DROP TABLE [{tempTableName}]", conn);
                tableDrop.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Insert the text of the revisions we're keeping
        /// </summary>
        /// <param name="conn">connection to use for insertion</param>
        private void BulkInsertRevisionText(DatabasePump pump, SqlConnection conn)
        {
            // build our data reader first
            PageRevisionTextDataReader prtdr = new(this.namespaceId, this.pageId, this.revisions.Values);

            long bulkActivity = pump.StartActivity("Bulk Insert Text", this.namespaceId, this.pageId, prtdr.Count);

            // if it inserts nothing, skip all this work
            if (prtdr.Count == 0)
            {
                pump.CompleteActivity(bulkActivity, 0, "No text to insert");
                return;
            }

            // build a unique temporary table name
            string tempTableName = $"#Text_{System.Environment.MachineName}_{Environment.CurrentManagedThreadId}";

            // create that temporary table
            using var tableCreate = new SqlCommand(
                $"CREATE TABLE [{tempTableName}] (" +
                "   NamespaceID INT NOT NULL, " +
                "	PageID BIGINT NOT NULL," +
                "	PageRevisionID BIGINT NOT NULL," +
                "   ArticleText TEXT NOT NULL);", conn);
            tableCreate.ExecuteNonQuery();

            try
            {
                // bulk insert into the temporary table
                SqlBulkCopy sbc = new (conn);

                sbc.DestinationTableName = tempTableName;
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("PageID", "PageID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("NamespaceID", "NamespaceID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("PageRevisionID", "PageRevisionID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ArticleText", "ArticleText"));
                Trace.Assert(conn.State == ConnectionState.Open);
                sbc.WriteToServer(prtdr);
                Trace.Assert(conn.State == ConnectionState.Open);

                pump.CompleteActivity(bulkActivity, prtdr.Count, "inserted text");
                bulkActivity = -1;

                // merge up.
                SqlException? mergeException = null;
                for (int retries = 10; retries > 0; retries--)
                {
                    long mergeActivity = pump.StartActivity("Merge Text", this.namespaceId, this.pageId, prtdr.Count);
                    try
                    {
                        using var tableMerge = new SqlCommand(
                            "MERGE INTO [PageRevisionText] WITH (HOLDLOCK) " +
                            "USING [" + tempTableName + "] AS SRC  " +
                            "   ON [PageRevisionText].NamespaceID = SRC.NamespaceID " +
                            "  AND [PageRevisionText].PageID = SRC.PageID " +
                            "  AND [PageRevisionText].PageRevisionID = SRC.PageRevisionID " +
                            "WHEN NOT MATCHED THEN " +
                            "INSERT (NamespaceID, PageID, PageRevisionID, ArticleText) VALUES (SRC.NamespaceID, SRC.PageID, SRC.PageRevisionID, SRC.ArticleText);", conn);
                        this._usersAdded = tableMerge.ExecuteNonQuery();
                        this._usersAlready = prtdr.Count - this._usersAdded;
                        mergeException = null;
                        break;
                    }
                    catch (SqlException sex)
                    {
                        if (sex.Number == 1205)
                        {
                            Console.WriteLine($"[[{this.pageName}]]: Deadlock encountered during TEXT merge of {prtdr.Count} rows. Retry {retries}");
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else if (sex.Number == -2)
                        {
                            Console.WriteLine($"[[{this.pageName}]]: Timeout encountered during TEXT merge of {prtdr.Count} rows. Retry {retries}");
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else
                        {
                            Console.WriteLine($"[[{this.pageName}]]: Exception during TEXT merge of {prtdr.Count} rows. {sex.Number}: {sex.Source}\n{sex.Message}");
                            throw sex;
                        }
                    }
                    catch (InvalidOperationException ioe)
                    {
                        Console.WriteLine($"[[{this.pageName}]]: Exception during TEXT merge of {prtdr.Count} rows. {ioe.Message}\n{ioe.StackTrace}");
                        throw ioe;
                    }
                    finally
                    {
                        pump.CompleteActivity(mergeActivity, _usersAdded, mergeException?.Message);
                    }

                }
                if (mergeException != null)
                {
                    Console.WriteLine($"[[{this.pageName}]]: TEXT merge failed 10 times: {mergeException.Number}, {mergeException.Source}\n{mergeException.Message}");
                    throw mergeException;
                }
            }
            catch (Exception ex)
            {
                if (bulkActivity != -1)
                {
                    pump.CompleteActivity(bulkActivity, null, ex.Message);
                    bulkActivity = -1;
                }
            }
            finally
            {
                // drop the temporary table
                using var tableDrop = new SqlCommand($"DROP TABLE [{tempTableName}];", conn);
                tableDrop.ExecuteNonQuery();
            }
        }

        private void SelectAndInsertRevisions(DatabasePump pump, IInsertableProgress progress, IInsertable? previous, SqlConnection conn)
        {
            long checkActivityID = pump.StartActivity("Check existing PageRevisions", null, this.pageId, this.revisions.Count);

            // build a hash set of all the known revisions of this page
            HashSet<long> knownRevisions = new();

            using var cmdSelect = new SqlCommand("select PageRevisionID FROM PageRevision WHERE PageID = @PageID", conn);
            cmdSelect.Parameters.AddWithValue("@NamespaceID", this.namespaceId);
            cmdSelect.Parameters.AddWithValue("@PageID", this.pageId);
            cmdSelect.CommandTimeout = 3600;

            using (var reader = cmdSelect.ExecuteReader())
            {
                while (reader.Read())
                    knownRevisions.Add((long)reader["PageRevisionID"]);
            }

            SortedList<long, PageRevision> neededRevisions = new();

            foreach ((long revID, var rev) in this.revisions)
            {
                if (knownRevisions.Contains(revID))
                {
                    progress.CompleteRevisions(1);
                    this._revsAlready += 1;
                    continue;
                }

                neededRevisions.Add(revID, rev);
            }

            pump.CompleteActivity(checkActivityID, this.revisions.Count, "Processed");


            this.BulkInsertPageRevisions(pump, progress, previous, neededRevisions, conn);
        }


        private void BulkInsertPageRevisions(DatabasePump pump, IInsertableProgress progress, IInsertable? previous, SortedList<long, PageRevision> neededRevisions, SqlConnection conn)
        {
            long bulkActivityID = -1;

            // now, bulk insert the ones we didin't find
            try
            {
                if (neededRevisions.Count > 0)
                {
                    bulkActivityID = pump.StartActivity("Bulk Insert PageRevisions", this.namespaceId, this.pageId, neededRevisions.Count);

                    // bulk insert into the temporary table
                    PageRevisionDataReader prdr = new(this.namespaceId, this.pageId, neededRevisions.Values);
                    var sbc = new SqlBulkCopy(conn);
                    sbc.BulkCopyTimeout = 3600;

                    sbc.DestinationTableName = "PageRevision";
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("PageID", "PageID"));
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("NamespaceID", "NamespaceID"));
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("PageRevisionID", "PageRevisionID"));
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ParentPageRevisionID", "ParentPageRevisionID"));
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevisionWhen", "RevisionWhen"));
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ContributorID", "ContributorID"));
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Comment", "Comment"));
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("IsMinor", "IsMinor"));
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ArticleTextLength", "ArticleTextLength"));
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("UserDeleted", "UserDeleted"));
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("TextDeleted", "TextDeleted"));
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("IPAddress", "IPAddress"));
                    sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("TextAvailable", "TextAvailable"));

                    Trace.Assert(conn.State == ConnectionState.Open);
                    sbc.WriteToServer(prdr);
                    Trace.Assert(conn.State == ConnectionState.Open);

                    pump.CompleteActivity(bulkActivityID, null, null);
                    bulkActivityID = -1;

                    progress.CompleteRevisions(neededRevisions.Count);
                    this._revsAdded += neededRevisions.Count;
                }

                // wait until the previous revision is done, if we've got one
                if (previous != null)
                {
                    // Console.WriteLine($"[[{(this as IInsertable).ObjectName}]] waiting on [[{previous.ObjectName}]]");

                    ManualResetEvent mre = previous.GetCompletedEvent();
                    while (!mre.WaitOne(1000))
                        Console.WriteLine($"[[{(this as IInsertable).ObjectName}]] is waiting on [[{previous.ObjectName}]]");
                }
                else
                {
                    // Console.WriteLine($"[[{(this as IInsertable).ObjectName}]] not waiting, no previous");
                }
            }
            catch (Exception ex)
            {
                if (bulkActivityID != -1)
                {
                    pump.CompleteActivity(bulkActivityID, null, ex.Message);
                    bulkActivityID = -1;
                }
            }
            finally
            {
                // signal the next in the chain of waiters
                this.completeEvent.Set();
            }
        }

        private void BulkMergeRevisions(DatabasePump pump, IInsertableProgress progress, IInsertable? previous, SqlConnection conn)
        {
            // build a unique temporary table name
            string tempTableName = $"#Revisions_{System.Environment.MachineName}_{Environment.CurrentManagedThreadId}";

            // create that temporary table
            using var tableCreate = new SqlCommand(
                $"CREATE TABLE [{tempTableName}] (" +
                "   NamespaceID INT NOT NULL, " +
                "   PageID BIGINT NOT NULL, " +
                "   PageRevisionID BIGINT NOT NULL, " +
                "   ParentPageRevisionID BIGINT NOT NULL, " +
                "   RevisionWhen DATETIME NOT NULL, " +
                "   ContributorID BIGINT, " +
                "   IPAddress VARCHAR(39), " +
                "   Comment NVARCHAR(255), " +
                "   TextAvailable BIT NOT NULL, " +
                "   IsMinor BIT NOT NULL, " +
                "   ArticleTextLength INT NOT NULL, " +
                "   TextDeleted BIT NOT NULL, " +
                "   UserDeleted BIT NOT NULL );", conn);
            tableCreate.ExecuteNonQuery();

            long bulkActivityID = -1;

            try {
                // bulk insert into the temporary table
                PageRevisionDataReader prdr = new(this.namespaceId, this.pageId, this.revisions.Values);
                var sbc = new SqlBulkCopy(conn);

                bulkActivityID = pump.StartActivity("Bulk Insert PageRevisions", this.namespaceId, this.pageId, prdr.Count);

                sbc.DestinationTableName = tempTableName;
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("PageID", "PageID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("NamespaceID", "NamespaceID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("PageRevisionID", "PageRevisionID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ParentPageRevisionID", "ParentPageRevisionID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevisionWhen", "RevisionWhen"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ContributorID", "ContributorID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Comment", "Comment"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("IsMinor", "IsMinor"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ArticleTextLength", "ArticleTextLength"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("UserDeleted", "UserDeleted"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("TextDeleted", "TextDeleted"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("IPAddress", "IPAddress"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("TextAvailable", "TextAvailable"));

                Trace.Assert(conn.State == ConnectionState.Open);
                sbc.WriteToServer(prdr);
                Trace.Assert(conn.State == ConnectionState.Open);

                pump.CompleteActivity(bulkActivityID, null, null);
                bulkActivityID = -1;

                // wait until the previous revision is done, if we've got one
                if (previous != null)
                {
                    ManualResetEvent mre = previous.GetCompletedEvent();
                    mre.WaitOne();
                }

                // merge up!
                SqlException? mergeException = null;
                for (int retries = 10; retries > 0; retries--)
                {
                    long mergeActivity = pump.StartActivity("Merge PageRevisions", this.namespaceId, this.pageId, prdr.Count);
                    try
                    {
                        using var tableMerge = new SqlCommand(
                            "  MERGE INTO [PageRevision] WITH (HOLDLOCK)" +
                            $"  USING [{tempTableName}] AS SRC " +
                            "     ON [PageRevision].PageRevisionID = SRC.PageRevisionID " +
                            "	 AND [PageRevision].PageID = SRC.PageID " +
                            "    AND [PageRevision].NamespaceID = SRC.NamespaceID " +
                            "WHEN NOT MATCHED THEN " +
                            " INSERT (NamespaceID, PageID, PageRevisionID, ParentPageRevisionID, " +
                            "        RevisionWhen, ContributorID, IPAddress, Comment, " +
                            "        TextAvailable, IsMinor, ArticleTextLength, TextDeleted, UserDeleted) " +
                            " VALUES (SRC.NamespaceID, SRC.PageID, SRC.PageRevisionID, SRC.ParentPageRevisionID, " +
                            "        SRC.RevisionWhen, SRC.ContributorID, SRC.IPAddress, SRC.Comment, " + 
                            "	     SRC.TextAvailable, SRC.IsMinor, SRC.ArticleTextLength, SRC.TextDeleted, SRC.UserDeleted);",
                            conn);
                        tableMerge.CommandTimeout = 3600;
                        this._revsAdded = tableMerge.ExecuteNonQuery();
                        this._revsAlready = prdr.Count - this._revsAdded;
                        mergeException = null;
                        progress.CompleteRevisions(prdr.Count);
                        break;
                    }
                    catch (SqlException sex)
                    {
                        if (sex.Number == 1205)
                        {
                            Console.WriteLine($"[[{this.pageName}]]: Deadlock encountered during PageRevision merge of {prdr.Count} rows. Retry {retries}");
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else if (sex.Number == -2)
                        {
                            Console.WriteLine($"[[{this.pageName}]]: Timeout encountered during PageRevision merge of {prdr.Count} rows. Retry {retries}");
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else
                        {
                            Console.WriteLine($"[[{this.pageName}]]: Exception during PageRegision merge of {prdr.Count} rows. {sex.Number}: {sex.Source}\n{sex.Message}");
                            throw sex;
                        }
                    }
                    catch (InvalidOperationException ioe)
                    {
                        Console.WriteLine($"[[{this.pageName}]]: Exception during PageRevision merge of {prdr.Count} rows. {ioe.Message}\n{ioe.StackTrace}");
                        throw ioe;
                    }
                    finally
                    {
                        pump.CompleteActivity(mergeActivity, this._revsAdded, mergeException?.Message);
                    }
                }

                if (mergeException != null)
                {
                    Console.WriteLine($"[[{this.pageName}]]: PageRevision merge failed 10 times: {mergeException.Number}, {mergeException.Source}\n{mergeException.Message}");
                    throw mergeException;
                }
            }
            catch (Exception ex)
            {
                if (bulkActivityID != -1)
                {
                    pump.CompleteActivity(bulkActivityID, null, ex.Message);
                    bulkActivityID = -1;
                }
            }
            finally
            {
                // drop the temporary table
                using var tableDrop = new SqlCommand($"DROP TABLE [{tempTableName}];", conn);
                tableDrop.ExecuteNonQuery();

                // signal the next in the chain of waiters
                this.completeEvent.Set();
            }
        }

        /// <summary>
        /// Insert the page object itself. There's no batching here, since pages are infrequent
        /// compared to all their revisions and text. We'll blindly insert, then handle any duplicate
        /// errors by doing an update (since redirection may be updated).
        /// </summary>
        /// <param name="pump">Database pump object to record our activity</param>
        /// <param name="progress">implementation of IProgress interface to track our work</param>
        /// <param name="conn">Connection to use for insertion</param>
        private void InsertPage(DatabasePump pump, IInsertableProgress progress, SqlConnection conn)
        {
            long activityID = pump.StartActivity("Insert Page", this.namespaceId, this.pageId, 1);
            using var cmd = new SqlCommand("INSERT INTO [Page] (NamespaceID, PageID, PageName, RedirectTitle) VALUES (@NamespaceID, @PageID, @PageName, @RedirectTitle);", conn);
            cmd.Parameters.AddWithValue("@NamespaceID", this.namespaceId);
            cmd.Parameters.AddWithValue("@PageID", this.pageId);
            cmd.Parameters.AddWithValue("@PageName", this.pageName);
            cmd.Parameters.AddWithValue("@RedirectTitle", this.redirectTitle ?? (object)DBNull.Value);

            Exception? exResult = null;
            int inserted = 0;
            try
            {
                cmd.ExecuteNonQuery();
                progress.InsertedPages(1);
                inserted++;
            }
            catch (SqlException sex)
            {
                exResult = sex;
                if (sex.Number == 8152)
                    Console.WriteLine($"Error: page name is too long at {this.pageName.Length} characters");
                else if (sex.Number == 2601 || sex.Number == 2627)
                {
                    // duplicate! we'll do an update instead
                    using var updateCmd = new SqlCommand("UPDATE [Page] SET RedirectTitle = @RedirectTitle WHERE PageID = @PageID AND NamespaceID = @NamespaceID;", conn);
                    updateCmd.Parameters.AddWithValue("@NamespaceID", this.namespaceId);
                    updateCmd.Parameters.AddWithValue("@PageID", this.pageId);
                    updateCmd.Parameters.AddWithValue("@RedirectTitle", this.redirectTitle ?? (object)DBNull.Value);

                    updateCmd.ExecuteNonQuery();
                    exResult = null;
                }
                else
                {
                    exResult = null;
                    throw;
                }
            }
            catch (Exception ex)
            {
                exResult = ex;
            }
            finally
            {
                pump.CompleteActivity(activityID, inserted, exResult?.Message);
            }
        }

        /// <summary>
        /// Gets our object name; this is used to name the connection in SQL Server.
        /// </summary>
        string IInsertable.ObjectName
        {
            get { return $"Page Inserter ({this.pageName})"; }
        }

        /// <summary>
        /// How many revisions do we plan to insert?
        /// </summary>
        int IInsertable.RevisionCount
        {
            get { return this.revisions.Count; }
        }
    }

}
