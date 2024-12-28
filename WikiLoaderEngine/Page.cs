namespace WikiLoaderEngine
{
    using System;
    using System.Collections.Concurrent;
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
    internal class Page : IInsertable
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

        private int usersAdded = 0;
        private int usersAlready = 0;
        private int revsAdded = 0;
        private int revsAlready = 0;

        private string state = "idle";

        /// <summary>
        /// set indicating which users we've already inserted.
        /// </summary>
        private static readonly ConcurrentDictionary<long, bool> InsertedUserSet = new ();

        /// <summary>
        /// Map of revisions from RevisionID to the PageRevision at that ID.
        /// </summary>
        private readonly SortedList<long, PageRevision> revisions = new ();

        /// <summary>
        /// Initializes a new instance of the <see cref="Page"/> class.
        /// </summary>
        /// <param name="namespaceId">namespaceId where this page lives.</param>
        /// <param name="pageId">pageID for this page.</param>
        /// <param name="pageName">name of this page.</param>
        /// <param name="redirectName">name of page this redirects to, null if not a redirect.</param>
        /// <param name="runId">RunID we're working.</param>
        /// <param name="filePosition">position of file to start working (0 if entire file).</param>
        public Page(int namespaceId, long pageId, string pageName, string? redirectName, long runId, long filePosition)
        {
            this.namespaceId = namespaceId;
            this.pageName = pageName;
            this.pageId = pageId;
            //TODO: add truncation logging
            if (redirectName != null && redirectName.Length > 510)
                this.redirectTitle = redirectName[..510];
            else
                this.redirectTitle = redirectName;
            this.runId = runId;
            this.filePosition = filePosition;
        }

        /// <summary>
        /// add a revision to the history of this page in memory.
        /// </summary>
        /// <param name="pr">PageRevision to be added.</param>
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
        /// Insert this page, all its revisions, text, and related users.
        /// </summary>
        /// <param name="pump">reference to our database pump.</param>
        /// <param name="conn">connection to use for insertion.</param>
        /// <param name="progress">InsertableProgress interface for progress callbacks.</param>
        /// <param name="parserProgress">IXmlDumpParserProgress interface which receives notifications for completed pages.</param>
        public void Insert(DatabasePump pump, SqlConnection conn, IInsertableProgress progress, IXmlDumpParserProgress parserProgress)
        {
            // tally pending revisions for status
            progress.AddPendingRevisions(this.revisions.Count);

            // first, insert the page record itself
            Stopwatch pageTime = Stopwatch.StartNew();
            state = "inserting page";
            this.InsertPage(pump, progress, conn);
            pageTime.Stop();

            // see what revisions are needed
            Stopwatch revisionsReadTime = Stopwatch.StartNew();
            var neededRevisions = this.FindNeededRevisions(pump, progress, conn);
            revisionsReadTime.Stop();

            // insert users for those revisions
            Stopwatch usersTime = Stopwatch.StartNew();
            state = "inserting users";
            this.InsertUsers(pump, neededRevisions, conn);
            usersTime.Stop();

            // then, insert the revisions themselves
            Stopwatch revisionsTime = Stopwatch.StartNew();
            this.BulkInsertPageRevisions(pump, progress, neededRevisions, conn);
            revisionsTime.Stop();

            // insert the text that we have
            Stopwatch revisionsTextTime = Stopwatch.StartNew();
            state = "inserting text";
            this.BulkInsertRevisionText(pump, conn);
            revisionsTextTime.Stop();

            Stopwatch progressTime = Stopwatch.StartNew();
            state = "updating progress";
            pump.UpdateRunProgress(conn, this.filePosition, this.pageId);
            progressTime.Stop();

            long totalMilliseconds = usersTime.ElapsedMilliseconds + pageTime.ElapsedMilliseconds + revisionsReadTime.ElapsedMilliseconds + revisionsTime.ElapsedMilliseconds + revisionsTextTime.ElapsedMilliseconds + progressTime.ElapsedMilliseconds;

            parserProgress.CompletedPage(this.pageName, this.usersAdded, this.usersAlready, this.revsAdded, this.revsAlready, totalMilliseconds);

            state = "complete"; 

            /*
            Console.WriteLine($"   {this.pageName}: usersTime: {usersTime.ElapsedMilliseconds}, pageTime: {pageTime.ElapsedMilliseconds}, " +
                $"revisionsTime: {revisionsTime.ElapsedMilliseconds}, revisionsTextTime: {revisionsTextTime.ElapsedMilliseconds}, " +
                $"revisionsReadTime: {revisionsReadTime.ElapsedMilliseconds}, progressTime: {progressTime.ElapsedMilliseconds}");
            */
        }

        private void InsertUsers(DatabasePump pump, SortedList<long, PageRevision> neededRevisions, SqlConnection conn)
        {
            if (neededRevisions.Count == 0)
                return;

            // build a list of the users not before inserted
            List<User> neededUsers = new();
            foreach (PageRevision pr in neededRevisions.Values)
            {
                if (pr.Contributor == null)
                    continue;

                if (!pr.Contributor.IsAnonymous)
                {
                    if (InsertedUserSet.TryAdd(pr.Contributor.ID, true))
                    {
                        neededUsers.Add(pr.Contributor);
                    }
                }
            }

            if (neededUsers.Count >= 50)
            {
                BulkInsertUsers(pump, neededUsers, conn);
            }
            else if (neededUsers.Count != 0)
            {
                // do it directly one by each
                SqlCommand cmd = new ("INSERT INTO [User] (UserID, UserName) VALUES (@UserID, @UserName)", conn);

                foreach (User u in neededUsers)
                {
                    cmd.Parameters.Clear();

                    cmd.Parameters.AddWithValue("@UserID", u.ID);
                    cmd.Parameters.AddWithValue("@UserName", u.Name);

                    try
                    {
                        this.usersAdded += cmd.ExecuteNonQuery();
                        InsertedUserSet.TryAdd(u.ID, true);
                    }
                    catch (SqlException sex)
                    {
                        if (sex.Number == 2627)
                        {
                            this.usersAlready += 1;
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Insert the users who have edited this page.
        /// </summary>
        /// <param name="pump">DatabasePump to record our activity.</param>
        /// <param name="conn">connection to use for insertion.</param>
        private void BulkInsertUsers(DatabasePump pump, List<User> neededUsers, SqlConnection conn)
        {
            if (neededUsers.Count == 0)
                return;

            // build a unique temporary table name
            string tempTableName = $"#Users_{System.Environment.MachineName}_{Environment.CurrentManagedThreadId}";

            // create that temporary table
            using var tableCreate = new SqlCommand(
                $"CREATE TABLE [{tempTableName}] (" +
                "	UserID BIGINT NOT NULL," +
                "	UserName NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL);", conn);
            tableCreate.ExecuteNonQuery();

            long bulkActivity = -1;

            try
            {
                // bulk insert into the temporary table
                UserDataReader udr = new (neededUsers);
                SqlBulkCopy sbc = new (conn);

                bulkActivity = pump.StartActivity("Bulk Insert Users", this.namespaceId, this.pageId, udr.Count);

                sbc.DestinationTableName = tempTableName;
                sbc.ColumnMappings.Add("UserID", "UserID");
                sbc.ColumnMappings.Add("UserName", "UserName");
                Trace.Assert(conn.State == ConnectionState.Open);
                sbc.WriteToServer(udr);
                Trace.Assert(conn.State == ConnectionState.Open);

                pump.CompleteActivity(bulkActivity, udr.Count, null);
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
                        this.usersAdded = tableMerge.ExecuteNonQuery();
                        this.usersAlready = udr.Count - this.usersAdded;
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
                            throw;
                        }
                    }
                    catch (InvalidOperationException ioe)
                    {
                        Console.WriteLine($"[[{this.pageName}]]: Exception during PageRevision merge of {udr.Count} rows. {ioe.Message}\n{ioe.StackTrace}");
                        throw;
                    }
                    finally
                    {
                        pump.CompleteActivity(mergeActivity, this.usersAdded, mergeException?.Message);
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
        /// Insert the text of the revisions we're keeping.
        /// </summary>
        /// <param name="conn">connection to use for insertion.</param>
        private void BulkInsertRevisionText(DatabasePump pump, SqlConnection conn)
        {
            // build our data reader first
            PageRevisionTextDataReader prtdr = new (this.namespaceId, this.pageId, this.revisions.Values);

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
                        int inserted = tableMerge.ExecuteNonQuery();

                        // now update TextAvailable
                        using var availableUpdate = new SqlCommand(
                            "UPDATE PageRevision " +
                            "   SET TextAvailable = 1 " +
                           $" WHERE PageRevisionID IN (SELECT PageRevisionID FROM {tempTableName})", conn);
                        int updated = availableUpdate.ExecuteNonQuery();

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
                            throw;
                        }
                    }
                    catch (InvalidOperationException ioe)
                    {
                        Console.WriteLine($"[[{this.pageName}]]: Exception during TEXT merge of {prtdr.Count} rows. {ioe.Message}\n{ioe.StackTrace}");
                        throw;
                    }
                    finally
                    {
                        pump.CompleteActivity(mergeActivity, this.usersAdded, mergeException?.Message);
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

        private SortedList<long, PageRevision> FindNeededRevisions(DatabasePump pump, IInsertableProgress progress, SqlConnection conn)
        {
            long checkActivityID = pump.StartActivity("Check existing PageRevisions", this.namespaceId, this.pageId, this.revisions.Count);

            state = "read existing revisions";
            // build a hash set of all the known revisions of this page
            Dictionary<long, ExistingPageRevisionInfo> knownRevisions = new ();

            using var cmdSelect = new SqlCommand("select PageRevisionID, TextAvailable FROM PageRevision WHERE PageID = @PageID", conn);
            cmdSelect.Parameters.AddWithValue("@NamespaceID", this.namespaceId);
            cmdSelect.Parameters.AddWithValue("@PageID", this.pageId);
            cmdSelect.CommandTimeout = 3600;

            using (var reader = cmdSelect.ExecuteReader())
            {
                while (reader.Read())
                {
                    ExistingPageRevisionInfo epri = new ()
                    {
                        ID = (long)reader["PageRevisionID"],
                        TextAvailable = (bool)reader["TextAvailable"],
                    };
                    knownRevisions.Add(epri.ID, epri);
                }
            }

            SortedList<long, PageRevision> neededRevisions = new ();

            foreach ((long revID, var rev) in this.revisions)
            {

                // do we have it? skip it if we don't already have its text and have it here
                if (knownRevisions.TryGetValue(revID, out ExistingPageRevisionInfo? revInfo))
                {
                    // skip, unless new input has text and we don't already have it
                    if (!(!revInfo.TextAvailable && rev.TextAvailable))
                    {
                        progress.CompleteRevisions(1);
                        this.revsAlready += 1;
                        continue;
                    }
                }

                neededRevisions.Add(revID, rev);
            }

            pump.CompleteActivity(checkActivityID, this.revisions.Count, null);

            return neededRevisions;
        }


        private void BulkInsertPageRevisions(DatabasePump pump, IInsertableProgress progress, SortedList<long, PageRevision> neededRevisions, SqlConnection conn)
        {
            long bulkActivityID = -1;

            state = "insert revisions";
            // now, bulk insert the ones we didn't find
            try
            {
                if (neededRevisions.Count > 0)
                {
                    bulkActivityID = pump.StartActivity("Bulk Insert PageRevisions", this.namespaceId, this.pageId, neededRevisions.Count);

                    // bulk insert into the temporary table
                    PageRevisionDataReader prdr = new (this.namespaceId, this.pageId, neededRevisions.Values);
                    var sbc = new SqlBulkCopy(conn)
                    {
                        BulkCopyTimeout = 3600,
                        DestinationTableName = "PageRevision",
                    };

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

                    progress.CompleteRevisions(neededRevisions.Count);
                    this.revsAdded += neededRevisions.Count;
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
                if (bulkActivityID != -1)
                {
                    pump.CompleteActivity(bulkActivityID, neededRevisions.Count, null);
                }
            }
        }

        private void BulkMergeRevisions(DatabasePump pump, IInsertableProgress progress, SqlConnection conn)
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

            try
            {
                // bulk insert into the temporary table
                PageRevisionDataReader prdr = new (this.namespaceId, this.pageId, this.revisions.Values);
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
                        this.revsAdded = tableMerge.ExecuteNonQuery();
                        this.revsAlready = prdr.Count - this.revsAdded;
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
                            throw;
                        }
                    }
                    catch (InvalidOperationException ioe)
                    {
                        Console.WriteLine($"[[{this.pageName}]]: Exception during PageRevision merge of {prdr.Count} rows. {ioe.Message}\n{ioe.StackTrace}");
                        throw;
                    }
                    finally
                    {
                        pump.CompleteActivity(mergeActivity, this.revsAdded, mergeException?.Message);
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
            }
        }

        /// <summary>
        /// Insert the page object itself. There's no batching here, since pages are infrequent
        /// compared to all their revisions and text. Check to see if the page exists with a SELECT,
        /// then INSERT or UPDATE as needed because that proves to be faster.
        /// </summary>
        /// <param name="pump">Database pump object to record our activity</param>
        /// <param name="progress">implementation of IProgress interface to track our work</param>
        /// <param name="conn">Connection to use for insertion</param>
        private void InsertPage(DatabasePump pump, IInsertableProgress progress, SqlConnection conn)
        {
            long activityID = pump.StartActivity("Insert Page", this.namespaceId, this.pageId, 1);
            state = "reading pages";

            using var cmdSelect = new SqlCommand("SELECT PageID, NamespaceID FROM [Page] WHERE PageID = @PageID", conn);
            cmdSelect.Parameters.AddWithValue("@PageID", this.pageId);

            bool found = false;
            bool updateNeeded = false;
            using (var reader = cmdSelect.ExecuteReader())
            {
                while (reader.Read())
                {
                    found = true;
                    object o = reader["NamespaceID"];
                    if (o == null || o == DBNull.Value)
                    {
                        if (this.redirectTitle != null)
                            updateNeeded = true;
                    }
                    else
                    {
                        if (this.redirectTitle == null)
                            updateNeeded = true;
                    }
                }
            }

            state = "inserting page";
            Exception? exResult = null;
            int inserted = 0;
            try
            {
                // what did we find?
                if (!found)
                {
                    try
                    {
                        // not found, must insert
                        using var insertCmd = new SqlCommand("INSERT INTO [Page] (NamespaceID, PageID, PageName, RedirectTitle) VALUES (@NamespaceID, @PageID, @PageName, @RedirectTitle);", conn);
                        insertCmd.Parameters.AddWithValue("@NamespaceID", this.namespaceId);
                        insertCmd.Parameters.AddWithValue("@PageID", this.pageId);
                        insertCmd.Parameters.AddWithValue("@PageName", this.pageName);
                        insertCmd.Parameters.AddWithValue("@RedirectTitle", this.redirectTitle ?? (object)DBNull.Value);

                        insertCmd.ExecuteNonQuery();
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
                            throw new InvalidOperationException("unexpected duplicate when adding page");
                        }
                        else
                        {
                            exResult = null;
                            throw;
                        }
                    }
                }
                else if (updateNeeded)
                {
                    // it's there, but not current. Do an update
                    using var updateCmd = new SqlCommand("UPDATE [Page] SET RedirectTitle = @RedirectTitle WHERE PageID = @PageID AND NamespaceID = @NamespaceID;", conn);
                    updateCmd.Parameters.AddWithValue("@NamespaceID", this.namespaceId);
                    updateCmd.Parameters.AddWithValue("@PageID", this.pageId);
                    updateCmd.Parameters.AddWithValue("@RedirectTitle", this.redirectTitle ?? (object)DBNull.Value);

                    updateCmd.ExecuteNonQuery();
                    exResult = null;
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
            get { return "Page Inserter"; }
        }

        string IInsertable.ObjectTarget
        {
            get { return this.pageName; }
        }

        string IInsertable.ObjectState
        {
            get { return state; }
        }

        /// <summary>
        /// Gets the number of revisions we plan to insert.
        /// </summary>
        int IInsertable.RevisionCount
        {
            get { return this.revisions.Count; }
        }

        /// <summary>
        /// Gets the number of revisions remaining.
        /// </summary>
        int IInsertable.RemainingRevisionCount
        {
            get { return this.revisions.Count - this.revsAlready; }
        }
    }
}
