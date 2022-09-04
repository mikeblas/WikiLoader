using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Diagnostics;
using System.Runtime.InteropServices.ObjectiveC;

namespace WikiReader
{
    /// <summary>
    /// Class that represents a single page. It directly holds the page name, the page's namespaceID,
    /// and the pageID. It contains a list of zero or more revisions of the page,
    /// each an instance of the PageRevision class.
    /// </summary>
    class Page : IInsertable
    {
        /// <summary>
        /// Name of the page we represent
        /// </summary>
        readonly string _pageName;

        /// <summary>
        /// Name of the page redirected to, if not null
        /// </summary>
        readonly string? _redirectTitle;

        /// <summary>
        /// Namespace which holds that page
        /// </summary>
        readonly int _namespaceId = 0;

        /// <summary>
        ///  ID number of this page
        /// </summary>
        readonly Int64 _pageId = 0;

        readonly ManualResetEvent completeEvent = new(false);

        public ManualResetEvent GetCompletedEvent()
        {
            return completeEvent;
        }

        /// <summary>
        /// set indicating which users we've already inserted
        /// </summary>
        static readonly HashSet<Int64> _insertedUserSet = new ();

        /// <summary>
        /// Map of revisions from RevisionID to the PageRevision at that ID
        /// </summary>
        private readonly SortedList<Int64, PageRevision> _revisions = new ();

        private int _usersAdded = 0;
        private int _usersAlready = 0;
        private int _revsAdded = 0;
        private int _revsAlready = 0;

        /// <summary>
        /// Create a new Page instance
        /// </summary>
        /// <param name="namespaceId">namespaceId where this page lives</param>
        /// <param name="pageId">pageID for this page</param>
        /// <param name="pageName">name of this page</param>
        public Page(int namespaceId, Int64 pageId, string pageName, string? redirectName)
        {
            _namespaceId = namespaceId;
            _pageName = pageName;
            _pageId = pageId;
            _redirectTitle = redirectName;
        }

        /// <summary>
        /// add a revision to the history of this page in memory
        /// </summary>
        /// <param name="pr"></param>
        public void AddRevision(PageRevision pr)
        {
            if (_revisions.ContainsKey(pr.RevisionId))
            {
                System.Console.WriteLine($"Page {_pageName} already has revision {pr.RevisionId}");
                System.Console.WriteLine($"current: {_revisions[pr.RevisionId].TimeStamp} with id {_revisions[pr.RevisionId].RevisionId}, parent id {_revisions[pr.RevisionId].ParentRevisionId}");
                System.Console.WriteLine($"    new: {pr.TimeStamp} with id {pr.RevisionId}, parent id {pr.ParentRevisionId}");
            }
            _revisions.Add(pr.RevisionId, pr);

            // always keep first and last revisions
            // keep every 100th revision
            // keep the most recent revision
            int nCandidate = _revisions.Count - 2;
            if (nCandidate > 0 && nCandidate % 100 != 0)
            {
                _revisions.Values[nCandidate].Text = null;
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
            progress.AddPendingRevisions(_revisions.Count);

            // first, insert all the users
            BulkInsertUsers(pump, conn);

            // insert the page record itself
            InsertPage(pump, progress, conn);

            // then, insert the revisions
            SelectAndInsertRevisions(pump, progress, previous, conn);

            // insert the text that we have
            BulkInsertRevisionText(pump, conn);

            System.Console.WriteLine(
                $"{_pageName}\n" +
                $"   {_revsAdded} revisions added, {_revsAlready} revisions exist\n" +
                $"   {_usersAdded} users added, {_usersAlready} users exist");
        }

        /// <summary>
        /// Insert the users who have edited this page. 
        /// </summary>
        /// <param name="pump">DatabasePump to record our activity</param>
        /// <param name="conn">connection to use for insertion</param>
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
                UserDataReader udr = new(_insertedUserSet, _revisions.Values);
                SqlBulkCopy sbc = new(conn);

                bulkActivity = pump.StartActivity("Bulk Insert Users", _namespaceId, _pageId, udr.Count);

                sbc.DestinationTableName = tempTableName;
                sbc.ColumnMappings.Add("UserID", "UserID");
                sbc.ColumnMappings.Add("UserName", "UserName");
                Trace.Assert(conn.State == ConnectionState.Open);
                sbc.WriteToServer(udr);
                Trace.Assert(conn.State == ConnectionState.Open);

                pump.CompleteActivity(bulkActivity, _insertedUserSet.Count, "inserted users");
                bulkActivity = -1;

                // merge up.
                SqlException? mergeException = null;
                for (int retries = 10; retries > 0; retries--)
                {
                    long mergeActivity = pump.StartActivity("Merge Users", _namespaceId, _pageId, udr.Count);
                    try
                    {
                        using var tableMerge = new SqlCommand(
                            "MERGE INTO [User] WITH (HOLDLOCK) " +
                            $"USING [{tempTableName}] AS SRC ON [User].UserID = SRC.UserID " +
                            "WHEN NOT MATCHED THEN " +
                            " INSERT (UserID, UserName) VALUES (SRC.UserID, SRC.UserName);", conn);
                        tableMerge.CommandTimeout = 300;
                        _usersAdded = tableMerge.ExecuteNonQuery();
                        _usersAlready = udr.Count - _usersAdded;
                        mergeException = null;
                        break;
                    }
                    catch (SqlException sex)
                    {
                        if (sex.Number == 1205)
                        {
                            System.Console.WriteLine($"{_pageName}: Deadlock encountered during USER merge of {udr.Count} rows. Retry {retries}");
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else if (sex.Number == -2)
                        {
                            System.Console.WriteLine($"{_pageName}: Timeout encountered during USER merge of {udr.Count} rows. Retry {retries}");
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else
                        {
                            System.Console.WriteLine($"{_pageName}: Exception during USER merge of {udr.Count} rows. {sex.Number}: {sex.Source}\n{sex.Message}");
                            throw sex;
                        }
                    }
                    catch (InvalidOperationException ioe)
                    {
                        System.Console.WriteLine($"{_pageName}: Exception during PageRevision merge of {udr.Count} rows. {ioe.Message}\n{ioe.StackTrace}");
                        throw ioe;
                    }
                    finally
                    {
                        pump.CompleteActivity(mergeActivity, _usersAdded, mergeException?.Message);
                    }

                }
                if (mergeException != null)
                {
                    System.Console.WriteLine($"{_pageName}: USER merge failed 10 times: {mergeException.Number}, {mergeException.Source}\n{mergeException.Message}");
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
            PageRevisionTextDataReader prtdr = new(_namespaceId, _pageId, _revisions.Values);

            long bulkActivity = pump.StartActivity("Bulk Insert Text", _namespaceId, _pageId, prtdr.Count);

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
                    long mergeActivity = pump.StartActivity("Merge Text", _namespaceId, _pageId, prtdr.Count);
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
                        _usersAdded = tableMerge.ExecuteNonQuery();
                        _usersAlready = prtdr.Count - _usersAdded;
                        mergeException = null;
                        break;
                    }
                    catch (SqlException sex)
                    {
                        if (sex.Number == 1205)
                        {
                            System.Console.WriteLine($"{_pageName}: Deadlock encountered during TEXT merge of {prtdr.Count} rows. Retry {retries}");
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else if (sex.Number == -2)
                        {
                            System.Console.WriteLine($"{_pageName}: Timeout encountered during TEXT merge of {prtdr.Count} rows. Retry {retries}");
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else
                        {
                            System.Console.WriteLine($"{_pageName}: Exception during TEXT merge of {prtdr.Count} rows. {sex.Number}: {sex.Source}\n{sex.Message}");
                            throw sex;
                        }
                    }
                    catch (InvalidOperationException ioe)
                    {
                        System.Console.WriteLine($"{_pageName}: Exception during TEXT merge of {prtdr.Count} rows. {ioe.Message}\n{ioe.StackTrace}");
                        throw ioe;
                    }
                    finally
                    {
                        pump.CompleteActivity(mergeActivity, _usersAdded, mergeException?.Message);
                    }

                }
                if (mergeException != null)
                {
                    System.Console.WriteLine($"{_pageName}: TEXT merge failed 10 times: {mergeException.Number}, {mergeException.Source}\n{mergeException.Message}");
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
            long checkActivityID = pump.StartActivity("Check existing PageRevisions", null, _pageId, _revisions.Count);

            // build a hash set of all the known revisions of this page
            HashSet<Int64> knownRevisions = new();

            using var cmdSelect = new SqlCommand("select PageRevisionID FROM PageRevision WHERE PageID = @PageID", conn);
            cmdSelect.Parameters.AddWithValue("@NamespaceID", _namespaceId);
            cmdSelect.Parameters.AddWithValue("@PageID", _pageId);
            cmdSelect.CommandTimeout = 3600;

            using (var reader = cmdSelect.ExecuteReader())
            {
                while (reader.Read())
                    knownRevisions.Add((Int64)reader["PageRevisionID"]);
            }

            SortedList<Int64, PageRevision> neededRevisions = new();

            foreach ((Int64 revID, var rev) in _revisions)
            {
                if (knownRevisions.Contains(revID))
                {
                    progress.CompleteRevisions(1);
                    _revsAlready += 1;
                    continue;
                }

                neededRevisions.Add(revID, rev);
            }

            pump.CompleteActivity(checkActivityID, _revisions.Count, "Processed");


            BulkInsertPageRevisions(pump, progress, previous, neededRevisions, conn);
        }


        private void BulkInsertPageRevisions(DatabasePump pump, IInsertableProgress progress, IInsertable? previous, SortedList<Int64, PageRevision> neededRevisions, SqlConnection conn)
        {
            long bulkActivityID = -1;

            // now, bulk insert the ones we didin't find
            try
            {
                if (neededRevisions.Count > 0)
                {
                    bulkActivityID = pump.StartActivity("Bulk Insert PageRevisions", _namespaceId, _pageId, neededRevisions.Count);

                    // bulk insert into the temporary table
                    PageRevisionDataReader prdr = new(_namespaceId, _pageId, neededRevisions.Values);
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
                    _revsAdded += neededRevisions.Count;
                }

                // wait until the previous revision is done, if we've got one
                if (previous != null)
                {
                    // Console.WriteLine($"[[{(this as IInsertable).ObjectName}]] waiting on [[{previous.ObjectName}]]");

                    ManualResetEvent mre = previous.GetCompletedEvent();
                    mre.WaitOne();
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
                completeEvent.Set();
            }
        }

        private void BulkMergeRevisions(DatabasePump pump, IInsertableProgress progress, IInsertable? previous, SqlConnection conn)
        {
            // build a unique temporary table name
            String tempTableName = $"#Revisions_{System.Environment.MachineName}_{Environment.CurrentManagedThreadId}";

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
                PageRevisionDataReader prdr = new(_namespaceId, _pageId, _revisions.Values);
                var sbc = new SqlBulkCopy(conn);

                bulkActivityID = pump.StartActivity("Bulk Insert PageRevisions", _namespaceId, _pageId, prdr.Count);

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
                    long mergeActivity = pump.StartActivity("Merge PageRevisions", _namespaceId, _pageId, prdr.Count);
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
                        _revsAdded = tableMerge.ExecuteNonQuery();
                        _revsAlready = prdr.Count - _revsAdded;
                        mergeException = null;
                        progress.CompleteRevisions(prdr.Count);
                        break;
                    }
                    catch (SqlException sex)
                    {
                        if (sex.Number == 1205)
                        {
                            System.Console.WriteLine($"{_pageName}: Deadlock encountered during PageRevision merge of {prdr.Count} rows. Retry {retries}");
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else if (sex.Number == -2)
                        {
                            System.Console.WriteLine($"{_pageName}: Timeout encountered during PageRevision merge of {prdr.Count} rows. Retry {retries}");
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else
                        {
                            System.Console.WriteLine($"{_pageName}: Exception during PageRegision merge of {prdr.Count} rows. {sex.Number}: {sex.Source}\n{sex.Message}");
                            throw sex;
                        }
                    }
                    catch (InvalidOperationException ioe)
                    {
                        System.Console.WriteLine($"{_pageName}: Exception during PageRevision merge of {prdr.Count} rows. {ioe.Message}\n{ioe.StackTrace}");
                        throw ioe;
                    }
                    finally
                    {
                        pump.CompleteActivity(mergeActivity, _revsAdded, mergeException?.Message);
                    }
                }
                if (mergeException != null)
                {
                    System.Console.WriteLine($"{_pageName}: PageRevision merge failed 10 times: {mergeException.Number}, {mergeException.Source}\n{mergeException.Message}");
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
                completeEvent.Set();
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
            long activityID = pump.StartActivity("Insert Page", _namespaceId, _pageId, 1);
            using var cmd = new SqlCommand("INSERT INTO [Page] (NamespaceID, PageID, PageName, RedirectTitle) VALUES (@NamespaceID, @PageID, @PageName, @RedirectTitle);", conn);
            cmd.Parameters.AddWithValue("@NamespaceID", _namespaceId);
            cmd.Parameters.AddWithValue("@PageID", _pageId);
            cmd.Parameters.AddWithValue("@PageName", _pageName);
            cmd.Parameters.AddWithValue("@RedirectTitle", _redirectTitle ?? (object)DBNull.Value);

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
                    System.Console.WriteLine($"Error: page name is too long at {_pageName.Length} characters");
                else if (sex.Number == 2601 || sex.Number == 2627)
                {
                    // duplicate! we'll do an update instead
                    using var updateCmd = new SqlCommand("UPDATE [Page] SET RedirectTitle = @RedirectTitle WHERE PageID = @PageID AND NamespaceID = @NamespaceID;", conn);
                    updateCmd.Parameters.AddWithValue("@NamespaceID", _namespaceId);
                    updateCmd.Parameters.AddWithValue("@PageID", _pageId);
                    updateCmd.Parameters.AddWithValue("@RedirectTitle", _redirectTitle ?? (object)DBNull.Value);

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
        /// Get our object name; this is used to name the connection in SQL Server
        /// </summary>
        String IInsertable.ObjectName
        {
            get { return "Page Inserter (" + _pageName + ")"; }
        }

        /// <summary>
        /// How many revisions do we plan to insert?
        /// </summary>
        int IInsertable.RevisionCount
        {
            get { return _revisions.Count; }
        }
    }

}
