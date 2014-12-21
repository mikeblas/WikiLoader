using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Threading;
using System.Diagnostics;

namespace WikiReader
{
    /// <summary>
    /// Class that represents a single page. It directly holds the page name, the page's namespaceID,
    /// and the pageID. It contains a list of zero or more revisions of the page,
    /// each an instance of the PageRevision class.
    /// </summary>
    class Page : Insertable
    {
        /// <summary>
        /// Name of the page we represent
        /// </summary>
        String _pageName = null;

        /// <summary>
        /// Namespace which holds that page
        /// </summary>
        int _namespaceId = 0;

        /// <summary>
        ///  ID number of this page
        /// </summary>
        Int64 _pageId = 0;

        InsertableProgress _progress;

        DatabasePump _pump;

        /// <summary>
        /// set indicating which users we've already inserted
        /// </summary>
        static HashSet<Int64> _insertedUserSet = new HashSet<Int64>();

        /// <summary>
        /// Map of revisions from RevisionID to the PageRevision at that ID
        /// </summary>
        SortedList<Int64, PageRevision> revisions = new SortedList<Int64, PageRevision>();

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
        public Page(int namespaceId, Int64 pageId, String pageName)
        {
            _namespaceId = namespaceId;
            _pageName = pageName;
            _pageId = pageId;
        }

        /// <summary>
        /// add a revision to the history of this page
        /// </summary>
        /// <param name="pr"></param>
        public void AddRevision(PageRevision pr)
        {
            if (revisions.ContainsKey(pr.revisionId))
            {
                System.Console.WriteLine("Page {0} already has revision {1}", _pageName, pr.revisionId);
                System.Console.WriteLine("current: {0} with id {1}, parent id {2}",
                    revisions[pr.revisionId].timestamp, revisions[pr.revisionId].revisionId, revisions[pr.revisionId].parentRevisionId);
                System.Console.WriteLine("    new: {0} with id {1}, parent id {2}",
                    pr.timestamp, pr.revisionId, pr.parentRevisionId);
            }
            revisions.Add(pr.revisionId, pr);

            // always keep first and last revisions
            // keep every 100th revision
            // keep the most recent revision
            int nCandidate = revisions.Count - 2;
            if (nCandidate > 0 && nCandidate % 100 != 0)
            {
                revisions.Values[nCandidate].Text = null;
            }
        }

        public void Insert(DatabasePump pump, SqlConnection conn, InsertableProgress progress)
        {
            _pump = pump;
            _progress = progress;
            _progress.AddPendingRevisions(revisions.Count);

            // first, insert all the users
            BulkInsertUsers(conn);

            // then, insert the revisions themselves
            BulkInsertRevisions(conn);

            // finally, insert the page itself
            InsertPage(conn);

            System.Console.WriteLine(
                "{0}\n" +
                "   {1} revisions added, {2} revisions exist\n" +
                "   {3} users added, {4} users exist", 
                _pageName,
                _revsAdded, _revsAlready,
                _usersAdded, _usersAlready);
        }

        private void BulkInsertUsers(SqlConnection conn)
        {
            // build a unique temporary table name
            String tempTableName = String.Format("#Users_{0}_{1}", System.Environment.MachineName, Thread.CurrentThread.ManagedThreadId);

            // create that temporary table
            SqlCommand tableCreate = new SqlCommand(
                "CREATE TABLE [" + tempTableName + "] (" +
                "	UserID BIGINT NOT NULL," +
                "	UserName NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL );", conn);
            tableCreate.ExecuteNonQuery();

            long bulkActivity = -1;

            try
            {
                // bulk insert into the temporary table
                UserDataReader udr = new UserDataReader(_insertedUserSet, revisions.Values);
                SqlBulkCopy sbc = new SqlBulkCopy(conn);

                bulkActivity = _pump.StartActivity("Bulk Insert Users", _namespaceId, _pageId, udr.Count);

                sbc.DestinationTableName = tempTableName;
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("UserID", "UserID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("UserName", "UserName"));
                Trace.Assert(conn.State == ConnectionState.Open);
                sbc.WriteToServer(udr);
                Trace.Assert(conn.State == ConnectionState.Open);

                _pump.CompleteActivity(bulkActivity, null, null);
                bulkActivity = -1;

                // merge up.
                SqlException mergeException = null;
                for (int retries = 10; retries > 0; retries--)
                {
                    long mergeActivity = _pump.StartActivity("Merge Users", _namespaceId, _pageId, udr.Count);
                    try
                    {
                        SqlCommand tableMerge = new SqlCommand(
                            "MERGE INTO [User] WITH (HOLDLOCK) " +
                            "USING [" + tempTableName + "] AS SRC ON [User].UserID = SRC.UserID " +
                            "WHEN NOT MATCHED THEN " +
                            " INSERT (UserID, UserName) VALUES (SRC.UserID, SRC.UserName);", conn);
                        _usersAdded = tableMerge.ExecuteNonQuery();
                        _usersAlready = udr.Count - _usersAdded;
                        mergeException = null;
                        break;
                    }
                    catch (SqlException sex)
                    {
                        if (sex.Number == 1205)
                        {
                            System.Console.WriteLine("{0}: Deadlock encountered during USER merge of {2} rows. Retry {1}",
                                _pageName, retries, udr.Count);
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else if (sex.Number == -2)
                        {
                            System.Console.WriteLine("{0}: Timeout encountered during USER merge of {2} rows. Retry {1}",
                                _pageName, retries, udr.Count);
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else
                        {
                            System.Console.WriteLine("{0}: Exception during USER merge of {1} rows. {2}: {3}\n{4}",
                                _pageName, udr.Count,
                                sex.Number, sex.Source, sex.Message);
                            throw sex;
                        }
                    }
                    catch (InvalidOperationException ioe)
                    {
                        System.Console.WriteLine("{0}: Exception during PageRevision merge of {1} rows. {2}\n{3}",
                            _pageName, udr.Count,
                            ioe.Message, ioe.StackTrace);
                        throw ioe;
                    }
                    finally
                    {
                        _pump.CompleteActivity(mergeActivity, _usersAdded, (mergeException == null) ? null : mergeException.Message);
                    }

                }
                if (mergeException != null)
                {
                    System.Console.WriteLine("{0}: USER merge failed 10 times: {1}, {2}\n{3}",
                        _pageName, mergeException.Number, mergeException.Source, mergeException.Message);
                    throw mergeException;
                }
            }
            catch (Exception ex)
            {
                if (bulkActivity != -1)
                {
                    _pump.CompleteActivity(bulkActivity, null, ex.Message);
                    bulkActivity = -1;
                }
            }
            finally
            {
                // drop the temporary table
                SqlCommand tableDrop = new SqlCommand("DROP TABLE [" + tempTableName + "];", conn);
                tableDrop.ExecuteNonQuery();
            }
        }

        private void BulkInsertRevisions(SqlConnection conn)
        {
            // build a unique temporary table name
            String tempTableName = String.Format("#Revisions_{0}_{1}", System.Environment.MachineName, Thread.CurrentThread.ManagedThreadId);

            // create that temporary table
            SqlCommand tableCreate = new SqlCommand(
                "CREATE TABLE [" + tempTableName + "] (" +
                "   NamespaceID INT NOT NULL, " +
                "   PageID BIGINT NOT NULL, " +
                "   PageRevisionID BIGINT NOT NULL, " +
                "   ParentPageRevisionID BIGINT NOT NULL, " +
                "   RevisionWhen DATETIME NOT NULL, " +
                "   ContributorID BIGINT, " +
                "   IPAddress VARCHAR(39), " +
                "   Comment NVARCHAR(255), " +
                "   ArticleText TEXT, " +
                "   IsMinor BIT NOT NULL, " +
                "   ArticleTextLength INT NOT NULL, " +
                "   TextDeleted BIT NOT NULL, " +
                "   UserDeleted BIT NOT NULL );", conn);
            tableCreate.ExecuteNonQuery();

            long bulkActivityID = -1;

            try {

                // bulk insert into the temporary table
                PageRevisionDataReader prdr = new PageRevisionDataReader(_namespaceId, _pageId, revisions.Values);
                SqlBulkCopy sbc = new SqlBulkCopy(conn);

                bulkActivityID = _pump.StartActivity("Bulk Insert PageRevisions", _namespaceId, _pageId, prdr.Count);

                sbc.DestinationTableName = tempTableName;
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("PageID", "PageID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("NamespaceID", "NamespaceID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("PageRevisionID", "PageRevisionID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ParentPageRevisionID", "ParentPageRevisionID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevisionWhen", "RevisionWhen"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ContributorID", "ContributorID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Comment", "Comment"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ArticleText", "ArticleText"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("IsMinor", "IsMinor"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ArticleTextLength", "ArticleTextLength"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("UserDeleted", "UserDeleted"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("TextDeleted", "TextDeleted"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("IPAddress", "IPAddress"));

                Trace.Assert(conn.State == ConnectionState.Open);
                sbc.WriteToServer(prdr);
                Trace.Assert(conn.State == ConnectionState.Open);

                _pump.CompleteActivity(bulkActivityID, null, null);
                bulkActivityID = -1;

                // merge up.
                SqlException mergeException = null;
                for (int retries = 10; retries > 0; retries--)
                {
                    long mergeActivity = _pump.StartActivity("Merge PageRevisions", _namespaceId, _pageId, prdr.Count);
                    try
                    {
                        SqlCommand tableMerge = new SqlCommand(
                            "  MERGE INTO [PageRevision] WITH (HOLDLOCK)" +
                            "  USING [" + tempTableName + "] AS SRC " +
                            "     ON [PageRevision].PageRevisionID = SRC.PageRevisionID " +
                            "	AND [PageRevision].PageID = SRC.PageID " +
                            "	AND [PageRevision].NamespaceID = SRC.NamespaceID " +
                            "WHEN NOT MATCHED THEN " +
                            "INSERT (NamespaceID, PageID, PageRevisionID, ParentPageRevisionID, " +
                            "        RevisionWhen, ContributorID, IPAddress, Comment, " +
                            "		ArticleText, IsMinor, ArticleTextLength, TextDeleted, UserDeleted) " +
                            "VALUES (SRC.NamespaceID, SRC.PageID, SRC.PageRevisionID, SRC.ParentPageRevisionID, " +
                            "	SRC.RevisionWhen, SRC.ContributorID, SRC.IPAddress, SRC.Comment, " +
                            "	SRC.ArticleText, SRC.IsMinor, SRC.ArticleTextLength, SRC.TextDeleted, SRC.UserDeleted);",
                            conn);
                        tableMerge.CommandTimeout = 300;
                        _revsAdded = tableMerge.ExecuteNonQuery();
                        _revsAlready = prdr.Count - _revsAdded;
                        mergeException = null;
                        _progress.CompleteRevisions(prdr.Count);
                        break;
                    }
                    catch (SqlException sex)
                    {
                        if (sex.Number == 1205)
                        {
                            System.Console.WriteLine("{0}: Deadlock encountered during PageRevision merge of {2} rows. Retry {1}",
                                _pageName, retries, prdr.Count);
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else if (sex.Number == -2)
                        {
                            System.Console.WriteLine("{0}: Timeout encountered during PageRevision merge of {2} rows. Retry {1}",
                                _pageName, retries, prdr.Count);
                            mergeException = sex;
                            Thread.Sleep(2500);
                            continue;
                        }
                        else
                        {
                            System.Console.WriteLine("{0}: Exception during PageRegision merge of {1} rows. {2}: {3}\n{4}",
                                _pageName, prdr.Count,
                                sex.Number, sex.Source, sex.Message);
                            throw sex;
                        }
                    }
                    catch (InvalidOperationException ioe)
                    {
                        System.Console.WriteLine("{0}: Exception during PageRevision merge of {1} rows. {2}\n{3}",
                            _pageName, prdr.Count,
                            ioe.Message, ioe.StackTrace);
                        throw ioe;
                    }
                    finally
                    {
                        _pump.CompleteActivity(mergeActivity, _revsAdded, (mergeException == null) ? null : mergeException.Message);
                    }
                }
                if (mergeException != null)
                {
                    System.Console.WriteLine("{0}: PageRevision merge failed 10 times: {1}, {2}\n{3}",
                        _pageName, mergeException.Number, mergeException.Source, mergeException.Message);
                    throw mergeException;
                }
            }
            catch (Exception ex)
            {
                if (bulkActivityID != -1)
                {
                    _pump.CompleteActivity(bulkActivityID, null, ex.Message);
                    bulkActivityID = -1;
                }
            }
            finally
            {
                // drop the temporary table
                SqlCommand tableDrop = new SqlCommand("DROP TABLE [" + tempTableName + "];", conn);
                tableDrop.ExecuteNonQuery();
            }
        }


        private void InsertRevisions(SqlConnection conn)
        {
            SqlCommand cmd = new SqlCommand(
                "INSERT INTO [PageRevision] (NamespaceID, PageID, PageRevisionID, ParentPageRevisionID, RevisionWhen, ContributorID, " +
                "   Comment, ArticleText, IsMinor, ArticleTextLength, UserDeleted, TextDeleted, IPAddress) " +
                " VALUES (@NamespaceID, @PageID, @RevID, @ParentRevID, @RevWhen, @ContributorID, @Comment, " +
                "       @ArticleText, @IsMinor, @ArticleTextLen, @UserDeleted, @TextDeleted, @IPAddress);", conn);

            foreach (PageRevision pr in revisions.Values)
            {
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@NamespaceID", _namespaceId);
                cmd.Parameters.AddWithValue("@PageID", _pageId);
                cmd.Parameters.AddWithValue("@RevID", pr.revisionId);
                cmd.Parameters.AddWithValue("@ParentRevId", pr.parentRevisionId);
                cmd.Parameters.AddWithValue("@RevWhen", pr.timestamp);

                if (pr.Contributor == null)
                {
                    // deleted contributor
                    cmd.Parameters.AddWithValue("@ContributorID", DBNull.Value);
                    cmd.Parameters.AddWithValue("@IPAddress", DBNull.Value);
                    cmd.Parameters.AddWithValue("@UserDeleted", true);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@UserDeleted", false);
                    if (pr.Contributor.IsAnonymous)
                    {
                        cmd.Parameters.AddWithValue("@ContributorID", DBNull.Value);
                        cmd.Parameters.AddWithValue("@IPAddress", pr.Contributor.IPAddress);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@ContributorID", pr.Contributor.ID);
                        cmd.Parameters.AddWithValue("@IPAddress", DBNull.Value);
                    }
                }

                if (null == pr.Comment)
                {
                    cmd.Parameters.AddWithValue("@Comment", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@Comment", pr.Comment);
                }
                cmd.Parameters.AddWithValue("@TextDeleted", pr.TextDeleted);

                if (pr.Text == null)
                {
                    cmd.Parameters.AddWithValue("@ArticleText", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@ArticleText", pr.Text);
                }
                cmd.Parameters.AddWithValue("@IsMinor", pr.IsMinor);
                cmd.Parameters.AddWithValue("@ArticleTextLen", pr.TextLength);

                try
                {
                    cmd.ExecuteNonQuery();
                    _revsAdded += 1;
                    _progress.CompleteRevisions(1);
                }
                catch (SqlException sex)
                {
                    if (sex.Number == 2601)
                    {
                        _revsAlready += 1;
                        _progress.CompleteRevisions(1);
                    }
                    else
                    {
                        throw sex;
                    }
                }
            }
        }

        /// <summary>
        /// Insert the page object itself
        /// </summary>
        /// <param name="conn">Connection to use for insertion</param>
        private void InsertPage(SqlConnection conn)
        {
            long activityID = _pump.StartActivity("Insert Page", _namespaceId, _pageId, 1);
            SqlCommand cmd = new SqlCommand("INSERT INTO [Page] (NamespaceID, PageID, PageName) VALUES (@NamespaceID, @PageID, @PageName);", conn);
            cmd.Parameters.AddWithValue("@NamespaceID", _namespaceId);
            cmd.Parameters.AddWithValue("@PageID", _pageId);
            cmd.Parameters.AddWithValue("@PageName", _pageName);

            Exception exResult = null;
            int inserted = 0;
            try
            {
                cmd.ExecuteNonQuery();
                _progress.InsertedPages(1);
                inserted++;
            }
            catch (SqlException sex)
            {
                exResult = sex;
                if (sex.Number != 2601)
                {
                    exResult = null;
                    throw sex;
                }
            }
            catch (Exception ex)
            {
                exResult = ex;
            }
            finally
            {
                _pump.CompleteActivity(activityID, inserted, (exResult == null) ? null : exResult.Message);
            }

        }

        /// <summary>
        /// Get our object name; this is used to name the connection in SQL Server
        /// </summary>
        String Insertable.ObjectName
        {
            get { return "Page Inserter (" + _pageName + ")"; }
        }

        /// <summary>
        /// How many revisions do we plan to insert?
        /// </summary>
        int Insertable.RevisionCount
        {
            get { return revisions.Count; }
        }
    }

}
