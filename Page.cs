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
            if (revisions.Count % 100 == 0)
            {
                CloseRevisions();
            }
        }

        public void CloseRevisions()
        {
            for (int n = 0; n < revisions.Count - 1; n++)
            {
                revisions.Values[n].Text = null;
            }
        }

        public void Insert(System.Data.SqlClient.SqlConnection conn, InsertableProgress progress)
        {
            _progress = progress;
            _progress.AddPendingRevisions(revisions.Count);

            // first, insert all the users
            BulkInsertUsers(conn);

            // then, insert the revisions themselves
            InsertRevisions(conn);

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

            try
            {
                // bulk insert into the temporary table
                UserDataReader udr = new UserDataReader(_insertedUserSet, revisions.Values);
                SqlBulkCopy sbc = new SqlBulkCopy(conn);
                sbc.DestinationTableName = tempTableName;
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("UserID", "UserID"));
                sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping("UserName", "UserName"));
                Trace.Assert(conn.State == ConnectionState.Open);
                sbc.WriteToServer(udr);
                Trace.Assert(conn.State == ConnectionState.Open);

                // merge up.
                SqlException mergeException = null;
                for (int retries = 10; retries > 0; retries--)
                {
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
                            System.Console.WriteLine("MERGE got an exception: {0}, {1}\n{2}",
                                sex.Number, sex.Source, sex.Message);
                            throw sex;
                        }
                    }
                }
                if (mergeException != null)
                {
                    System.Console.WriteLine("{0}: USER merge failed 10 times: {1}, {2}\n{3}",
                        _pageName, mergeException.Number, mergeException.Source, mergeException.Message);
                    throw mergeException;
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
            SqlCommand cmd = new SqlCommand("INSERT INTO [Page] (NamespaceID, PageID, PageName) VALUES (@NamespaceID, @PageID, @PageName);", conn);
            cmd.Parameters.AddWithValue("@NamespaceID", _namespaceId);
            cmd.Parameters.AddWithValue("@PageID", _pageId);
            cmd.Parameters.AddWithValue("@PageName", _pageName);

            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (SqlException sex)
            {
                if (sex.Number != 2601)
                {
                    throw sex;
                }
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
