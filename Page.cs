using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Sql;
using System.Data.SqlClient;

namespace WikiReader
{
    /// <summary>
    /// Class that represents a single page. It directly holds the page name, the page's namespaceID,
    /// and the pageID. It contains a list of zero or more revisions of the page,
    /// each an instance of the PageRevision class.
    /// </summary>
    class Page : Insertable
    {
        String _pageName = null;
        int _namespaceId = 0;
        Int64 _pageId = 0;
        static HashSet<Int64> _insertedSet = new HashSet<Int64>();

        SortedList<Int64, PageRevision> revisions = new SortedList<Int64, PageRevision>();

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

        public void Insert(System.Data.SqlClient.SqlConnection conn)
        {
            // first, insert all the users
            InsertUsers(conn);

            // then, insert the revisions themselves
            InsertRevisions(conn);

            // finally, insert the page itself
            InsertPage(conn);
        }

        private void InsertUsers( SqlConnection conn)
        {
            int usersAdded = 0;
            int usersAlready = 0;
            Int64 lastUserID = -1;
            SqlCommand cmd = new SqlCommand("INSERT INTO [User] (UserID, UserName) VALUES (@ID, @Name);", conn);
            foreach (PageRevision pr in revisions.Values)
            {
                lock (_insertedSet)
                {
                    if (lastUserID != -1)
                    {
                        _insertedSet.Add(lastUserID);
                    }
                    lastUserID = -1;
                    // if the contributor was deleted, skip it
                    if (null == pr.Contributor)
                        continue;

                    // if we're not anonymous and we've already seen this ID, then skip
                    if (false == pr.Contributor.IsAnonymous && _insertedSet.Contains(pr.Contributor.ID))
                        continue;
                }

                // if we're not anonymous, insert this user
                if (false == pr.Contributor.IsAnonymous)
                {
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@ID", pr.Contributor.ID);
                    cmd.Parameters.AddWithValue("@Name", pr.Contributor.Name);

                    try
                    {
                        cmd.ExecuteNonQuery();
                        usersAdded += 1;
                        lastUserID = pr.Contributor.ID;
                    }
                    catch (SqlException sex)
                    {
                        if (sex.Number == 2601)
                        {
                            usersAlready += 1;
                            lastUserID = pr.Contributor.ID;
                        }
                        else
                        {
                            throw sex;
                        }
                    }
                }
            }

            // now that we're done looping, we might have a lastUserID left over
            // if so, mark it in our inserted set
            if (lastUserID != -1)
            {
                lock (_insertedSet)
                {
                    _insertedSet.Add(lastUserID);
                }
            }

            System.Console.WriteLine("{2}: {0} users added, {1} already there", usersAdded, usersAlready, _pageName);
        }

        private void InsertRevisions(SqlConnection conn)
        {
            if (_pageId == 600)
                System.Console.WriteLine("This one!");

            int revsAdded = 0;
            int revsAlready = 0;
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
                    revsAdded += 1;
                }
                catch (SqlException sex)
                {
                    if (sex.Number == 2601)
                    {
                        revsAlready += 1;
                    }
                    else
                    {
                        throw sex;
                    }
                }
            }

            System.Console.WriteLine("{0}: {1} revisions added, {2} revisions exist", _pageName, revsAdded, revsAlready);
        }

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
    }

}
