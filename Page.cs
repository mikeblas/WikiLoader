using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WikiReader
{
    /// <summary>
    /// Class that represents a single page. It directly holds the page name, the page's namespaceID,
    /// and the pageID. It contains a list of zero or more revisions of the page,
    /// each an instance of the PageRevision class.
    /// </summary>
    class Page
    {
        String _pageName = null;
        int _namespaceId = 0;
        Int64 _pageId = 0;

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
                revisions.Values[n].text = null;
            }
        }
    }

}
