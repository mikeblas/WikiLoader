using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.IO;

// https://en.wikipedia.org/wiki/Wikipedia:Database_download#XML_schema

namespace WikiReader
{
    class LargestString
    {
        String _str;
        String _name;

        LargestString(String name)
        {
            _name = name;
        }

        public void test(String str)
        {
            if (_str == null || str.Length > _str.Length)
            {
                _str = str;
            }
        }

        public String Name
        {
            get { return _name; }
        }

        public String Largest
        {
            get { return _str; }
        }
    }

    /// <summary>
    /// Represents a user; contains their name (as a string) and an ID integer.
    /// </summary>
    class User
    {
        String _userName = null;
        Int64 _userId = 0;

        /// <summary>
        /// Create a User object
        /// </summary>
        /// <param name="userId">user ID integer</param>
        /// <param name="userName">user name as a string</param>
        public User( Int64 userId, String userName )
        {
            _userId = userId;
            _userName = userName;
        }
    }

    /// <summary>
    /// Represents a revision to a Page.
    /// 
    /// https://www.mediawiki.org/wiki/Manual:Revision_table
    /// 
    /// </summary>
    class PageRevision
    {
        Int64 _parentRevisionId = 0;
        Int64 _revisionId = 0;
        DateTime _timestamp = DateTime.MinValue;
        User _contributor = null;
        String _comment = null;
        String _text = null;
        int _textLength = 0;
        bool _minor = false;
        bool _commentDeleted = false;
        bool _textDeleted = false;

        public PageRevision(Int64 parentRevisionId, Int64 revisionId, DateTime timestamp, User contributor, String comment, String text, bool minor)
        {
            _parentRevisionId = parentRevisionId;
            _revisionId = revisionId;
            _timestamp = timestamp;
            _contributor = contributor;
            _comment = comment;
            _text = text;
            _textLength = _text.Length;
            _minor = minor;
        }

        public Int64 revisionId
        {
            get { return _revisionId; }
        }

        public Int64 parentRevisionId
        {
            get { return _parentRevisionId; }
        }

        public User contributor
        {
            get { return _contributor; }
        }

        public DateTime timestamp
        {
            get { return _timestamp; }
        }

        public String comment
        {
            get { return _comment; }
            set { _comment = comment; }
        }

        public String text
        {
            get { return _text; }
            set {
                _text = value;
                if (value != null) _textLength = _text.Length; 
            }
        }
    }

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
            for ( int n = 0; n < revisions.Count - 1; n++ )
            {
                revisions.Values[n].text = null;
            }

        }
    }

    /// <summary>
    /// Represense a namespace.
    /// 
    /// Includes the name and Id of the namespace. Also manages a count
    /// which is used to tally the number of articles read per namespace.
    /// </summary>
    class NamespaceInfo
    {
        int _pageCount;
        Int64 _namespaceId;
        String _name;
        /// <summary>
        /// Create a new namespace instance.
        /// pageCount is initalized to one.
        /// 
        /// </summary>
        /// <param name="name">Name of this namespace</param>
        /// <param name="namespaceId">ID for this namespace</param>
        public NamespaceInfo(String name, Int64 namespaceId)
        {
            _name = name;
            _pageCount = 1;
            _namespaceId = namespaceId;
        }

        public String Name
        {
            get { return _name; }
        }

        public int PageCount
        {
            get { return _pageCount; }
        }

        public void IncrementCount()
        {
            _pageCount += 1;
        }
    }


    class Program
    {
        String _fileName;

        Program(String fileName)
        {
            _fileName = fileName;
        }

        static void Main(string[] args)
        {
            String fileName = @"C:\Junk\enwiki-latest-pages-meta-history1.xml-p000000010p000002933";
            if (args.Length >= 1)
                fileName = args[0];

            Program p = new Program(fileName);
            p.Parse();
        }


        void Parse()
        {
            FileStream s = File.OpenRead(_fileName);
            XmlReader reader = XmlReader.Create(s, null);

            String pageName = null;
            Int64 revisionId = 0;
            Int64 contributorId = 0;
            Int64 parentRevisionId = 0;
            String contributorIp = null;
            int namespaceId = 0;
            int pageId = 0;
            Boolean inRevision = false;
            Boolean inContributor = false;
            DateTime timestamp = DateTime.MinValue;
            String comment = null;
            String articleText = null;
            String contributorUserName = null;
            int revisionCount = 0;
            int minorRevisionCount = 0;
            int totalPages = 0;
            int totalRevisions = 0;
            int totalMinorRevisions = 0;
            int anonymousRevisions = 0;
            bool sawMinor = false;

            // dictionary from string of user name to user ID
            Dictionary<String, int> contributorMap = new Dictionary<String, int>();

            // dictionary from page names to Page objects
            // note that page objects internally contain a list of revisions
            Dictionary<String, Page> pageMap = new Dictionary<String, Page>();

            // dictionary from namespace ID to namespace string
            Dictionary<int, NamespaceInfo> namespaceMap = new Dictionary<int, NamespaceInfo>();

            while (reader.Read())
            {
                if (reader.IsStartElement())
                {
                    switch (reader.Name)
                    {
                        case "minor":
                            sawMinor = true;
                            break;

                        case "namespace":
                            int key = Int32.Parse(reader["key"]);
                            reader.Read();
                            String namespaceName = reader.Value;
                            // Console.WriteLine("{0}: {1}", key, namespaceName);
                            namespaceMap.Add( key, new NamespaceInfo( namespaceName, key ) );
                            break;

                        case "parentid":
                            reader.Read();
                            parentRevisionId = Int64.Parse(reader.Value);
                            break;

                        case "id":
                            reader.Read();
                            if (inContributor)
                                contributorId = Int64.Parse(reader.Value);
                            else if (inRevision)
                                revisionId = Int32.Parse(reader.Value);
                            else 
                                pageId = Int32.Parse(reader.Value);
                            break;

                        case "username":
                            reader.Read();
                            contributorUserName = reader.Value;
                            break;

                        case "ip":
                            reader.Read();
                            contributorIp = reader.Value;
                            anonymousRevisions += 1;
                            break;

                        case "timestamp":
                            reader.Read();
                            timestamp = DateTime.Parse(reader.Value);
                            break;

                        case "title":
                            reader.Read();
                            pageName = reader.Value;
                            break;

                        case "ns":
                            reader.Read();
                            namespaceId = Int32.Parse(reader.Value);
                            break;

                        case "revision":
                            inRevision = true;
                            break;

                        case "contributor":
                            // "contributor" may be an empty element;
                            // if so, we're not inside it (and won't have contributor name or ID)
                            if (!reader.IsEmptyElement)
                            {
                                inContributor = true;
                            }
                            else
                            {
                                contributorUserName = null;
                                System.Console.WriteLine("Empty element!");
                            }
                            break;

                        case "text":
                            reader.Read();
                            articleText = reader.Value;
                            break;

                        case "comment":
                            reader.Read();
                            comment = reader.Value;
                            break;
                    }
                }
                else if (reader.NodeType == XmlNodeType.EndElement)
                {
                    switch (reader.Name)
                    {
                        case "page":
                            System.Console.WriteLine("({0}) {1}: {2} revisions, {3} minor revisions", namespaceId, pageName, revisionCount, minorRevisionCount);
                            System.Console.WriteLine("{0} / {1}: {2:##0.000}", s.Position, s.Length, (s.Position * 100.0) / s.Length);
                            totalRevisions += revisionCount;
                            totalMinorRevisions += minorRevisionCount;
                            totalPages += 1;
                            revisionCount = 0;
                            minorRevisionCount = 0;

                            namespaceMap[namespaceId].IncrementCount();

                            pageMap[pageName].CloseRevisions();
                            pageMap.Remove(pageName);
                            System.Console.WriteLine("Removed {0}", pageName);
                            break;

                        case "revision":
                            // System.Console.WriteLine(" {0}: {1}, {2}, {3}", pageName, revisionId, timestamp, articleText.Length );
                            // System.Console.WriteLine(" {0}: ", contributorUserName, comment);
                            revisionCount += 1;

                            if (sawMinor)
                                minorRevisionCount += 1;

                            User contributor = null;
                            if (contributorId == 0)
                                contributor = new User(0, contributorIp);
                            else
                            {
                                if (contributorUserName != null)
                                    contributor = new User(contributorId, contributorUserName);
                            }
                            
                            PageRevision rev = new PageRevision(parentRevisionId, revisionId, timestamp, contributor, comment, articleText, sawMinor);
                            if (pageMap.ContainsKey(pageName))
                            {
                                pageMap[pageName].AddRevision(rev);
                            }
                            else
                            {
                                Page newPage = new Page(namespaceId, pageId, pageName);
                                newPage.AddRevision(rev);
                                pageMap.Add(pageName, newPage);
                            }

                            inRevision = false;
                            sawMinor = false;
                            contributorUserName = null;
                            contributorIp = null;
                            contributorId = 0;
                            break;

                        case "contributor":
                            // System.Console.WriteLine("inContributor == {0}", inContributor);
                            inContributor = false;
                            if (contributorIp == null)
                            {
                                //REVIEW: how to handle anonymous edits?
                                if (contributorMap.ContainsKey(contributorUserName))
                                {
                                    contributorMap[contributorUserName] += 1;
                                }
                                else
                                {
                                    contributorMap.Add(contributorUserName, 1);
                                }
                            }
                            break;

                        case "namespaces":
                            System.Console.WriteLine("Read {0} namespaces", namespaceMap.Count );
                            break;
                    }
                }
            }

            System.Console.WriteLine("{0} total pages", totalPages);
            System.Console.WriteLine("{0} total revisions", totalRevisions);
            System.Console.WriteLine("{0} total minor revisions", totalMinorRevisions);
            System.Console.WriteLine("{0} distinct contributors", contributorMap.Count);

            foreach (NamespaceInfo ns in namespaceMap.Values)
            {
                System.Console.WriteLine("{0},{1}", ns.PageCount, ns.Name);
            }
        }
    }
}


