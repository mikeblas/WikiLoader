using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.IO;

// https://en.wikipedia.org/wiki/Wikipedia:Database_download#XML_schema

namespace WikiReader
{
    class Program
    {
        DatabasePump _pump;

        Program()
        {
            _pump = new DatabasePump();
            _pump.TestConnection();
        }

        static void Main(string[] args)
        {
            // String fileName = @"C:\Junk\enwiki-latest-pages-meta-history1.xml-p000000010p000002933";
            // String fileName = @"f:\junk\enwiki-latest-pages-meta-history4.xml-p000066951p000074581";
            // String fileName = @"f:\junk\enwiki-latest-pages-meta-history10.xml-p000925001p000972034";
            // String fileName = @"f:\junk\enwiki-latest-pages-meta-history19.xml-p009225001p009575994";
            String fileName = @"f:\junk\enwiki-latest-pages-meta-history10.xml-p000925001p000972034";
            if (args.Length >= 1)
                fileName = args[0];

            Program p = new Program();
            String strResult = null;
            try
            {
                p._pump.StartRun(fileName);
                p.Parse(fileName);
                strResult = "Success.";
            }
            catch (Exception x)
            {
                strResult = x.Message;
                throw x;
            }
            finally
            {
                p._pump.CompleteRun( strResult );
            }
        }


        void Parse(String fileName)
        {
            FileStream s = File.OpenRead(fileName);
            XmlReader reader = XmlReader.Create(s, null);

            String pageName = null;
            Int64 revisionId = 0;
            Int64 contributorId = 0;
            Int64 parentRevisionId = 0;
            LargestString contributorIp = new LargestString("contributorIp");
            int namespaceId = 0;
            int pageId = 0;
            Boolean inRevision = false;
            Boolean inContributor = false;
            DateTime timestamp = DateTime.MinValue;
            LargestString comment = new LargestString("Comment");
            LargestString articleText = new LargestString("ArticleText");
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
            NamespaceInfos namespaceMap = new NamespaceInfos();

            long currentActivity = -1;
            Page previousPage = null;

            while (reader.Read())
            {
                if (reader.IsStartElement())
                {
                    // Console.Write('S');
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
                            namespaceMap.Add( new NamespaceInfo( namespaceName, key ) );
                            break;

                        case "parentid":
                            reader.Read();
                            parentRevisionId = Int64.Parse(reader.Value);
                            break;

                        case "id":
                            reader.Read();
                            if (inContributor)
                            {
                                contributorId = Int64.Parse(reader.Value);
                            }
                            else if (inRevision)
                                revisionId = Int32.Parse(reader.Value);
                            else
                            {
                                pageId = Int32.Parse(reader.Value);
                            }
                            break;

                        case "username":
                            reader.Read();
                            contributorUserName = reader.Value;
                            break;

                        case "ip":
                            reader.Read();
                            contributorIp.Current = reader.Value;
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

                            // by this point, everything we need to know about a page should be set
                            // start an action for this page, then, if we don't have one already flying
                            if (currentActivity == -1)
                            {
                                currentActivity = _pump.StartActivity("Read Page", namespaceId, pageId, null);
                            }
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
                                String str = reader.GetAttribute("deleted");
                                contributorUserName = null;
                                // System.Console.WriteLine("Empty element! RevisionID = {1}, Attribute = {0}", str, revisionId);
                            }
                            break;

                        case "text":
                            // "text" may be an empty element;
                            // if so, we're not inside it (and won't have contributor name or ID)
                            if (!reader.IsEmptyElement)
                            {
                                reader.Read();
                                try
                                {
                                    articleText.Current = reader.Value;
                                }
                                catch (OutOfMemoryException oom)
                                {
                                    if ( articleText.Current != null )
                                        System.Console.WriteLine("articleText == {0}", articleText.Current.Length);
                                    else
                                        System.Console.WriteLine("articleText == is null");
                                    System.Console.WriteLine("revisionID == {0}", revisionId);
                                    System.Console.WriteLine("reader == {0}", reader.Value.Length);
                                    System.Console.WriteLine("timestamp == {0}", timestamp);
                                    throw oom;
                                }
                            }
                            else
                            {
                                if (null != reader.GetAttribute("deleted"))
                                {
                                    // System.Console.WriteLine("Deleted text! RevisionID = {0}", revisionId);
                                    articleText.Current = null;
                                }
                                else
                                {
                                    articleText.Current = "";
                                }
                            }
                            break;

                        case "comment":
                            // "comment" may be an empty element;
                            // if so, we're not inside it (and won't have contributor name or ID)
                            if (!reader.IsEmptyElement)
                            {
                                reader.Read();
                                comment.Current = reader.Value;
                            }
                            else
                            {
                                // System.Console.WriteLine("Deleted comment! RevisionID = {0}",  revisionId);
                                comment.Current = null;
                            }
                            break;
                    }
                }
                else if (reader.NodeType == XmlNodeType.EndElement)
                {
                    // Console.Write('E');
                    switch (reader.Name)
                    {
                        case "page":
                            // grab a copy of the page
                            // clean it up and push it to the pump
                            Page page = pageMap[pageName];
                            pageMap.Remove(pageName);
                            long running = 0;
                            long queued = 0;
                            long pendingRevisions = 0;
                            _pump.Enqueue(page, previousPage, ref running, ref queued, ref pendingRevisions);
                            previousPage = page;

                            // write some stats
                            System.Console.WriteLine(
                                "{0} / {1}: {2:##0.000}\n" +
                                "   Queued {3}: {4} revisions, {5} minor revisions\n" +
                                "   {6} running, {7} queued, {8} pending revisions",
                                s.Position, s.Length, (s.Position * 100.0) / s.Length,
                                pageName, revisionCount, minorRevisionCount,
                                running, queued, pendingRevisions);

                            // tally our stats
                            totalRevisions += revisionCount;
                            totalMinorRevisions += minorRevisionCount;
                            totalPages += 1;

                            // per namespace inserts
                            namespaceMap[namespaceId].IncrementCount();

                            // push the activity in
                            if (currentActivity != -1)
                            {
                                _pump.CompleteActivity(currentActivity, revisionCount, null);
                                currentActivity = -1;
                            }

                            // reset revision counts in reader
                            revisionCount = 0;
                            minorRevisionCount = 0;
                            pageName = null;
                            break;

                        case "revision":
                            // System.Console.WriteLine(" {0}: {1}, {2}, {3}", pageName, revisionId, timestamp, articleText.Length );
                            // System.Console.WriteLine(" {0}: ", contributorUserName, comment);
                            revisionCount += 1;

                            if (revisionCount % 1000 == 0)
                                System.Console.WriteLine(" {0}: read {1} revisions", pageName, revisionCount);

                            if (sawMinor)
                                minorRevisionCount += 1;

                            User contributor = null;
                            if (contributorId == 0 && contributorUserName == null)
                            {
                                if (contributorIp.Current == null)
                                {
                                    // deletd contribution; contributor remains null
                                    // for the PageRevision constructor
                                }
                                else
                                {
                                    // anonymous edit
                                    contributor = new User(contributorIp.Current);
                                }
                            }
                            else
                            {
                                if (contributorUserName != null)
                                    contributor = new User(contributorId, contributorUserName);
                            }
                            
                            PageRevision rev = new PageRevision(parentRevisionId, revisionId, timestamp, contributor, comment.Current, articleText.Current, sawMinor);
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
                            contributorIp.Reset();
                            contributorId = 0;
                            comment.Current = null;
                            articleText.Current = null;
                            break;

                        case "contributor":
                            // System.Console.WriteLine("inContributor == {0}", inContributor);
                            inContributor = false;
                            if (contributorIp.Current == null)
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
                            running = 0;
                            queued = 0;
                            pendingRevisions = 0;
                            _pump.Enqueue(namespaceMap, null, ref running, ref queued, ref pendingRevisions);
                            break;
                    }
                }
            }
            reader.Close();

            // wait for the pump to complete before spewing stats
            _pump.WaitForComplete();

            // done, spew stats!
            System.Console.WriteLine("{0} total pages read, {1} inserted", totalPages, _pump.getInsertedPages());
            System.Console.WriteLine("{0} total revisions", totalRevisions);
            System.Console.WriteLine("{0} total minor revisions", totalMinorRevisions);
            System.Console.WriteLine("{0} distinct contributors", contributorMap.Count);

            System.Console.WriteLine("Longest comment is {0}: {1}", comment.Largest.Length, comment.Largest);
            System.Console.WriteLine("Longest text is {0}", articleText.Largest.Length );

            foreach (NamespaceInfo ns in namespaceMap.Values)
            {
                System.Console.WriteLine("{0},{1}", ns.PageCount, ns.Name);
            }

        }
    }
}


