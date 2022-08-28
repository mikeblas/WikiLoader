using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.IO;
using System.Diagnostics;

// https://en.wikipedia.org/wiki/Wikipedia:Database_download#XML_schema

namespace WikiReader
{
    class Program
    {
        readonly DatabasePump _pump;

        Program()
        {
            _pump = new DatabasePump();
            DatabasePump.TestConnection();
        }

        static void Main(string[] args)
        {
            // string fileName = @"C:\Junk\enwiki-latest-pages-meta-history1.xml-p000000010p000002933";
            // string fileName = @"f:\junk\enwiki-latest-pages-meta-history4.xml-p000066951p000074581";
            // string fileName = @"f:\junk\enwiki-latest-pages-meta-history10.xml-p000925001p000972034";
            // string fileName = @"f:\junk\enwiki-latest-pages-meta-history19.xml-p009225001p009575994";
            // string fileName = @"f:\junk\enwiki-latest-pages-meta-history3.xml-p000039229p000043715";
            string fileName = @"f:\wiki\20220820\unzipped\enwiki-20220820-stub-meta-history3.xml";
            if (args.Length >= 1)
                fileName = args[0];

            Program p = new();
            string strResult = "Unknown exception!";
            try
            {
                p._pump.StartRun(fileName);
                p.Parse(fileName);
                strResult = "Success.";
            }
            catch (Exception x)
            {
                strResult = x.Message;
                throw;
            }
            finally
            {
                p._pump.CompleteRun( strResult );
            }
        }


        void Parse(string fileName)
        {
            FileStream s = File.OpenRead(fileName);
            XmlReader reader = XmlReader.Create(s, null);

            string? pageName = null;
            string? redirectTitle = null;
            Int64 revisionId = 0;
            Int64 contributorId = 0;
            Int64 parentRevisionId = 0;
            LargestString contributorIp = new("contributorIp");
            int namespaceId = 0;
            int pageId = 0;
            Boolean inRevision = false;
            Boolean inContributor = false;
            DateTime timestamp = DateTime.MinValue;
            LargestString comment = new("Comment");
            LargestString articleText = new("ArticleText");
            string? contributorUserName = null;
            int revisionCount = 0;
            int minorRevisionCount = 0;
            int totalPages = 0;
            int totalRevisions = 0;
            int totalMinorRevisions = 0;
            int anonymousRevisions = 0;
            bool sawMinor = false;

            // dictionary from string of user name to user ID
            Dictionary<string, int> contributorMap = new();

            // dictionary from page names to Page objects
            // note that page objects internally contain a list of revisions
            Dictionary<string, Page> pageMap = new();

            // dictionary from namespace ID to namespace string
            NamespaceInfos namespaceMap = new();

            long currentActivity = -1;
            Page? previousPage = null;

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
                            string? keystring = reader["key"];
                            Debug.Assert(keystring != null, "Expected key in namespace tag");
                            int key = Int32.Parse(keystring);
                            reader.Read();
                            string namespaceName = reader.Value;
                            // Console.WriteLine($"{key}: {namespaceName}");
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

                        case "redirect":
                            redirectTitle = reader.GetAttribute("title");
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
                                currentActivity = _pump.StartActivity("Read Page", namespaceId, pageId, null);
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
                                // String str = reader.GetAttribute("deleted");
                                // System.Console.WriteLine($"Empty element! RevisionID = {revisionId}, Attribute = {str}");
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
                                        System.Console.WriteLine($"articleText == {articleText.Current.Length}");
                                    else
                                        System.Console.WriteLine("articleText == is null");
                                    System.Console.WriteLine($"revisionID == {revisionId}");
                                    System.Console.WriteLine($"reader == {reader.Value.Length}");
                                    System.Console.WriteLine($"timestamp == {timestamp}");
                                    throw oom;
                                }
                            }
                            else
                            {
                                articleText.Current = null;
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
                                // System.Console.WriteLine($"Deleted comment! RevisionID = {revisionId}");
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
                            Debug.Assert(pageName != null, "Must have valuid page name by now");

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
                                $"{s.Position} / {s.Length}: {(s.Position * 100.0) / s.Length:##0.0000}\n" +
                                $"   Queued {pageName}: {revisionCount} revisions, {minorRevisionCount} minor revisions\n" +
                                $"   {running} running, {queued} queued, {pendingRevisions} pending revisions");

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
                            redirectTitle = null;
                            break;

                        case "revision":

                            if (pageName == null)
                                throw new InvalidOperationException("Expected valid page name when processing revision");

                            // System.Console.WriteLine(" {0}: {1}, {2}, {3}", pageName, revisionId, timestamp, articleText.Length );
                            // System.Console.WriteLine(" {0}: ", contributorUserName, comment);
                            revisionCount += 1;

                            if (revisionCount % 1000 == 0)
                                System.Console.WriteLine($" {pageName}: read {revisionCount} revisions");

                            if (sawMinor)
                                minorRevisionCount += 1;

                            User? contributor = null;
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
                            
                            var rev = new PageRevision(parentRevisionId, revisionId, timestamp, contributor, comment.Current, articleText.Current, sawMinor);
                            if (pageMap.ContainsKey(pageName))
                            {
                                pageMap[pageName].AddRevision(rev);
                            }
                            else
                            {
                                Page newPage = new(namespaceId, pageId, pageName, redirectTitle);
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
                            redirectTitle = null;
                            break;

                        case "contributor":
                            // System.Console.WriteLine($"inContributor == {inContributor}");
                            inContributor = false;
                            if (contributorIp.Current == null)
                            {
                                if (contributorUserName == null)
                                    throw new InvalidOperationException("Can't have null contributor IP and null contributor User Name");
                                //REVIEW: how to handle anonymous edits?
                                if (contributorMap.ContainsKey(contributorUserName))
                                    contributorMap[contributorUserName] += 1;
                                else
                                    contributorMap.Add(contributorUserName, 1);
                            }
                            break;

                        case "namespaces":
                            System.Console.WriteLine($"Read {namespaceMap.Count} namespaces");
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
            System.Console.WriteLine($"{totalPages} total pages read, {_pump.getInsertedPages()} inserted");
            System.Console.WriteLine($"{totalRevisions} total revisions");
            System.Console.WriteLine($"{totalMinorRevisions} total minor revisions");
            System.Console.WriteLine($"{contributorMap.Count} distinct contributors");

            System.Console.WriteLine($"Longest comment is {comment.LargestLength}: {comment.Largest}");
            System.Console.WriteLine($"Longest text is {articleText.LargestLength}");

            foreach (NamespaceInfo ns in namespaceMap.Values)
                System.Console.WriteLine($"{ns.PageCount},{ns.Name}");
        }
    }
}
