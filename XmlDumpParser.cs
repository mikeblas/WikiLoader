// https://en.wikipedia.org/wiki/Wikipedia:Database_download#XML_schema

namespace WikiReader
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Xml;

    internal class XmlDumpParser
    {
        private string? pageName = null;
        private string? redirectTitle = null;
        private long revisionId = 0;
        private long contributorId = 0;
        private long parentRevisionId = 0;
        private LargestString contributorIp = new("contributorIp");
        private int namespaceId = 0;
        private int pageId = 0;
        private bool inRevision = false;
        private bool inContributor = false;
        private DateTime timestamp = DateTime.MinValue;
        private LargestString comment = new("Comment");
        private LargestString articleText = new("ArticleText");
        private string? contributorUserName = null;
        private int revisionCount = 0;
        private int minorRevisionCount = 0;
        private int totalPages = 0;
        private int totalRevisions = 0;
        private int totalMinorRevisions = 0;
        private int anonymousRevisions = 0;
        private bool sawMinor = false;
        private bool sawPageBegin = true;

        // dictionary from string of user name to user ID
        private Dictionary<string, int> contributorMap = new();

        // dictionary from page names to Page objects
        // note that page objects internally contain a list of revisions
        private Dictionary<string, Page> pageMap = new();

        // dictionary from namespace ID to namespace string
        internal NamespaceInfos namespaceMap = new();

        private long currentActivity = -1;
        private Page? previousPage = null;

        private bool quitNow = false;

        private long skipUntilPosition = 341958656;

        private FileStream s;
        private XmlReader reader;
        private DatabasePump pump;

        internal XmlDumpParser(FileStream s, XmlReader reader, DatabasePump pump)
        {
            this.s = s;
            this.reader = reader;
            this.pump = pump;
        }

        internal int TotalMinorRevisions
        {
            get { return this.totalMinorRevisions;  }
        }

        internal LargestString Comment
        {
            get { return this.comment; }
        }


        internal int ContributorCount
        {
            get { return this.contributorMap.Count; }
        }

        internal int TotalPages
        {
            get { return this.totalPages; }
        }

        internal int TotalRevisions
        {
            get { return this.totalRevisions; }
        }

        internal LargestString ArticleText
        {
            get { return this.articleText;  }
        }

        internal void HandleStartElement()
        {
            // always look for a title
            switch (this.reader.Name)
            {
                case "title":
                    reader.Read();
                    pageName = reader.Value;
                    break;

                case "namespace":
                    string? keystring = this.reader["key"];
                    Debug.Assert(keystring != null, "Expected key in namespace tag");
                    int key = int.Parse(keystring);
                    this.reader.Read();
                    string namespaceName = this.reader.Value;
                    this.namespaceMap.Add(new NamespaceInfo(namespaceName, key));
                    break;
            }

            // only do this work if we're not skipping
            if (s.Position >= skipUntilPosition)
                this.HandleSkippedStartElement();
        }

        private void HandleSkippedStartElement()
        {
            switch (this.reader.Name)
            {
                case "page":
                    this.sawPageBegin = true;
                    break;

                case "minor":
                    this.sawMinor = true;
                    break;

                case "parentid":
                    this.reader.Read();
                    this.parentRevisionId = long.Parse(this.reader.Value);
                    break;

                case "id":
                    this.reader.Read();
                    if (this.inContributor)
                    {
                        this.contributorId = long.Parse(this.reader.Value);
                    }
                    else if (this.inRevision)
                        this.revisionId = int.Parse(this.reader.Value);
                    else
                    {
                        this.pageId = int.Parse(this.reader.Value);
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

                case "redirect":
                    redirectTitle = reader.GetAttribute("title");
                    break;

                case "ns":
                    reader.Read();
                    namespaceId = int.Parse(reader.Value);
                    break;

                case "revision":
                    inRevision = true;

                    // by this point, everything we need to know about a page should be set
                    // start an action for this page, then, if we don't have one already flying
                    if (currentActivity == -1)
                        currentActivity = pump.StartActivity("Read Page", namespaceId, pageId, null);
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
                        // Console.WriteLine($"Empty element! RevisionID = {revisionId}, Attribute = {str}");
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
                            if (articleText.Current != null)
                                Console.WriteLine($"articleText == {articleText.Current.Length}");
                            else
                                Console.WriteLine("articleText == is null");
                            Console.WriteLine($"revisionID == {revisionId}");
                            Console.WriteLine($"reader == {reader.Value.Length}");
                            Console.WriteLine($"timestamp == {timestamp}");
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
                        // Console.WriteLine($"Deleted comment! RevisionID = {revisionId}");
                        comment.Current = null;
                    }

                    break;
            }
        }

        internal void HandleEndElement()
        {
            switch (this.reader.Name)
            {
                case "page":
                    Debug.Assert(pageName != null, "Must have valid page name by now");

                    if (s.Position < skipUntilPosition)
                    {
                        Console.WriteLine(
                            $"{s.Position} / {s.Length}: {(s.Position * 100.0) / s.Length:##0.0000}\n" +
                            "   Skipped");
                        this.sawPageBegin = false;
                    }
                    else if (!sawPageBegin)
                    {
                        Console.WriteLine(
                            $"{s.Position} / {s.Length}: {(s.Position * 100.0) / s.Length:##0.0000}\n" +
                            "   Incompletely read");
                        this.sawPageBegin = false;
                    }
                    else
                    {
                        // grab a copy of the page
                        // clean it up and push it to the pump
                        Page page = pageMap[pageName];
                        pageMap.Remove(pageName);

                        (long running, long queued, long pendingRevisions) = this.pump.Enqueue(page, previousPage);

                        previousPage = page;

                        // write some stats
                        Console.WriteLine(
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
                            pump.CompleteActivity(currentActivity, revisionCount, null);
                            currentActivity = -1;
                        }
                    }

                    // reset revision counts in reader
                    revisionCount = 0;
                    minorRevisionCount = 0;
                    pageName = null;
                    redirectTitle = null;

                    if (WikiLoaderProgram.sigintReceived)
                        quitNow = true;
                    break;
            }

            if (s.Position >= skipUntilPosition)
                HandleSkippedEndElement();
        }

        internal void HandleSkippedEndElement()
        {
            switch (this.reader.Name)
            {
                case "revision":
                    if (pageName == null)
                        throw new InvalidOperationException("Expected valid page name when processing revision");

                    // Console.WriteLine(" {0}: {1}, {2}, {3}", pageName, revisionId, timestamp, articleText.Length );
                    // Console.WriteLine(" {0}: ", contributorUserName, comment);
                    revisionCount += 1;

                    if (revisionCount % 1000 == 0)
                        Console.WriteLine($" {pageName}: read {revisionCount} revisions");

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
                        Page newPage = new(namespaceId, pageId, pageName, redirectTitle, this.pump.RunID, s.Position);
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
                    // Console.WriteLine($"inContributor == {inContributor}");
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
            }
        }

        internal bool Read()
        {
            if (quitNow)
                return false;

            return reader.Read();
        }

        internal void Work()
        {
            if (this.reader.IsStartElement())
            {
                this.HandleStartElement();
            }
            else if (this.reader.NodeType == XmlNodeType.EndElement)
            {
                this.HandleEndElement();
            }
        }
    }
}
