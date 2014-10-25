﻿using System;
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
        String _current;
        String _largest;
        String _name;

        public LargestString(String name)
        {
            _name = name;
        }

        public void test(String str)
        {
        }

        public void Reset()
        {
            _current = null;
            _largest = null;
        }

        public String Name
        {
            get { return _name; }
        }

        public String Current
        {
            get { return _current; }
            set
            {
                if (_largest == null || value.Length > _largest.Length)
                {
                    _largest = value;
                }
                _current = value;
            }
        }

        public String Largest
        {
            get { return _largest; }
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



    class Program
    {
        String _fileName;
        DatabasePump _pump;

        Program(String fileName)
        {
            _fileName = fileName;
            _pump = new DatabasePump();
            _pump.TestConnection();
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
                            articleText.Current = reader.Value;
                            break;

                        case "comment":
                            reader.Read();
                            comment.Current = reader.Value;
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

                            // grab a copy of the page
                            // clean it up and push it to the pump
                            Page page = pageMap[pageName];
                            page.CloseRevisions();
                            pageMap.Remove(pageName);
                            // _pump.Enqueue(page);
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
                                contributor = new User(0, contributorIp.Current);
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
                            contributorIp.Reset();;
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
                            _pump.Enqueue(namespaceMap);
                            break;
                    }
                }
            }

            System.Console.WriteLine("{0} total pages", totalPages);
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

