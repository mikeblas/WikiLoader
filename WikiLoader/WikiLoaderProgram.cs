// https://en.wikipedia.org/wiki/Wikipedia:Database_download#XML_schema

namespace WikiLoader
{
    using System;
    using System.IO;
    using System.IO.Enumeration;
    using System.Reflection.PortableExecutable;
    using System.Xml;

    using WikiLoaderEngine;

    internal class WikiLoaderProgram : IXmlDumpParserProgress
    {
        internal static bool SigintReceived = false;

        private readonly DatabasePump pump;

        internal WikiLoaderProgram()
        {
            this.pump = new DatabasePump();
            DatabasePump.TestConnection();

            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                WikiLoaderProgram.SigintReceived = true;
                Console.WriteLine("CTRL+C received! Shutting down ...");
            };
        }

        private static void Main(string[] args)
        {
            // string fileName = @"C:\Junk\enwiki-latest-pages-meta-history1.xml-p000000010p000002933";
            // string fileName = @"f:\junk\enwiki-latest-pages-meta-history4.xml-p000066951p000074581";
            // string fileName = @"f:\junk\enwiki-latest-pages-meta-history10.xml-p000925001p000972034";
            // string fileName = @"f:\junk\enwiki-latest-pages-meta-history19.xml-p009225001p009575994";
            // string fileName = @"f:\junk\enwiki-latest-pages-meta-history3.xml-p000039229p000043715";
            string fileName = @"f:\wiki\20220820\unzipped\enwiki-20220820-stub-meta-history5.xml";
            if (args.Length >= 1)
                fileName = args[0];

            WikiLoaderProgram p = new ();
            p.Run(fileName);
        }

        private void Run(string fileName)
        {
            string strResult = "Unknown exception!";
            try
            {
                this.pump.StartRun(fileName);
                this.Parse(fileName);
                if (SigintReceived)
                    strResult = "Cancelled";
                else
                    strResult = "Success";
            }
            catch (Exception x)
            {
                strResult = x.Message;
                throw;
            }
            finally
            {
                this.pump.CompleteRun(strResult);
            }
        }

        private void Parse(string fileName)
        {
            FileStream s = File.OpenRead(fileName);
            using XmlReader reader = XmlReader.Create(s, null);

            long skipPosition = pump.DetermineSkipPosition(fileName);

            XmlDumpParser xdp = new (s, reader, this.pump, skipPosition, this);

            while (xdp.Read())
            {
                xdp.Work();
            }

            // wait for the pump to complete before spewing stats
            this.pump.WaitForComplete();

            // done, spew stats!
            Console.WriteLine($"{xdp.TotalPages} total pages read, {this.pump.InsertedPages()} inserted");
            Console.WriteLine($"{xdp.TotalRevisions} total revisions");
            Console.WriteLine($"{xdp.TotalMinorRevisions} total minor revisions");
            Console.WriteLine($"{xdp.ContributorCount} distinct contributors");

            Console.WriteLine($"Longest comment is {xdp.LargestCommentLength}: {xdp.LargestComment}");
            Console.WriteLine($"Longest text is {xdp.LargestArticleTextLength}");

            foreach (NamespaceInfo ns in xdp.NamespaceMap.Values)
                Console.WriteLine($"{ns.PageCount},{ns.Name}");
        }

        public void FileProgress(long position, long length, bool skipping)
        {
            if (skipping)
                Console.WriteLine($"Skipped: {position} / {length}: {(position * 100.0) / length:##0.0000}");
            else
            {
                Console.BackgroundColor = ConsoleColor.DarkBlue;
                Console.Write($"{position} / {length}: {(position * 100.0) / length:##0.0000}");
                Console.ResetColor();
                Console.WriteLine();
            }

        }

        public void CompletedPage(string pageName, int usersAdded, int usersExist, int revisionsAdded, int revisionsExist)
        {
            Console.WriteLine(
                $"[[{pageName}]]\n" +
                $"   {revisionsAdded} revisions added, {revisionsExist} revisions exist\n" +
                $"   {usersAdded} users added, {usersExist} users exist");
        }

        public void BackPressurePulse(int running, int queued, int pendingRevisions)
        {
            // $"   Queued [[{pageName}]]: {revisionCount} revisions, {minorRevisionCount} minor revisions");
            // $"   {running} running, {queued} queued, {pendingRevisions} pending revisions");
            throw new NotImplementedException();
        }
    }
}
