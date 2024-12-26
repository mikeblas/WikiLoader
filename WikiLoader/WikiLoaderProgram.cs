// https://en.wikipedia.org/wiki/Wikipedia:Database_download#XML_schema

namespace WikiLoader
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    using WikiLoaderEngine;

    internal class WikiLoaderProgram : IXmlDumpParserProgress
    {
        private static bool SigintReceived = false;

        private readonly DatabasePump pump;

        private long previousPendingRevisionCount = -1;

        // position percentage last time we reported it
        private string? lastPositionPercent = null;

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

        public void FileProgress(long position, long length, bool skipping)
        {
            lock (this)
            {
                string positionPercent = $"{(position * 100.0) / length:##0.0000}";
                if (this.lastPositionPercent == null || !positionPercent.Equals(this.lastPositionPercent))
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

                    this.lastPositionPercent = positionPercent;
                }
            }
        }

        public void CompletedPage(string pageName, int usersAdded, int usersExist, int revisionsAdded, int revisionsExist, long timeMilliseconds)
        {
            lock (this)
            {
                if (revisionsAdded != 0 || usersAdded != 0)
                {
                    Console.WriteLine(
                        $"[[{pageName}]]\n" +
                        $"   {revisionsAdded} revisions added, {revisionsExist} revisions exist\n" +
                        $"   {usersAdded} users added, {usersExist} users exist\n" +
                        $"   {timeMilliseconds} milliseconds");
                }
            }
        }

        public void BackPressurePulse(long running, long queued, int pendingRevisions, IEnumerable<IWorkItemDescription> runningSet)
        {
            string main = $"Backpressure: {running} running, {queued} queued, {pendingRevisions} pending revisions";
            if (previousPendingRevisionCount != -1)
            {
                long delta = pendingRevisions - this.previousPendingRevisionCount;
                main = $"{main} ({delta:+#;-#;0})";
            }

            main += "\n";

            lock (runningSet)
            {
                foreach (var wii in runningSet)
                {
                    main += $"   {wii.ObjectName}, {wii.RemainingRevisionCount} / {wii.RevisionCount}\n";
                }
            }

            lock (this)
            {
                Console.Write(main);
            }

            previousPendingRevisionCount = pendingRevisions;
        }


        private static void Main(string[] args)
        {
            string fileName = @"v:\wiki\202411\history\uncompressed\enwiki-20241101-stub-meta-history24.xml";
            // string fileName = @"f:\wiki\20240701\history\unzipped\enwiki-20240701-stub-meta-history27.xml";
            // string fileName = @"f:\wiki\20240701\current\unzipped\enwiki-20240701-pages-meta-current2.xml-p41243p151573";
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
                    strResult = "Canceled";
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

            long skipPosition = pump.DetermineSkipPosition(fileName);

            XmlDumpParser xdp = new (s, this.pump, skipPosition, this);

            while (xdp.Read())
            {
                xdp.Work();
                if (SigintReceived)
                    xdp.Interrupt();
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
    }
}
