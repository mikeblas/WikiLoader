// https://en.wikipedia.org/wiki/Wikipedia:Database_download#XML_schema

namespace WikiLoader
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using WikiLoaderEngine;

    internal class WikiLoaderProgram : IXmlDumpParserProgress
    {
        private static bool sigintReceived = false;

        private readonly DatabasePump pump;

        private long previousPendingRevisionCount = -1;

        private long noUpdatePagesCount = 0;

        // position percentage last time we reported it
        private string? lastPositionPercent = null;

        internal WikiLoaderProgram()
        {
            this.pump = new DatabasePump();
            DatabasePump.TestConnection();

            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                WikiLoaderProgram.sigintReceived = true;
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
                        $"   {timeMilliseconds} milliseconds\n" +
                        $"   {this.noUpdatePagesCount} pages with no updates");
                    this.noUpdatePagesCount = 0;
                }
                else
                {
                    this.noUpdatePagesCount += 1;
                }
            }
        }

        public void BackPressurePulse(long running, long queued, int pendingRevisions, IDictionary<IWorkItemDescription, bool> runningSet)
        {
            StringBuilder builder = new ($"Back pressure: {running} running, {queued} queued, {pendingRevisions} pending revisions");
            if (previousPendingRevisionCount != -1)
            {
                long delta = pendingRevisions - this.previousPendingRevisionCount;
                builder.Append($" ({delta:+#;-#;0})");
            }

            builder.Append(Environment.NewLine);

            lock (runningSet)
            {
                foreach (var wii in runningSet.Keys)
                {
                    builder.Append($"   {wii.ObjectName}, {wii.RemainingRevisionCount} revisions remaining of {wii.RevisionCount}\n");
                }
            }

            lock (this)
            {
                // Console.ForegroundColor = ConsoleColor.Yellow;
                Console.Write(builder.ToString());
            }

            previousPendingRevisionCount = pendingRevisions;
        }


        private static void Main(string[] args)
        {
            string fileName = @"v:\wiki\202411\history\uncompressed\enwiki-20241101-stub-meta-history25.xml";
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
                if (sigintReceived)
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
                if (sigintReceived)
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
