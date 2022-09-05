// https://en.wikipedia.org/wiki/Wikipedia:Database_download#XML_schema

namespace WikiReader
{
    using System;
    using System.IO;
    using System.Reflection.PortableExecutable;
    using System.Xml;

    class WikiLoaderProgram
    {
        internal static bool sigintReceived = false;

        private readonly DatabasePump pump;

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

        private static void Main(string[] args)
        {
            // string fileName = @"C:\Junk\enwiki-latest-pages-meta-history1.xml-p000000010p000002933";
            // string fileName = @"f:\junk\enwiki-latest-pages-meta-history4.xml-p000066951p000074581";
            // string fileName = @"f:\junk\enwiki-latest-pages-meta-history10.xml-p000925001p000972034";
            // string fileName = @"f:\junk\enwiki-latest-pages-meta-history19.xml-p009225001p009575994";
            // string fileName = @"f:\junk\enwiki-latest-pages-meta-history3.xml-p000039229p000043715";
            string fileName = @"f:\wiki\20220820\unzipped\enwiki-20220820-stub-meta-history3.xml";
            if (args.Length >= 1)
                fileName = args[0];

            WikiLoaderProgram p = new();
            p.Run(fileName);
        }

        private void Run(string fileName)
        {
            string strResult = "Unknown exception!";
            try
            {
                this.pump.StartRun(fileName);
                this.Parse(fileName);
                strResult = "Success.";
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

            XmlDumpParser xdp = new(s, reader, this.pump);

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

            Console.WriteLine($"Longest comment is {xdp.Comment.LargestLength}: {xdp.Comment.Largest}");
            Console.WriteLine($"Longest text is {xdp.ArticleText.LargestLength}");

            foreach (NamespaceInfo ns in xdp.namespaceMap.Values)
                Console.WriteLine($"{ns.PageCount},{ns.Name}");
        }
    }
}
