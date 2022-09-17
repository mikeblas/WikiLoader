namespace WikiLoaderEngine
{
    using System.Data.SqlClient;
    using System.Threading;

    public interface IInsertable
    {
        string ObjectName
        {
            get;
        }

        int RevisionCount
        {
            get;
        }

        int RemainingRevisionCount
        {
            get;
        }

        ManualResetEvent GetCompletedEvent();

        void Insert(IInsertable? previous, DatabasePump pump, SqlConnection conn, IInsertableProgress progress, IXmlDumpParserProgress parserProgress);
    }
}
