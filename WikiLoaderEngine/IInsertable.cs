namespace WikiLoaderEngine
{
    using System.Data.SqlClient;

    public interface IInsertable
    {
        string ObjectName
        {
            get;
        }

        string ObjectState
        {
            get;
        }

        string ObjectTarget
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

        void Insert(DatabasePump pump, SqlConnection conn, IInsertableProgress progress, IXmlDumpParserProgress parserProgress);
    }
}
