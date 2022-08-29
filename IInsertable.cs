using System.Data.SqlClient;
using System.Threading;

namespace WikiReader
{
    interface IInsertable
    {
        void Insert(IInsertable? previous, DatabasePump pump, SqlConnection conn, IInsertableProgress progress);

        string ObjectName
        {
            get;
        }

        int RevisionCount
        {
            get;
        }

        ManualResetEvent GetCompletedEvent();
    }
}
