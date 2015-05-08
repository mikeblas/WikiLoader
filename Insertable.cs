using System;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Threading;

namespace WikiReader
{
    interface Insertable
    {
        void Insert(Insertable previous, DatabasePump pump, SqlConnection conn, InsertableProgress progress);

        String ObjectName
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
