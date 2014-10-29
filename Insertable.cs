using System;
using System.Data.Sql;
using System.Data.SqlClient;

namespace WikiReader
{
    interface Insertable
    {
        void Insert(SqlConnection conn);

        String ObjectName
        {
            get;
        }

        int RevisionCount
        {
            get;
        }
    }
}
