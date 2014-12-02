﻿using System;
using System.Data.Sql;
using System.Data.SqlClient;

namespace WikiReader
{
    interface Insertable
    {
        void Insert(SqlConnection conn, InsertableProgress progress);

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
