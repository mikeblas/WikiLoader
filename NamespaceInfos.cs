using System;
using System.Collections.Generic;
using System.Data.Sql;
using System.Data.SqlClient;

namespace WikiReader
{
    class NamespaceInfos : Insertable
    {
        Dictionary<int, NamespaceInfo> _namespaceMap;
        public NamespaceInfos()
        {
            _namespaceMap = new Dictionary<int, NamespaceInfo>();
        }

        public void Add( int key, NamespaceInfo nsi )
        {
            _namespaceMap.Add(key, nsi);
        }

        public int Count
        {
            get { return _namespaceMap.Count; }
        }

        public NamespaceInfo this[int key]
        {
            get { return _namespaceMap[key]; }
        }

        public Dictionary<int, NamespaceInfo>.ValueCollection Values
        {
            get { return _namespaceMap.Values; }
        }

        void Insertable.Insert(SqlConnection conn)
        {
            int inserts = 0;
            int already = 0;
            SqlCommand cmd = new SqlCommand("INSERT INTO Namespace (NamespaceID, NamespaceName) VALUES ( @ID, @Name );", conn);
            foreach (KeyValuePair<int, NamespaceInfo> kvp in _namespaceMap)
            {
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@ID", kvp.Key);
                cmd.Parameters.AddWithValue("@Name", kvp.Value.Name);

                try
                {
                    cmd.ExecuteNonQuery();
                    inserts += 1;
                }
                catch (SqlException sex)
                {
                    if (sex.Number == 2601)
                    {
                        // already know that namespace ID
                        already += 1;
                    }
                    else
                    {
                        // something else went wrong
                        throw sex;
                    }
                }
            }
            System.Console.WriteLine("Inserted {0} new namespaces; {1} already known", inserts, already);
        }
    }
}
