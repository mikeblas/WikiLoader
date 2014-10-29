using System;
using System.Collections.Generic;
using System.Data.Sql;
using System.Data.SqlClient;

namespace WikiReader
{
    /// <summary>
    /// NamespaceInfos contains a collection of Namespace objects,
    /// and implements the Insertable interface so the listcan be pushed
    /// to the database.
    /// 
    /// Note that the class has a few members (counts, particularly) that
    /// aren't persisted; these are used to show statistics for a run of
    /// the loader.
    /// </summary>
    class NamespaceInfos : Insertable
    {
        /// <summary>
        /// map of namespace IDs (as integers) to NamespaceInfo objects
        /// </summary>
        private Dictionary<Int64, NamespaceInfo> _namespaceMap;

        /// <summary>
        /// Initializes a new NamespaceInfos collection
        /// </summary>
        public NamespaceInfos()
        {
            _namespaceMap = new Dictionary<Int64, NamespaceInfo>();
        }

        /// <summary>
        /// Add a new NamespaceInfo
        /// </summary>
        /// <param name="nsi"></param>
        public void Add( NamespaceInfo nsi )
        {
            _namespaceMap.Add(nsi.ID, nsi);
        }

        /// <summary>
        /// number of items in this collection
        /// </summary>
        public int Count
        {
            get { return _namespaceMap.Count; }
        }

        /// <summary>
        /// Find a NamespaceInfo object given the ID key.
        /// Returns null if we don't have it.
        /// </summary>
        /// <param name="key">NamespaceID to find</param>
        /// <returns>NamespaceID, null if not found</returns>
        public NamespaceInfo this[Int64 key]
        {
            get { return _namespaceMap[key]; }
        }

        /// <summary>
        /// Get a collection containing our values (only)
        /// </summary>
        public Dictionary<Int64, NamespaceInfo>.ValueCollection Values
        {
            get { return _namespaceMap.Values; }
        }

        /// <summary>
        /// Insertable implementation: Insert method.
        /// Takes a SqlConnection and inserts the collection into it.
        /// </summary>
        /// <param name="conn">SqlConnection to write into</param>
        void Insertable.Insert(SqlConnection conn)
        {
            // count of rows actually inserted
            int inserts = 0;
            // count of rows we didn't insert becuase they were dupes
            int already = 0;

            // command to push a new row into the Namespace table
            SqlCommand cmd = new SqlCommand("INSERT INTO [Namespace] (NamespaceID, NamespaceName) VALUES ( @ID, @Name );", conn);

            // go through the whole collection
            foreach (KeyValuePair<Int64, NamespaceInfo> kvp in _namespaceMap)
            {
                // reset the parameters collection; bind the current item from the collection
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@ID", kvp.Key);
                cmd.Parameters.AddWithValue("@Name", kvp.Value.Name);

                // try to do the insert; tally if we're successful
                try
                {
                    cmd.ExecuteNonQuery();
                    inserts += 1;
                }
                catch (SqlException sex)
                {
                    // if the exception is a duplicate key, just tally that
                    if (sex.Number == 2601)
                    {
                        // already know that namespace ID
                        already += 1;
                    }
                    else
                    {
                        // otherwise, something else went wrong
                        throw sex;
                    }
                }
            }

            // show our results
            System.Console.WriteLine("Inserted {0} new namespaces; {1} already known", inserts, already);
        }

        String Insertable.ObjectName
        {
            get { return "Namespaces Inserter"; }
        }

        int Insertable.RevisionCount
        {
            get { return 0; }
        }
    }
}
