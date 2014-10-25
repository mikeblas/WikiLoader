using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WikiReader
{
    /// <summary>
    /// Represense a namespace.
    /// 
    /// Includes the name and Id of the namespace. Also manages a count
    /// which is used to tally the number of articles read per namespace.
    /// </summary>
    class NamespaceInfo
    {
        int _pageCount;
        Int64 _namespaceId;
        String _name;
        /// <summary>
        /// Create a new namespace instance.
        /// pageCount is initalized to one.
        /// 
        /// </summary>
        /// <param name="name">Name of this namespace</param>
        /// <param name="namespaceId">ID for this namespace</param>
        public NamespaceInfo(String name, Int64 namespaceId)
        {
            _name = name;
            _pageCount = 1;
            _namespaceId = namespaceId;
        }

        public String Name
        {
            get { return _name; }
        }

        public int PageCount
        {
            get { return _pageCount; }
        }

        public void IncrementCount()
        {
            _pageCount += 1;
        }
    }
}
