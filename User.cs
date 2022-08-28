using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WikiReader
{
    /// <summary>
    /// Represents a user; contains their name (as a string) and an ID integer.
    /// </summary>
    class User
    {
        readonly string? _userName = null;
        readonly string? _ipAddress = null;
        readonly Int64 _userId = 0;

        /// <summary>
        /// Create a User object
        /// </summary>
        /// <param name="userId">user ID integer</param>
        /// <param name="userName">user name as a string</param>
        public User(Int64 userId, string userName)
        {
            _userId = userId;
            _userName = userName;
        }

        /// <summary>
        /// Create an anonymous user object, identified only by the IP address
        /// </summary>
        /// <param name="ipAddress">IP address, as a string, where the edit was logged</param>
        public User(string ipAddress)
        {
            _ipAddress = ipAddress;
        }

        public bool IsAnonymous
        {
            get { return (_userId == 0) && (_userName == null); }
        }

        public Int64 ID
        {
            get { return _userId; }
        }

        public string? Name
        {
            get { return _userName; }
        }

        public string? IPAddress
        {
            get { return _ipAddress; }
        }
    }
}
